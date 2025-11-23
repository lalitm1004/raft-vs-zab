use crate::{
    config::{Config, Peer},
    state::{LogEntry, RaftState, Role},
    storage::Storage,
};
use protobuf_build::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
    raft_server::Raft,
};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status};

/// The main Raft node structure
#[derive(Clone)]
pub struct RaftNode {
    config: Arc<Config>,
    state: Arc<Mutex<RaftState>>,
    storage: Arc<Storage>,
    /// Notifier for when new entries are applied
    apply_notifier: Arc<Notify>,
}

impl RaftNode {
    /// Create a new Raft node
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = Arc::new(Storage::open(&config.log_dir)?);
        let persistent = storage.load_state()?;
        let state = Arc::new(Mutex::new(RaftState::new(config.id, persistent)));

        Ok(Self {
            config: Arc::new(config),
            state,
            storage,
            apply_notifier: Arc::new(Notify::new()),
        })
    }

    /// Get access to the state (for counter service)
    pub fn state(&self) -> &Arc<Mutex<RaftState>> {
        &self.state
    }

    /// Get the port for a given peer ID
    pub fn get_peer_port(&self, peer_id: u16) -> u32 {
        self.config
            .peers
            .iter()
            .find(|p| p.id == peer_id)
            .map(|p| p.addr.port() as u32)
            .unwrap_or(0)
    }

    /// Append a log entry (called by counter service)
    /// Returns the index of the committed entry
    pub async fn append_log_entry(
        &self,
        command: Vec<u8>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let mut state = self.state.lock().await;

        // Only leaders can accept client requests
        if state.role != Role::Leader {
            return Err("Not the leader".into());
        }

        // Create log entry
        let entry = LogEntry {
            term: state.persistent.current_term,
            command,
        };

        // Append to log
        state.persistent.log.push(entry);
        let log_index = state.last_log_index();

        // Persist immediately
        self.storage.save_state(&state.persistent)?;

        tracing::debug!(
            node = state.id,
            index = log_index,
            term = state.persistent.current_term,
            "Appended log entry"
        );

        drop(state);

        // Trigger immediate replication (instead of waiting for heartbeat)
        Arc::clone(&self).send_heartbeats().await;

        Ok(log_index)
    }

    /// Wait for a log entry to be applied to the state machine
    pub async fn wait_for_apply(&self, index: i64) {
        loop {
            let state = self.state.lock().await;
            if state.volatile.last_applied >= index {
                return;
            }
            drop(state);

            // Wait for notification that entries were applied
            self.apply_notifier.notified().await;
        }
    }

    /// Apply committed entries to the state machine
    /// This should be called by the counter service
    pub async fn apply_committed_entries<F, Fut>(
        &self,
        apply_fn: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
    {
        let mut state = self.state.lock().await;

        while state.volatile.last_applied < state.volatile.commit_index {
            state.volatile.last_applied += 1;
            let index = state.volatile.last_applied;

            if let Some(entry) = state.persistent.log.get((index - 1) as usize) {
                let command = entry.command.clone();

                tracing::debug!(
                    node = state.id,
                    index,
                    "Applying log entry to state machine"
                );

                // Release lock while applying
                drop(state);

                // Apply the command
                apply_fn(command).await?;

                // Notify waiters
                self.apply_notifier.notify_waiters();

                // Reacquire lock for next iteration
                state = self.state.lock().await;
            }
        }

        Ok(())
    }

    /// Start the node's background tasks
    pub async fn run(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        // Spawn election timer task
        let node_for_timer = Arc::clone(&self);
        tokio::spawn(async move {
            node_for_timer.election_timer_loop().await;
        });

        // If leader, spawn heartbeat task
        let node_for_heartbeat = Arc::clone(&self);
        tokio::spawn(async move {
            node_for_heartbeat.heartbeat_loop().await;
        });

        Ok(())
    }

    /// Election timer loop - triggers elections when no heartbeat received
    async fn election_timer_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            self.config.election_timeout_ms / 2,
        ));

        loop {
            interval.tick().await;

            let mut state = self.state.lock().await;

            // Only followers and candidates care about election timeout
            if state.role == Role::Leader {
                continue;
            }

            let elapsed = state.last_heartbeat.elapsed();
            let timeout = std::time::Duration::from_millis(self.config.election_timeout_ms);

            if elapsed >= timeout {
                tracing::info!(
                    node = state.id,
                    role = ?state.role,
                    elapsed_ms = elapsed.as_millis(),
                    "Election timeout - starting election"
                );

                // Start election
                state.become_candidate();

                // Save state before releasing lock
                if let Err(e) = self.storage.save_state(&state.persistent) {
                    tracing::error!(error = ?e, "Failed to save state");
                }

                let term = state.persistent.current_term;
                let candidate_id = state.id;
                let last_log_index = state.last_log_index();
                let last_log_term = state.last_log_term();

                // Release lock before making RPC calls
                drop(state);

                // Request votes from all peers
                self.request_votes(term, candidate_id, last_log_index, last_log_term)
                    .await;
            }
        }
    }

    /// Request votes from all peers
    async fn request_votes(
        self: &Arc<Self>,
        term: i64,
        candidate_id: u16,
        last_log_index: i64,
        last_log_term: i64,
    ) {
        let mut vote_handles = vec![];

        for peer in &self.config.peers {
            let peer_addr = peer.addr;
            let request = RequestVoteRequest {
                term,
                candidate_id: candidate_id as u32,
                last_log_index,
                last_log_term,
            };

            let handle = tokio::spawn(async move {
                let uri = format!("http://{}", peer_addr);
                match protobuf_build::raft::raft_client::RaftClient::connect(uri).await {
                    Ok(mut client) => match client.request_vote(request).await {
                        Ok(response) => Some(response.into_inner()),
                        Err(e) => {
                            tracing::debug!(peer = ?peer_addr, error = ?e, "RequestVote failed");
                            None
                        }
                    },
                    Err(e) => {
                        tracing::debug!(peer = ?peer_addr, error = ?e, "Failed to connect");
                        None
                    }
                }
            });

            vote_handles.push(handle);
        }

        // Wait for all votes
        let mut votes_granted = 1; // Vote for self
        let majority = (self.config.cluster_size / 2) + 1;

        for handle in vote_handles {
            if let Ok(Some(response)) = handle.await {
                let mut state = self.state.lock().await;

                // Check if we're still a candidate in the same term
                if state.role != Role::Candidate || state.persistent.current_term != term {
                    return;
                }

                // If response has higher term, become follower
                if response.term > state.persistent.current_term {
                    state.become_follower(response.term);
                    if let Err(e) = self.storage.save_state(&state.persistent) {
                        tracing::error!(error = ?e, "Failed to save state");
                    }
                    return;
                }

                if response.vote_granted {
                    votes_granted += 1;
                    tracing::debug!(
                        node = state.id,
                        votes = votes_granted,
                        needed = majority,
                        "Received vote"
                    );

                    if votes_granted >= majority {
                        // Won election!
                        let peer_ids: Vec<u16> = self.config.peers.iter().map(|p| p.id).collect();
                        state.become_leader(&peer_ids);

                        if let Err(e) = self.storage.save_state(&state.persistent) {
                            tracing::error!(error = ?e, "Failed to save state");
                        }

                        // Send immediate heartbeats
                        drop(state);
                        self.send_heartbeats().await;
                        return;
                    }
                }
            }
        }
    }

    /// Heartbeat loop - leaders send periodic heartbeats
    async fn heartbeat_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            self.config.heartbeat_interval_ms,
        ));

        loop {
            interval.tick().await;

            let state = self.state.lock().await;
            if state.role == Role::Leader {
                drop(state);
                self.send_heartbeats().await;
            }
        }
    }

    /// Send heartbeats (empty AppendEntries) to all peers
    async fn send_heartbeats(self: &Arc<Self>) {
        let state = self.state.lock().await;

        if state.role != Role::Leader {
            return;
        }

        let term = state.persistent.current_term;
        let leader_id = state.id;
        let commit_index = state.volatile.commit_index;

        tracing::debug!(node = leader_id, term, "Sending heartbeats");

        for peer in &self.config.peers {
            let peer_addr = peer.addr;

            // Get the appropriate prev_log info for this peer
            let next_idx = state
                .leader_state
                .as_ref()
                .and_then(|ls| ls.next_index.get(&peer.id))
                .copied()
                .unwrap_or(1);

            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index > 0 {
                state
                    .persistent
                    .log
                    .get((prev_log_index - 1) as usize)
                    .map(|e| e.term)
                    .unwrap_or(0)
            } else {
                0
            };

            let request = AppendEntriesRequest {
                term,
                leader_id: leader_id as u32,
                prev_log_index,
                prev_log_term,
                entries: vec![], // Empty for heartbeat
                leader_commit: commit_index,
            };

            tokio::spawn(async move {
                let uri = format!("http://{}", peer_addr);
                if let Ok(mut client) =
                    protobuf_build::raft::raft_client::RaftClient::connect(uri).await
                {
                    let _ = client.append_entries(request).await;
                }
            });
        }
    }
}

#[tonic::async_trait]
impl Raft for RaftNode {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.lock().await;

        tracing::info!(
            node = state.id,
            candidate = req.candidate_id,
            term = req.term,
            current_term = state.persistent.current_term,
            "Received RequestVote"
        );

        // If request term is higher, update our term and become follower
        if req.term > state.persistent.current_term {
            state.become_follower(req.term);
            if let Err(e) = self.storage.save_state(&state.persistent) {
                tracing::error!(error = ?e, "Failed to save state");
            }
        }

        let vote_granted = if req.term < state.persistent.current_term {
            // Reject if candidate's term is older
            false
        } else if let Some(voted_for) = state.persistent.voted_for {
            // Already voted in this term
            voted_for == req.candidate_id as u16
        } else if state.log_is_up_to_date(req.last_log_index, req.last_log_term) {
            // Grant vote if candidate's log is at least as up-to-date
            state.persistent.voted_for = Some(req.candidate_id as u16);
            state.reset_election_timer();
            if let Err(e) = self.storage.save_state(&state.persistent) {
                tracing::error!(error = ?e, "Failed to save state");
            }
            true
        } else {
            false
        };

        tracing::info!(
            node = state.id,
            candidate = req.candidate_id,
            vote_granted,
            "RequestVote response"
        );

        Ok(Response::new(RequestVoteResponse {
            term: state.persistent.current_term,
            vote_granted,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.lock().await;

        tracing::debug!(
            node = state.id,
            leader = req.leader_id,
            term = req.term,
            entries = req.entries.len(),
            "Received AppendEntries"
        );

        // If request term is higher, update our term and become follower
        if req.term > state.persistent.current_term {
            state.become_follower(req.term);
            if let Err(e) = self.storage.save_state(&state.persistent) {
                tracing::error!(error = ?e, "Failed to save state");
            }
        }

        // Reject if term is lower
        if req.term < state.persistent.current_term {
            return Ok(Response::new(AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            }));
        }

        // Valid leader for this term
        state.current_leader = Some(req.leader_id as u16);
        state.reset_election_timer();

        // For now, just accept heartbeats
        // TODO: Implement log replication logic

        Ok(Response::new(AppendEntriesResponse {
            term: state.persistent.current_term,
            success: true,
        }))
    }
}
