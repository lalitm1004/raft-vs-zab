use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// must survive crashes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: i64,
    pub voted_for: Option<u16>,
    pub log: Vec<LogEntry>,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![],
        }
    }
}

// entry stored in persistent log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: i64,
    pub command: Vec<u8>,
}

// in memory state
#[derive(Debug, Default)]
pub struct VolatileState {
    pub commit_index: i64,
    pub last_applied: i64,
}

// leader specific state
#[derive(Debug)]
pub struct LeaderState {
    pub next_index: HashMap<u16, i64>,
    pub match_index: HashMap<u16, i64>,
}

impl LeaderState {
    pub fn new(peer_ids: &[u16], last_log_index: i64) -> Self {
        let next_index = peer_ids
            .iter()
            .map(|&id| (id, last_log_index + 1))
            .collect();

        let match_index = peer_ids.iter().map(|&id| (id, 0)).collect();

        Self {
            next_index,
            match_index,
        }
    }
}

// main raft node state
pub struct RaftState {
    pub id: u16,
    pub role: Role,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub leader_state: Option<LeaderState>,
    pub current_leader: Option<u16>,
    pub last_heartbeat: std::time::Instant,
}

impl RaftState {
    pub fn new(id: u16, persistent: PersistentState) -> Self {
        Self {
            id,
            role: Role::Follower,
            persistent,
            volatile: VolatileState::default(),
            leader_state: None,
            current_leader: None,
            last_heartbeat: std::time::Instant::now(),
        }
    }

    // idx of last log entry
    #[inline]
    pub fn last_log_index(&self) -> i64 {
        self.persistent.log.len() as i64
    }

    // term of last log entry
    #[inline]
    pub fn last_log_term(&self) -> i64 {
        self.persistent
            .log
            .last()
            .map(|entry| entry.term)
            .unwrap_or(0)
    }

    pub fn log_is_up_to_date(&self, last_log_index: i64, last_log_term: i64) -> bool {
        let our_last_term = self.last_log_term();

        // higher term wins
        if last_log_term > our_last_term {
            return true;
        }

        // if same term, longer log wins
        if last_log_term == our_last_term && last_log_index >= self.last_log_index() {
            return true;
        }

        false
    }

    pub fn become_follower(&mut self, term: i64) {
        let old_role = self.role;
        tracing::info!(
            target: "raft",
            node = self.id,
            old_role = ?old_role,
            new_role = "follower",
            new_term = term,
            "transitioning to follower"
        );

        self.role = Role::Follower;
        self.persistent.current_term = term;
        self.persistent.voted_for = None;
        self.leader_state = None;
        self.last_heartbeat = std::time::Instant::now();
    }

    pub fn become_candidate(&mut self) {
        let old_role = self.role;
        let old_term = self.persistent.current_term;
        self.persistent.current_term += 1;
        self.role = Role::Candidate;
        self.persistent.voted_for = Some(self.id);
        self.leader_state = None;
        self.last_heartbeat = std::time::Instant::now();

        tracing::info!(
            target: "raft",
            node = self.id,
            old_role = ?old_role,
            new_role = "candidate",
            old_term,
            new_term = self.persistent.current_term,
            "starting election as candidate"
        );
    }

    pub fn become_leader(&mut self, peer_ids: &[u16]) {
        let old_role = self.role;
        let last_index = self.last_log_index();
        self.role = Role::Leader;
        self.current_leader = Some(self.id);
        self.leader_state = Some(LeaderState::new(peer_ids, last_index));

        tracing::info!(
            target: "raft",
            node = self.id,
            old_role = ?old_role,
            new_role = "leader",
            term = self.persistent.current_term,
            last_log_index = last_index,
            peer_count = peer_ids.len(),
            "node became leader"
        );
    }

    pub fn reset_election_timer(&mut self) {
        self.last_heartbeat = std::time::Instant::now();
        tracing::debug!(
            target = "raft",
            node = self.id,
            last_heartbeat = ?self.last_heartbeat,
            "election timer reset"
        );
    }
}
