use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: i64,
    pub command: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct VolatileState {
    pub commit_index: i64,
    pub last_applied: i64,
}

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

    pub fn last_log_index(&self) -> i64 {
        self.persistent.log.len() as i64
    }

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
        tracing::info!(
            node = self.id,
            old_role = ?self.role,
            new_term = term,
            "Becoming follower"
        );

        self.role = Role::Follower;
        self.persistent.current_term = term;
        self.persistent.voted_for = None;
        self.leader_state = None;
        self.last_heartbeat = std::time::Instant::now();
    }

    /// Transition to candidate state (start election)
    pub fn become_candidate(&mut self) {
        self.persistent.current_term += 1;
        self.role = Role::Candidate;
        self.persistent.voted_for = Some(self.id);
        self.leader_state = None;
        self.last_heartbeat = std::time::Instant::now();

        tracing::info!(
            node = self.id,
            term = self.persistent.current_term,
            "Starting election as candidate"
        );
    }

    /// Transition to leader state
    pub fn become_leader(&mut self, peer_ids: &[u16]) {
        tracing::info!(
            node = self.id,
            term = self.persistent.current_term,
            "Became leader"
        );

        self.role = Role::Leader;
        self.current_leader = Some(self.id);

        // Initialize leader state
        let last_log_index = self.last_log_index();
        self.leader_state = Some(LeaderState::new(peer_ids, last_log_index));
    }

    /// Reset election timer
    pub fn reset_election_timer(&mut self) {
        self.last_heartbeat = std::time::Instant::now();
    }
}
