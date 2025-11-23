use crate::state::PersistentState;
use sled::Db;
use std::path::Path;

const STATE_KEY: &[u8] = b"raft_state";

// handles persistence of raft state using sled
pub struct Storage {
    db: Db,
}

impl Storage {
    // open or create storage at the given path
    pub fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(path)?;
        let db = sled::open(path)?;

        tracing::info!(
            target: "raft",
            path = ?path,
            "opened persistent storage"
        );

        Ok(Self { db })
    }

    // load persistent state from disk
    pub fn load_state(&self) -> Result<PersistentState, Box<dyn std::error::Error>> {
        match self.db.get(STATE_KEY)? {
            Some(bytes) => {
                let state: PersistentState =
                    bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;

                tracing::info!(
                    target: "raft",
                    term = state.current_term,
                    log_len = state.log.len(),
                    voted_for = ?state.voted_for,
                    "loaded state from disk"
                );

                Ok(state)
            }
            None => {
                tracing::info!(target: "raft", "no existing state found, starting fresh");
                Ok(PersistentState::default())
            }
        }
    }

    // save persistent state to disk
    pub fn save_state(&self, state: &PersistentState) -> Result<(), Box<dyn std::error::Error>> {
        let bytes = bincode::encode_to_vec(state, bincode::config::standard())?;
        self.db.insert(STATE_KEY, bytes)?;
        self.db.flush()?;

        tracing::debug!(
            target: "raft",
            term = state.current_term,
            log_len = state.log.len(),
            voted_for = ?state.voted_for,
            "saved state to disk"
        );

        Ok(())
    }
}
