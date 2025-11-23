use crate::node::RaftNode;
use bincode::{Decode, Encode};
use protobuf_build::counter::{
    counter_service_server::CounterService, GetRequest, GetResponse, IncrementRequest,
    IncrementResponse, NotLeaderInfo, SetRequest, SetResponse,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone, Encode, Decode)]
pub enum CounterCommand {
    Increment(i64),
    Set(i64),
}

pub struct CounterServiceImpl {
    raft_node: Arc<RaftNode>,
    counter: Arc<Mutex<i64>>,
}

impl CounterServiceImpl {
    pub fn new(raft_node: Arc<RaftNode>, counter: Arc<Mutex<i64>>) -> Self {
        Self { raft_node, counter }
    }
}

#[tonic::async_trait]
impl CounterService for CounterServiceImpl {
    async fn get(&self, _request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let value = *self.counter.lock().await;
        Ok(Response::new(GetResponse {
            value,
            error: "".to_string(),
            not_leader: None,
        }))
    }

    async fn increment(
        &self,
        request: Request<IncrementRequest>,
    ) -> Result<Response<IncrementResponse>, Status> {
        let req = request.into_inner();
        let command = CounterCommand::Increment(req.amount);
        let command_bytes = bincode::encode_to_vec(&command, bincode::config::standard())
            .map_err(|e| Status::internal(format!("Failed to serialize command: {}", e)))?;

        match self.raft_node.append_log_entry(command_bytes).await {
            Ok(index) => {
                // Wait for the entry to be applied
                self.raft_node.wait_for_apply(index).await;
                let value = *self.counter.lock().await;
                Ok(Response::new(IncrementResponse {
                    success: true,
                    value,
                    error: "".to_string(),
                    not_leader: None,
                }))
            }
            Err(e) => {
                // Check if error is "Not the leader"
                let err_msg = e.to_string();
                if err_msg.contains("Not the leader") {
                    let leader_id = self
                        .raft_node
                        .state()
                        .lock()
                        .await
                        .current_leader
                        .unwrap_or(0);
                    let leader_port = self.raft_node.get_peer_port(leader_id);
                    Ok(Response::new(IncrementResponse {
                        success: false,
                        value: 0,
                        error: "Not the leader".to_string(),
                        not_leader: Some(NotLeaderInfo { leader_port }),
                    }))
                } else {
                    Ok(Response::new(IncrementResponse {
                        success: false,
                        value: 0,
                        error: err_msg,
                        not_leader: None,
                    }))
                }
            }
        }
    }

    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let command = CounterCommand::Set(req.value);
        let command_bytes = bincode::encode_to_vec(&command, bincode::config::standard())
            .map_err(|e| Status::internal(format!("Failed to serialize command: {}", e)))?;

        match self.raft_node.append_log_entry(command_bytes).await {
            Ok(index) => {
                self.raft_node.wait_for_apply(index).await;
                let value = *self.counter.lock().await;
                Ok(Response::new(SetResponse {
                    success: true,
                    value,
                    error: "".to_string(),
                    not_leader: None,
                }))
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("Not the leader") {
                    let leader_id = self
                        .raft_node
                        .state()
                        .lock()
                        .await
                        .current_leader
                        .unwrap_or(0);
                    let leader_port = self.raft_node.get_peer_port(leader_id);
                    Ok(Response::new(SetResponse {
                        success: false,
                        value: 0,
                        error: "Not the leader".to_string(),
                        not_leader: Some(NotLeaderInfo { leader_port }),
                    }))
                } else {
                    Ok(Response::new(SetResponse {
                        success: false,
                        value: 0,
                        error: err_msg,
                        not_leader: None,
                    }))
                }
            }
        }
    }
}
