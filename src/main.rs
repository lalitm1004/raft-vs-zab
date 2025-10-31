use tonic::{transport::Server, Request, Response, Status};

pub mod replicatedcounter {
    tonic::include_proto!("replicatedcounter");
}

use replicatedcounter::{
    counter_server::{Counter, CounterServer},
    AddRequest, CompareAndSetRequest, CompareAndSetResponse, DecrementRequest, IncrementRequest,
    ReadRequest, ReadResponse, SetRequest, WriteResponse,
};

use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct CounterState {
    value: i64,
    revision: u64,
}

impl CounterState {
    fn new() -> Self {
        Self {
            value: 0,
            revision: 0,
        }
    }

    fn increment(&mut self, _amount: i64) {
        // self.value += if amount == 0 { 1 } else { amount };
        self.value += 1;
        self.revision += 1;
    }

    fn decrement(&mut self, _amount: i64) {
        // self.value -= if amount == 0 { 1 } else { amount };
        self.value -= 1;
        self.revision += 1;
    }

    fn add(&mut self, amount: i64) {
        self.value += amount;
        self.revision += 1;
    }

    fn set(&mut self, value: i64) {
        self.value = value;
        self.revision += 1;
    }

    fn compare_and_set(&mut self, expected: i64, new_value: i64) -> bool {
        if self.value == expected {
            self.value = new_value;
            self.revision += 1;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct CounterService {
    state: Arc<RwLock<CounterState>>,
}

impl CounterService {
    fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(CounterState::new())),
        }
    }
}

#[tonic::async_trait]
impl Counter for CounterService {
    async fn read(&self, _request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let state = self.state.read().await;
        println!(
            "READ → value = {}, revision = {}",
            state.value, state.revision
        );

        Ok(Response::new(ReadResponse {
            value: state.value,
            revision: state.revision,
            leader_id: "single-node".to_string(),
        }))
    }

    async fn increment(
        &self,
        request: Request<IncrementRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.write().await;

        state.increment(req.amount);
        println!(
            "INCREMENT -> value = {}, revision = {}",
            state.value, state.revision
        );

        Ok(Response::new(WriteResponse {
            success: true,
            value: state.value,
            revision: state.revision,
            leader_id: "single-node".to_string(),
        }))
    }

    async fn decrement(
        &self,
        request: Request<DecrementRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.write().await;

        state.decrement(req.amount);
        println!(
            "DECREMENT -> value = {}, revision = {}",
            state.value, state.revision
        );

        Ok(Response::new(WriteResponse {
            success: true,
            value: state.value,
            revision: state.revision,
            leader_id: "single-node".to_string(),
        }))
    }

    async fn add(&self, request: Request<AddRequest>) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.write().await;

        state.add(req.amount);
        println!(
            "ADD (by {}) -> value = {}, revision = {}",
            req.amount, state.value, state.revision
        );

        Ok(Response::new(WriteResponse {
            success: true,
            value: state.value,
            revision: state.revision,
            leader_id: "single-node".to_string(),
        }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.write().await;

        state.set(req.value);
        println!("SET (to {}) -> revision = {}", state.value, state.revision);

        Ok(Response::new(WriteResponse {
            success: true,
            value: state.value,
            revision: state.revision,
            leader_id: "single-node".to_string(),
        }))
    }

    async fn compare_and_set(
        &self,
        request: Request<CompareAndSetRequest>,
    ) -> Result<Response<CompareAndSetResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.write().await;

        let success = state.compare_and_set(req.expected, req.new_value);
        println!(
            "COMPARE_AND_SET (expected {}, new {}) -> success = {}, value = {}, revision = {}",
            req.expected, req.new_value, success, state.value, state.revision
        );

        Ok(Response::new(CompareAndSetResponse {
            success,
            value: state.value,
            revision: state.revision,
            leader_id: "single-node".to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let counter_service = CounterService::new();

    println!("CounterServer listening on {}", addr);

    Server::builder()
        .add_service(CounterServer::new(counter_service))
        .serve(addr)
        .await?;

    Ok(())
}
