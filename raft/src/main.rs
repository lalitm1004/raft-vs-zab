use clap::Parser;
use protobuf_build::counter::counter_service_server::CounterServiceServer;
use protobuf_build::raft::raft_server::RaftServer;
use raft::counter::{CounterCommand, CounterServiceImpl};
use raft::node::RaftNode;
use raft::{Cli, Config};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config = Config::from(cli);
    let addr = SocketAddr::new(config.ip.into(), config.port);

    raft::init_logging(&config.log_dir)?;

    tracing::info!(target: "raft", "Starting Raft Node {} on {}", config.id, addr);

    // 1. Initialize RaftNode
    let node = Arc::new(RaftNode::new(config).await?);

    // 2. Initialize Counter State
    let counter = Arc::new(Mutex::new(0i64));

    // 3. Spawn Applier Task
    let applier_node = node.clone();
    let applier_counter = counter.clone();
    tokio::spawn(async move {
        loop {
            let result = applier_node
                .apply_committed_entries(|command_bytes| {
                    let applier_counter = applier_counter.clone();
                    async move {
                        let mut counter_lock = applier_counter.lock().await;
                        let (command, _): (CounterCommand, usize) =
                            bincode::decode_from_slice(&command_bytes, bincode::config::standard())
                                .map_err(|e| format!("Failed to deserialize: {}", e))?;

                        match command {
                            CounterCommand::Increment(n) => *counter_lock += n,
                            CounterCommand::Set(n) => *counter_lock = n,
                        }
                        
                        tracing::info!(target: "counter", "Applied command: {:?}, new value: {}", command, *counter_lock);
                        Ok(())
                    }
                })
                .await;

            if let Err(e) = result {
                tracing::error!("Applier task error: {}", e);
            }
            
            // Short sleep to avoid busy waiting
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });

    // 4. Start Raft Node background tasks
    let run_node = node.clone();
    tokio::spawn(async move {
        if let Err(e) = run_node.run().await {
            tracing::error!("Raft node background task failed: {}", e);
        }
    });

    // 5. Start gRPC Server
    let counter_service = CounterServiceImpl::new(node.clone(), counter);

    tracing::info!(target: "raft", "Server listening on {}", addr);

    Server::builder()
        .add_service(RaftServer::new(node.as_ref().clone()))
        .add_service(CounterServiceServer::new(counter_service))
        .serve(addr)
        .await?;

    Ok(())
}
