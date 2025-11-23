use clap::Parser;
use raft::{Cli, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from(Cli::parse());

    raft::init_logging(&config.log_dir, config.id)?;

    // tracing::info!("Starting raft node");
    tracing::info!(target: "raft", "hello");

    Ok(())
}
