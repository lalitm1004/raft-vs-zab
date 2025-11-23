use clap::Parser;
use raft::{Cli, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from(Cli::parse());

    raft::init_logging(&config.log_dir)?;

    tracing::info!(target: "raft", "Starting Raft Node");

    Ok(())
}
