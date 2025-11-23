use clap::Parser;
use raft::{Cli, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from(Cli::parse());

    println!("{:?}", config);

    Ok(())
}
