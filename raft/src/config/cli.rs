use clap::Parser;
use std::{net::Ipv4Addr, path::PathBuf};

#[derive(Parser, Debug, Clone)]
pub struct Cli {
    #[arg(long)]
    pub id: u16,

    #[arg(long)]
    pub cluster_size: usize,

    #[arg(long, value_parser, default_value = "127.0.0.1")]
    pub ip: Ipv4Addr,

    #[arg(long, default_value_t = 8000)]
    pub base_port: u16,

    #[arg(long, default_value_t = 150)]
    pub election_timeout_min_ms: u64,

    #[arg(long, default_value_t = 300)]
    pub election_timeout_max_ms: u64,

    #[arg(long, default_value_t = 75)]
    pub heartbeat_interval_ms: u64,

    #[arg(long, value_parser, default_value = "./data/raft")]
    pub log_dir: PathBuf,
}
