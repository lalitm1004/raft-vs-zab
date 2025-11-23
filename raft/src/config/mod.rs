mod cli;

use rand::Rng;
use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
};

pub use cli::Cli;

#[derive(Debug)]
pub struct Peer {
    pub id: u16,
    pub addr: SocketAddr,
}

#[derive(Debug)]
pub struct Config {
    pub id: u16,

    pub cluster_size: usize,

    pub ip: Ipv4Addr,
    pub port: u16,
    pub peers: Vec<Peer>,

    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,

    pub log_dir: PathBuf,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        let port = cli.base_port + cli.id;

        let mut peers = Vec::with_capacity(cli.cluster_size - 1);
        for peer_id in 0..(cli.cluster_size as u16) {
            if peer_id == cli.id {
                continue;
            }

            let peer_port = cli.base_port + peer_id;
            let addr = SocketAddr::new(cli.ip.into(), peer_port);

            peers.push(Peer { id: peer_id, addr });
        }

        let mut rng = rand::rng();
        let election_timeout_ms =
            rng.random_range(cli.election_timeout_min_ms..=cli.election_timeout_max_ms);

        let log_dir = cli.log_dir.join(format!("node{}", cli.id));

        Config {
            id: cli.id,
            cluster_size: cli.cluster_size,
            ip: cli.ip,
            port: port,
            peers,
            election_timeout_ms,
            heartbeat_interval_ms: cli.heartbeat_interval_ms,
            log_dir,
        }
    }
}
