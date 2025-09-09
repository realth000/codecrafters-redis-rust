use std::net::Ipv4Addr;

use anyhow::{Context, Result};

use crate::server::RedisServer;

mod command;
mod conn;
mod error;
mod replication;
mod server;
mod storage;
mod transaction;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let mut port = 6379;
    for w in args.windows(2) {
        match w[0].as_str() {
            "--port" => port = w[1].parse::<u16>().context("invalid port")?,
            _ => continue,
        }
    }

    let server = RedisServer::new(Ipv4Addr::new(127, 0, 0, 1), port);
    server.serve().await.context("when running server")?;
    Ok(())
}
