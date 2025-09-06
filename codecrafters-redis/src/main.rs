use std::net::Ipv4Addr;

use anyhow::{Context, Result};

use crate::server::RedisServer;

mod command;
mod conn;
mod error;
mod server;
mod storage;

#[tokio::main]
async fn main() -> Result<()> {
    println!(
        ">>> CPU num: {}",
        std::thread::available_parallelism().unwrap().get()
    );
    let server = RedisServer::new(Ipv4Addr::new(127, 0, 0, 1), 6379);
    server.serve().await.context("when running server")?;
    Ok(())
}
