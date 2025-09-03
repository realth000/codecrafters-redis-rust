use std::net::Ipv4Addr;

use anyhow::{Context, Result};

use crate::server::RedisServer;

mod protocol;
mod server;

#[tokio::main]
async fn main() -> Result<()> {
    let server = RedisServer::new(Ipv4Addr::new(127, 0, 0, 1), 6379);
    server.serve().await.context("when running server")?;
    Ok(())
}
