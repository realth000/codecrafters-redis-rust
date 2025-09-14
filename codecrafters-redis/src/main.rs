use std::{net::Ipv4Addr, str::FromStr};

use anyhow::{Context, Result};

use crate::server::RedisServer;

mod command;
mod conn;
mod error;
mod replication;
mod server;
mod storage;
mod transaction;

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let mut port = 6379;
    let mut master = None;
    for w in args.windows(2) {
        match w[0].as_str() {
            "--port" => port = w[1].parse::<u16>().context("invalid port")?,
            "--replicaof" => {
                match w[1].split_once(" ").map(|(ip, port)| {
                    (
                        if ip == "localhost" {
                            Ipv4Addr::new(127, 0, 0, 1)
                        } else {
                            Ipv4Addr::from_str(ip).unwrap()
                        },
                        port.parse::<u16>().unwrap(),
                    )
                }) {
                    Some((ip, port)) => master = Some((ip, port)),
                    None => continue,
                }
            }
            _ => continue,
        }
    }

    let server = RedisServer::new(Ipv4Addr::new(127, 0, 0, 1), port, master);
    if let Err(e) = server.replica_handshake().await {
        println!("[main][replica]: {e}");
    }
    server.serve().await.context("when running server")?;
    Ok(())
}
