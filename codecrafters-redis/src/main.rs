use std::{net::Ipv4Addr, str::FromStr};

use anyhow::{Context, Result};
use serde_redis::Array;
use tokio::io::AsyncReadExt;

use crate::{
    command::{dispatch_command, DispatchResult},
    conn::Conn,
    replication::ReplicationState,
    server::RedisServer,
};

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
    let mut master_config = None;
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
                    Some((ip, port)) => master_config = Some((ip, port)),
                    None => continue,
                }
            }
            _ => continue,
        }
    }

    let replication = ReplicationState::new(master_config);

    // The connection with master node, if current instance started with `--repliconf` config.
    // Master node may send commands via the connection, these connection shall be applied on current instance.
    let rep_master_conn = match replication.handshake(port).await {
        Ok(v) => Some(v),
        Err(e) => {
            println!("[main][replica] handshake failed: {e}");
            None
        }
    };

    let server = RedisServer::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let mut storage = server.clone_storage();
    let rep = replication.clone();

    // Run the loop where we act like replica node: receive commands provided
    // by master node and apply those commands. This loop keeps current instance
    // sync with master node.
    tokio::spawn(async move {
        if let Some(mut rep_master_conn) = rep_master_conn {
            let mut buf = [0u8; 1024];
            loop {
                let n = match rep_master_conn.read(&mut buf).await {
                    Ok(v) => v,
                    Err(e) => {
                        println!("failed to get read replica master connection: {e}, exit loop");
                        break;
                    }
                };

                let message: Array = match serde_redis::from_bytes(&buf[0..n]) {
                    Ok(v) => v,
                    Err(e) => {
                        println!("failed to deserialize repli master message: {e}, exit loop");
                        break;
                    }
                };
                let rep2 = rep.clone();
                let mut conn = Conn::new(30000, &mut rep_master_conn);
                match dispatch_command(&mut conn, message.clone(), &mut storage, rep2).await {
                    Ok(DispatchResult::None) | Ok(DispatchResult::Replica) => { /* Do nothing */ }
                    Ok(DispatchResult::ReplicaSync) => {
                        // Here in this async task we are acting like replica node.
                        // So every commands that need to be synced should be applied on current
                        // instance, because we are the replica node, the node need to be synced.
                        println!("[main][replica] sync command from master node: {message:?}");
                    }
                    Err(e) => {
                        println!("failed to dispatch replica command from master: {e}, exit loop");
                        break;
                    }
                }
            }
        }
    });

    server
        .serve(replication)
        .await
        .context("when running server")?;
    Ok(())
}
