use std::{net::Ipv4Addr, str::FromStr};

use anyhow::{bail, Context, Result};
use serde_redis::Array;
use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::{
    command::{dispatch_command, DispatchResult},
    conn::Conn,
    replication::ReplicationState,
    server::RedisServer,
    storage::Storage,
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

    let server = RedisServer::new(Ipv4Addr::new(127, 0, 0, 1), port);

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

    // Run the loop where we act like replica node: receive commands provided
    // by master node and apply those commands. This loop keeps current instance
    // sync with master node.
    let storage2 = server.clone_storage();
    let rep = replication.clone();

    tokio::spawn(async move {
        if let Err(e) = run_replica(rep, rep_master_conn, storage2).await {
            println!("[main][replica] failed to run replica task: {e}");
        }
    });

    server.serve(replication).await?;

    Ok(())
}

async fn run_replica(
    mut rep: ReplicationState,
    rep_master_conn: Option<TcpStream>,
    mut storage: Storage,
) -> Result<()> {
    println!("[main][replica] spawning replica task");
    let mut rep_master_conn = match rep_master_conn {
        Some(v) => v,
        None => {
            println!("[main][replica]: connection not available, skip replica task");
            return Ok::<(), anyhow::Error>(());
        }
    };
    println!("[main][replica] reading RDB file");
    // Read and skip the RDB file.
    // The master node will send a RDB file once connection is setup.
    // RDB file in this format:
    // `$<length_of_file>\r\n<binary_contents_of_file>`
    let mut ch_buf = [0u8; 1];
    rep_master_conn
        .read_exact(&mut ch_buf)
        .await
        .context("failed to read header doller sign in RDB file transfer")?;

    if ch_buf[0] != b'$' {
        bail!(
            "expected dollar sign as the header of RDB file transfer, got '{}'",
            ch_buf[0]
        )
    }

    println!("[main][replica]: reading RDB file length");

    let mut length_buf = vec![];

    // Read the length of RDB file content.
    loop {
        rep_master_conn
            .read_exact(&mut ch_buf)
            .await
            .context("failed to read length in RDB file transfer")?;
        if ch_buf[0] == b'\r' {
            break;
        }
        length_buf.push(ch_buf[0]);
    }

    // The next char shall be '\n'
    rep_master_conn
        .read_exact(&mut ch_buf)
        .await
        .context("failed to read length in RDB file transfer")?;
    if ch_buf[0] != b'\n' {
        bail!("expected LF after CR after length in RDB file transfer")
    }

    let length = length_buf
        .into_iter()
        .rev()
        .enumerate()
        .fold(0, |acc, (idx, ch)| {
            (ch as usize - 48) * 10_usize.pow(idx as u32) + acc
        });

    println!("[main][replica]: reading RDB file content, length is {length}");

    let mut rdb_content_buf = vec![0u8; length];

    rep_master_conn
        .read_exact(&mut rdb_content_buf)
        .await
        .context("failed to read RDB content")?;

    println!(
        "[main][replica] receive RDB file from master node, size is {}",
        length
    );

    let mut buf = [0u8; 1024];
    // Receving commands from master node.
    loop {
        println!("[main][replica] waiting for commands to sync");
        let n = rep_master_conn
            .read(&mut buf)
            .await
            .context("failed to get read replica master connection")?;

        println!(
            "[main][replica] read {n} bytes as command to sync, from master node: {:?}",
            String::from_utf8(buf[0..n].to_vec()).unwrap()
        );

        // Record where we are executing commands in the parsed data.
        let mut exec_pos = 0;
        loop {
            let (message, len): (Array, usize) = serde_redis::from_bytes_len(&buf[exec_pos..n])
                .context("failed to deserialize replia master message")?;
            println!("[main][replica] parsed {len} bytes command, total is {n}");
            let rep2 = rep.clone();
            let mut conn = Conn::new(30000, &mut rep_master_conn);
            match dispatch_command(&mut conn, message.clone(), &mut storage, rep2)
                .await
                .context("failed to dispatch replica command from master")?
            {
                DispatchResult::None | DispatchResult::Replica => { /* Do nothing */ }
                DispatchResult::ReplicaSync => {
                    // Here in this async task we are acting like replica node.
                    // So every command that need to be synced should be applied on current
                    // instance, because we are the replica node, the node need to be synced.
                    println!("[main][replica] sync command from master node: {message:?}");
                }
            }
            rep.add_offset(len);

            if len == 0 {
                // I think this is unreachable.
                unreachable!("something shall be produced when parsing synced commands")
            }
            exec_pos += len;

            if exec_pos == n {
                // All produced.
                break;
            } else if exec_pos > n {
                unreachable!("munched command bytes size not matched, exec_pos={exec_pos}, n={n}")
            }
        }
    }
}
