use std::net::{Ipv4Addr, SocketAddr};

use anyhow::{Context, Result};
use serde_redis::Array;
use tokio::net::{TcpListener, TcpStream};

use crate::{
    command::{dispatch_command, DispatchResult},
    conn::Conn,
    error::ServerError,
    replication::ReplicationState,
    storage::Storage,
};

pub struct RedisServer {
    ip: Ipv4Addr,
    port: u16,
    storage: Storage,
}

impl RedisServer {
    pub fn new(ip: Ipv4Addr, port: u16) -> Self {
        Self {
            ip,
            port,
            storage: Storage::new(),
        }
    }

    /// Run the server.
    ///
    /// Hold a replication settings to act like master node, sync commands to replicas connected.
    pub async fn serve(&self, rep: ReplicationState) -> Result<()> {
        let listener = TcpListener::bind((self.ip, self.port))
            .await
            .context("failed to bind tcp socket")?;

        let mut id = 0;

        loop {
            let (socket, addr) = listener
                .accept()
                .await
                .context("failed to accept new tcp connection")?;
            let mut s = self.storage.clone();
            let rep = rep.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle_task(&mut s, id, socket, addr, rep).await {
                    println!("[{id}] failed to handle task: {e:?}");
                }
            });
            id += 1;
        }
    }

    pub fn clone_storage(&self) -> Storage {
        self.storage.clone()
    }

    async fn handle_task(
        storage: &mut Storage,
        id: usize,
        mut stream: TcpStream,
        addr: SocketAddr,
        mut rep: ReplicationState,
    ) -> Result<()> {
        let mut conn = Conn::new(id, &mut stream);
        conn.log(format!("new connection with client {addr:?}"));
        loop {
            let mut buf = [0u8; 1024];
            let n = conn
                .read(&mut buf)
                .await
                .with_context(|| format!("[{id}] failed to read from stream"))?;
            if n == 0 {
                conn.log("connection closed");
                break;
            }
            conn.log("receive message");
            let message: Array =
                serde_redis::from_bytes(&buf[0..n]).map_err(ServerError::SerdeError)?;
            conn.log("responded to client");
            let rep2 = rep.clone();
            match dispatch_command(&mut conn, message.clone(), storage, rep2).await? {
                DispatchResult::None => { /* Do nothing */ }
                DispatchResult::Replica => {
                    rep.set_replica(stream);
                    break;
                }
                DispatchResult::ReplicaSync => {
                    let mut rep = rep.clone();
                    tokio::task::block_in_place(move || {
                        tokio::runtime::Handle::current()
                            .block_on(async move { rep.sync_command(message.clone()).await })
                    });
                }
            }
        }
        Ok(())
    }
}
