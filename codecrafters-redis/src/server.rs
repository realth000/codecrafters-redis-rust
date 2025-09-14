use std::net::{Ipv4Addr, SocketAddr};

use anyhow::{Context, Result};
use serde_redis::Array;
use tokio::{
    net::{TcpListener, TcpStream},
    select,
};

use crate::{
    command::{dispatch_command, DispatchResult},
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub struct RedisServer {
    ip: Ipv4Addr,
    port: u16,
    storage: Storage,
}

impl RedisServer {
    pub fn new(ip: Ipv4Addr, port: u16, master: Option<(Ipv4Addr, u16)>) -> Self {
        Self {
            ip,
            port,
            storage: Storage::new(master),
        }
    }

    pub async fn serve(&self) -> Result<()> {
        let listener = TcpListener::bind((self.ip, self.port))
            .await
            .context("failed to bind tcp socket")?;

        let mut id = 0;

        if self.storage.has_master() {
            // Note that once syncing from master is started, current
            // thread should not be used as master as well, chaining
            // is not implemented.
            let mut storage = self.storage.clone();

            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current()
                            .block_on(async move {                loop {
                    match storage.replica_sync_from_master().await {
                        Ok(Some(v)) => {
                            dispatch_command(v);
                        },
                        Ok(None) => { /* Do nothing */ }
                        Err(e) => {
                            println!("[main] [replica recv]: failed to receive replica sync message from master: {e}");
                            break;
                        }
                    }
                }
})
            });
        }

        loop {
            let (socket, addr) = listener
                .accept()
                .await
                .context("failed to accept new tcp connection")?;
            let mut s = self.storage.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle_task(&mut s, id, socket, addr).await {
                    println!("[{id}] failed to handle task: {e:?}");
                }
            });
            id += 1;
        }
    }

    async fn handle_task(
        storage: &mut Storage,
        id: usize,
        mut stream: TcpStream,
        addr: SocketAddr,
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
            match dispatch_command(&mut conn, message.clone(), storage).await? {
                DispatchResult::None => { /* Do nothing */ }
                DispatchResult::Replica => {
                    storage.set_replica(stream);
                    break;
                }
                DispatchResult::ReplicaSync => {
                    let mut storage = storage.clone();
                    tokio::task::block_in_place(move || {
                        tokio::runtime::Handle::current()
                            .block_on(async move { storage.replica_sync(message.clone()).await })
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn replica_handshake(&self) -> ServerResult<()> {
        self.storage.replica_handshake(self.port).await
    }
}
