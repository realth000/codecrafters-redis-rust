use std::net::{Ipv4Addr, SocketAddr};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Debug, Clone)]
pub struct RedisServer {
    ip: Ipv4Addr,
    port: u16,
}

impl RedisServer {
    pub fn new(ip: Ipv4Addr, port: u16) -> Self {
        Self { ip, port }
    }

    pub async fn serve(&self) -> Result<()> {
        let listener = TcpListener::bind((self.ip, self.port))
            .await
            .context("failed to bind tcp socket")?;

        let mut id = 0;

        loop {
            let (socket, addr) = listener
                .accept()
                .await
                .context("failed to accept new tcp connection")?;
            tokio::spawn(async move {
                if let Err(e) = Self::handle_task(id, socket, addr).await {
                    println!("[{id}] failed to handle task: {e:?}");
                }
            });
            id += 1;
        }
    }

    async fn handle_task(id: u32, mut stream: TcpStream, addr: SocketAddr) -> Result<()> {
        println!("[{id}] new connection with client {addr:?}");
        loop {
            let mut buf = [0u8; 1024];
            let n = stream
                .read(&mut buf)
                .await
                .with_context(|| format!("[{id}] failed to read from stream"))?;
            if n == 0 {
                println!("connection closed");
                break;
            }
            stream
                .write(b"+PONG\r\n")
                .await
                .with_context(|| format!("[{id}] failed to write response"))?;
            println!("[{id}] send PONG response");
        }
        Ok(())
    }
}
