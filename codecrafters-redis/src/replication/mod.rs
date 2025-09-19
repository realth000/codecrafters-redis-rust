use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context};
use serde_redis::{Array, BulkString, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
};

#[derive(Debug, Clone)]
pub(crate) struct ReplicationState {
    inner: Arc<Mutex<ReplicationInner>>,
}

#[derive(Debug)]
struct ReplicationInner {
    master: Option<(Ipv4Addr, u16)>,
    id: &'static str,
    offset: usize,
    replica: Vec<TcpStream>,
}

impl ReplicationState {
    pub(crate) fn new(master: Option<(Ipv4Addr, u16)>) -> Self {
        let inner = ReplicationInner {
            master,
            id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
            offset: 0,
            replica: vec![],
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub(crate) fn info(&self) -> Value {
        let lock = self.inner.lock().unwrap();
        lock.info()
    }

    pub(crate) async fn handshake(&self, port: u16) -> ServerResult<()> {
        let lock = self.inner.lock().unwrap();
        lock.handshake(port).await
    }

    pub(crate) fn id(&self) -> String {
        let lock = self.inner.lock().unwrap();
        lock.id()
    }

    pub(crate) async fn sync_command(&mut self, args: Array) {
        let mut lock = self.inner.lock().unwrap();
        lock.sync_command(args).await
    }

    pub(crate) fn set_replica(&mut self, socket: TcpStream) {
        let mut lock = self.inner.lock().unwrap();
        lock.set_replica(socket)
    }
}

impl ReplicationInner {
    fn info(&self) -> Value {
        let mut buf = vec![];
        buf.extend(b"# Replication\n");
        if self.master.is_some() {
            buf.extend(b"role:slave\n");
        } else {
            buf.extend(b"role:master\n");
        }

        buf.extend(b"master_replid:");
        buf.extend(self.id.as_bytes());
        buf.push(b'\n');

        buf.extend(b"master_repl_offset:");
        buf.extend(self.offset.to_string().as_bytes());
        buf.push(b'\n');

        Value::BulkString(BulkString::new(buf))
    }

    async fn handshake(&self, port: u16) -> ServerResult<()> {
        let master_addr = match self.master {
            Some(v) => v,
            None => return Err(ServerError::ReplicaConfigNotSet),
        };
        let socket = TcpSocket::new_v4()
            .context("[replica] failed to instaniate the socket")
            .map_err(ServerError::Custom)?;
        let mut conn = socket
            .connect(SocketAddr::new(
                std::net::IpAddr::V4(master_addr.0),
                master_addr.1,
            ))
            .await
            .context("[replica] failed to connect to master")
            .map_err(ServerError::Custom)?;

        let mut buf = [0u8; 1024];

        // Send PING

        let ping = Value::Array(Array::with_values(vec![Value::BulkString(
            BulkString::new("PING"),
        )]));
        let n = conn
            .write(serde_redis::to_vec(&ping).unwrap().as_slice())
            .await
            .context("[replica] failed to send PING message")
            .map_err(ServerError::Custom)?;
        println!("[replica] PING: sent {n} bytes");
        let n = conn
            .read(&mut buf)
            .await
            .context("failed to read PING reply")
            .map_err(ServerError::Custom)?;
        match serde_redis::from_bytes(&buf[0..n])
            .context("failed to read PING response:")
            .map_err(ServerError::Custom)?
        {
            Value::SimpleString(s) if s.value() == "PONG" => { /* Correct response */ }
            v => {
                return Err(ServerError::Custom(anyhow!(
                    "[replica] invalid PING response: {v:?}"
                )))
            }
        }

        // Send REPLCONF listening-port

        let replconf = Value::Array(Array::with_values(vec![
            Value::BulkString(BulkString::new("REPLCONF")),
            Value::BulkString(BulkString::new("listening-port")),
            Value::BulkString(BulkString::new(port.to_string())),
        ]));
        let n = conn
            .write(serde_redis::to_vec(&replconf).unwrap().as_slice())
            .await
            .context("failed to send REPLCONF listening-port")
            .map_err(ServerError::Custom)?;
        println!("[replica] REPLCONF listening-port: sent {n} bytes");
        let n = conn
            .read(&mut buf)
            .await
            .context("failed to read REPLCONF listening-port reply")
            .map_err(ServerError::Custom)?;
        match serde_redis::from_bytes(&buf[0..n])
            .context("failed to read REPLCONF listening-port response:")
            .map_err(ServerError::Custom)?
        {
            Value::SimpleString(s) if s.value() == "OK" => { /* Correct response */ }
            v => {
                return Err(ServerError::Custom(anyhow!(
                    "[replica] invalid REPLCONF listening-port response: {v:?}"
                )))
            }
        }

        // Send REPLCONF listening-port

        let replconf = Value::Array(Array::with_values(vec![
            Value::BulkString(BulkString::new("REPLCONF")),
            Value::BulkString(BulkString::new("capa")),
            Value::BulkString(BulkString::new("psync2")),
        ]));
        let n = conn
            .write(serde_redis::to_vec(&replconf).unwrap().as_slice())
            .await
            .context("failed to send REPLCONF capa")
            .map_err(ServerError::Custom)?;
        println!("[replica] REPLCONF capa: sent {n} bytes");
        let n = conn
            .read(&mut buf)
            .await
            .context("failed to read REPLCONF capa reply")
            .map_err(ServerError::Custom)?;
        match serde_redis::from_bytes(&buf[0..n])
            .context("failed to read REPLCONF capa response:")
            .map_err(ServerError::Custom)?
        {
            Value::SimpleString(s) if s.value() == "OK" => { /* Correct response */ }
            v => {
                return Err(ServerError::Custom(anyhow!(
                    "[replica] invalid REPLCONF capa response: {v:?}"
                )))
            }
        }

        // Send PSYNC

        let psync = Value::Array(Array::with_values(vec![
            Value::BulkString(BulkString::new("PSYNC")),
            Value::BulkString(BulkString::new("?")),
            Value::BulkString(BulkString::new("-1")),
        ]));
        let n = conn
            .write(serde_redis::to_vec(&psync).unwrap().as_slice())
            .await
            .context("failed to send psync")
            .map_err(ServerError::Custom)?;
        println!("[replica] psync: sent {n} bytes");
        let n = conn
            .read(&mut buf)
            .await
            .context("failed to read psync reply")
            .map_err(ServerError::Custom)?;
        let master_id = match serde_redis::from_bytes(&buf[0..n])
            .context("failed to read psync response:")
            .map_err(ServerError::Custom)?
        {
            Value::SimpleString(s) => {
                let segs = s.value().split(' ').collect::<Vec<_>>();
                if segs.len() == 3 && segs[0] == "FULLRESYNC" && segs[2] == "0" {
                    segs[1].to_string()
                } else {
                    return Err(ServerError::Custom(anyhow!(
                        "invalid psync response: {s:?}"
                    )));
                }
            }
            v => {
                return Err(ServerError::Custom(anyhow!(
                    "[replica] invalid REPLCONF capa response: {v:?}"
                )))
            }
        };

        println!("[replica] handshake success, master id is {master_id}");

        Ok(())
    }

    fn id(&self) -> String {
        self.id.into()
    }

    async fn sync_command(&mut self, args: Array) {
        for conn in self.replica.iter_mut() {
            let mut conn = Conn::new(10000, conn);
            if let Err(e) = conn.write_value(Value::Array(args.clone())).await {
                conn.log(format!("failed to replica sync: {e}"));
            }
        }
    }

    fn set_replica(&mut self, socket: TcpStream) {
        self.replica.push(socket);
    }
}
