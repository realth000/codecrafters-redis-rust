use std::net::{Ipv4Addr, SocketAddr};

use anyhow::Context;
use serde_redis::{to_vec, Array, BulkString, Value};
use tokio::{io::AsyncWriteExt, net::TcpSocket};

use crate::error::{ServerError, ServerResult};

#[derive(Debug)]
pub(crate) struct ReplicationState {
    master: Option<(Ipv4Addr, u16)>,
    id: &'static str,
    offset: usize,
}

impl ReplicationState {
    pub(crate) fn new(master: Option<(Ipv4Addr, u16)>) -> Self {
        Self {
            master,
            id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
            offset: 0,
        }
    }

    pub(crate) fn info(&self) -> Value {
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

    pub(crate) async fn handshake(&self) -> ServerResult<()> {
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

        let ping = Value::Array(Array::with_values(vec![Value::BulkString(
            BulkString::new("PING"),
        )]));

        let n = conn
            .write(to_vec(&ping).unwrap().as_slice())
            .await
            .context("[replica] failed to send PING message")
            .map_err(ServerError::Custom)?;
        println!("[replica] PING: sent {n} bytes");
        Ok(())
    }
}
