use std::net::Ipv4Addr;

use serde_redis::{BulkString, Value};

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
}
