use std::net::Ipv4Addr;

use serde_redis::{BulkString, Value};

#[derive(Debug)]
pub(crate) struct ReplicationState {
    master: Option<(Ipv4Addr, u16)>,
}

impl ReplicationState {
    pub(crate) fn new(master: Option<(Ipv4Addr, u16)>) -> Self {
        Self { master }
    }

    pub(crate) fn info(&self) -> Value {
        let mut buf = vec![];
        buf.extend(b"# Replication\n");
        if self.master.is_some() {
            buf.extend(b"role:slave\n");
        } else {
            buf.extend(b"role:master\n");
        }
        Value::BulkString(BulkString::new(buf))
    }
}
