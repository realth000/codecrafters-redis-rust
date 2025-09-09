use serde_redis::{BulkString, Value};

#[derive(Debug)]
pub(crate) struct ReplicationState {}

impl ReplicationState {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn info(&self) -> Value {
        let mut buf = vec![];
        buf.extend(b"# Replication\n");
        buf.extend(b"role:master\n");
        Value::BulkString(BulkString::new(buf))
    }
}
