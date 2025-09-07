use std::collections::HashMap;

use serde_redis::{BulkString, Value};

use crate::storage::{OpError, OpResult};

#[derive(Debug, Clone)]
pub struct StreamId {
    time_id: u32,
    seq_id: u32,
}

impl StreamId {
    fn new(time_id: u32, seq_id: u32) -> Self {
        Self { time_id, seq_id }
    }

    pub fn to_bulk_string(self) -> BulkString {
        BulkString::new(format!("{}-{}", self.time_id, self.seq_id))
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Sequence number part of name in the last entry.
    ///
    /// Should be more than zero.
    last_entry_seq_id: u32,

    /// All datas in stream.
    data: HashMap<u32, Vec<Value>>,
}

impl StreamEntry {
    fn new(seq_id: u32, values: HashMap<u32, Vec<Value>>) -> Self {
        Self {
            last_entry_seq_id: seq_id,
            data: values,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    /// Timestamp part of name in the last entry.
    last_entry_time_id: u32,

    /// All entries in stream.
    entries: HashMap<u32, StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            last_entry_time_id: 0,
            entries: HashMap::new(),
        }
    }

    pub fn add_entry(
        &mut self,
        time_id: u32,
        seq_id: u32,
        values: Vec<Value>,
    ) -> OpResult<StreamId> {
        if time_id == 0 && seq_id == 0 {
            return Err(OpError::InvalidStreamId);
        }
        if time_id < self.last_entry_time_id {
            return Err(OpError::TooSmallStreamId);
        }

        match self.entries.get_mut(&time_id) {
            Some(entry) => {
                // Add new record in existing entry.
                if seq_id <= entry.last_entry_seq_id {
                    return Err(OpError::TooSmallStreamId);
                }

                self.last_entry_time_id = time_id;
                entry.last_entry_seq_id = seq_id;
                entry.data.insert(seq_id, values);
                Ok(StreamId::new(time_id, seq_id))
            }
            None => {
                // Insert new entry.
                self.entries.insert(
                    time_id,
                    StreamEntry::new(seq_id, HashMap::from([(seq_id, values)])),
                );
                self.last_entry_time_id = time_id;
                Ok(StreamId::new(time_id, seq_id))
            }
        }
    }
}
