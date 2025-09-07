use std::collections::HashMap;

use serde_redis::{BulkString, Value};

use crate::storage::{OpError, OpResult};

#[derive(Debug, Clone)]
pub enum StreamId {
    Value { time_id: u128, seq_id: u128 },
    Auto,
    PartialAuto(/* time_id: */ u128),
}

impl StreamId {
    pub fn new(time_id: u128, seq_id: u128) -> Self {
        Self::Value { time_id, seq_id }
    }

    pub fn to_bulk_string(self) -> BulkString {
        let s = match self {
            StreamId::Value { time_id, seq_id } => format!("{}-{}", time_id, seq_id),
            StreamId::Auto => "*".into(),
            StreamId::PartialAuto(time_id) => format!("{time_id}-*"),
        };
        BulkString::new(s)
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Sequence number part of name in the last entry.
    ///
    /// Should be more than zero.
    last_entry_seq_id: u128,

    /// All datas in stream.
    data: HashMap<u128, Vec<Value>>,
}

impl StreamEntry {
    fn new(seq_id: u128, values: HashMap<u128, Vec<Value>>) -> Self {
        Self {
            last_entry_seq_id: seq_id,
            data: values,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    /// Timestamp part of name in the last entry.
    last_entry_time_id: u128,

    /// All entries in stream.
    entries: HashMap<u128, StreamEntry>,
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
        time_id: u128,
        seq_id: u128,
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
                // Add new record to existing entry.
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

    pub fn get_next_seq_id(&self, time_id: u128) -> u128 {
        self.entries
            .get(&time_id)
            .map_or_else(|| 0, |s| s.last_entry_seq_id + 1)
    }
}
