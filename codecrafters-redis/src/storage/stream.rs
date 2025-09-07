use std::collections::{BTreeMap, HashMap};

use serde_redis::{Array, BulkString, Integer, SimpleString, Value};

use crate::storage::{OpError, OpResult};

#[derive(Debug, Clone)]
pub enum StreamId {
    Value { time_id: u64, seq_id: u64 },
    Auto,
    PartialAuto(/* time_id: */ u64),
}

impl StreamId {
    pub fn new(time_id: u64, seq_id: u64) -> Self {
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
    last_entry_seq_id: u64,

    /// All datas in stream.
    data: HashMap<u64, Vec<Value>>,
}

impl StreamEntry {
    fn new(seq_id: u64, values: HashMap<u64, Vec<Value>>) -> Self {
        Self {
            last_entry_seq_id: seq_id,
            data: values,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    /// Timestamp part of name in the last entry.
    last_entry_time_id: u64,

    /// All entries in stream.
    entries: BTreeMap<u64, StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            last_entry_time_id: 0,
            entries: BTreeMap::new(),
        }
    }

    pub fn add_entry(
        &mut self,
        time_id: u64,
        seq_id: u64,
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

    pub fn get_next_seq_id(&self, time_id: u64) -> u64 {
        self.entries
            .get(&time_id)
            .map_or_else(|| 0, |s| s.last_entry_seq_id + 1)
    }

    pub fn get_range(&self, start: u64, end: u64) -> OpResult<Value> {
        let mut array = Array::new_empty();
        for (id, v) in self.entries.iter() {
            if &start <= id && id <= &end {
                let x = v
                    .data
                    .clone()
                    .into_iter()
                    .map(|x| Value::Array(Array::with_values(x.1)))
                    .collect::<Vec<_>>();
                array.push_back(Value::SimpleString(SimpleString::new(format!(
                    "{start}-{end}"
                ))));
                array.push_back(Value::Array(Array::with_values(x)));
            }
        }
        Ok(Value::Array(array))
    }
}
