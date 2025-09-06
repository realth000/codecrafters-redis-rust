use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use serde_redis::{Array, SimpleError, Value};

pub(crate) type OpResult<T> = Result<T, OpError>;

pub(crate) enum OpError {
    /// No such key in storage.
    KeyAbsent,

    /// Failed to operate on diffrent type.
    TypeMismatch,
}

impl OpError {
    /// Build the message to return according to current error.
    pub fn to_message_bytes(&self) -> Vec<u8> {
        let e = match self {
            OpError::KeyAbsent => {
                SimpleError::with_prefix("KEYNOTFOUND", "key not found in storage")
            }
            OpError::TypeMismatch => SimpleError::with_prefix(
                "WRONGTYPE",
                "Operation against a key holding the wrong kind of value",
            ),
        };

        serde_redis::to_vec(&e).unwrap()
    }
}

enum LiveValue {
    /// Value exists and is alive.
    Live(Value),

    /// Value exists but is expired.
    Expired,

    /// No value available.
    Absent,
}

#[derive(Debug, Clone)]
struct ValueCell {
    /// Value content.
    value: Value,

    /// When will the value expire.
    expiration: Option<SystemTime>,
}

impl ValueCell {
    fn live_value(&self) -> LiveValue {
        match self.expiration {
            Some(d) => {
                if d > SystemTime::now() {
                    LiveValue::Live(self.value.clone())
                } else {
                    // Expired.
                    LiveValue::Expired
                }
            }
            None => LiveValue::Live(self.value.clone()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Storage {
    inner: Arc<Mutex<StorageInner>>,
}

struct StorageInner {
    data: HashMap<String, ValueCell>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StorageInner {
                data: HashMap::new(),
            })),
        }
    }

    /// Duration is the live duration till value expire.
    pub fn insert(&self, key: String, value: Value, duration: Option<Duration>) {
        let mut lock = self.inner.lock().unwrap();
        let expiration = duration.map(|d| SystemTime::now().checked_add(d).unwrap());
        let cell = ValueCell { value, expiration };
        if lock.data.insert(key, cell).is_some() {
            println!("[storage] override");
        }
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        let mut lock = self.inner.lock().unwrap();
        match lock
            .data
            .get(key)
            .map(|c| c.live_value())
            .unwrap_or_else(|| LiveValue::Absent)
        {
            LiveValue::Live(value) => Some(value),
            LiveValue::Expired => {
                // Value exists but expired, clean up.
                lock.data.remove(key);
                println!("[storage] get {key}: expired");
                None
            }
            LiveValue::Absent => {
                // No value related to key
                None
            }
        }
    }

    /// Append elements to the list specified by `key`.
    ///
    /// If key not present and `create` is true, create a new list.
    ///
    /// ## Returns
    ///
    /// * `Some(v)` if saved successfully, return the current count of elements.
    /// * `None` if list not exists and `create` is false, nothing performed in this situaion.
    pub fn append_list(&self, key: String, value: Array, create: bool) -> OpResult<usize> {
        let mut lock = self.inner.lock().unwrap();

        match lock.data.get_mut(key.as_str()) {
            Some(v) => {
                if let Value::Array(arr) = &mut v.value {
                    arr.append(value);
                    Ok(arr.len())
                } else {
                    Err(OpError::TypeMismatch)
                }
            }
            None => {
                if !create {
                    return Err(OpError::KeyAbsent);
                }

                let count = value.len();
                let cell = ValueCell {
                    value: Value::Array(value),
                    expiration: None,
                };

                lock.data.insert(key, cell);
                Ok(count)
            }
        }
    }
}
