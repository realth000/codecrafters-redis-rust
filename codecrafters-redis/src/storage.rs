use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use serde_redis::Value;

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
}
