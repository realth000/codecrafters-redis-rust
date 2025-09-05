use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde_redis::Value;

#[derive(Clone)]
pub(crate) struct Storage {
    inner: Arc<Mutex<StorageInner>>,
}

struct StorageInner {
    data: HashMap<String, Value>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StorageInner {
                data: HashMap::new(),
            })),
        }
    }

    pub fn insert(&self, key: String, value: Value) {
        let mut lock = self.inner.lock().unwrap();
        if lock.data.insert(key, value).is_some() {
            println!("[storage] override");
        }
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        let lock = self.inner.lock().unwrap();
        lock.data.get(key).cloned()
    }
}
