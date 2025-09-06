use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use serde_redis::{Array, BulkString, SimpleError, Value};

pub(crate) type OpResult<T> = Result<T, OpError>;

pub(crate) enum OpError {
    /// No such key in storage.
    KeyAbsent,

    /// Failed to operate on diffrent type.
    TypeMismatch,
}

impl OpError {
    /// Build the message to return according to current error.
    pub fn to_message(&self) -> Value {
        let e = match self {
            OpError::KeyAbsent => {
                SimpleError::with_prefix("KEYNOTFOUND", "key not found in storage")
            }
            OpError::TypeMismatch => SimpleError::with_prefix(
                "WRONGTYPE",
                "Operation against a key holding the wrong kind of value",
            ),
        };

        Value::SimpleError(e)
    }
    /// Build the message to return according to current error.
    pub fn to_message_bytes(&self) -> Vec<u8> {
        let e = self.to_message();
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
    /// Set `prepend` to true if want to prepend `value` before the head of current element.
    ///
    /// ## Returns
    ///
    /// * `Some(v)` if saved successfully, return the current count of elements.
    /// * `None` if list not exists and `create` is false, nothing performed in this situaion.
    pub fn append_list(
        &self,
        key: String,
        value: Array,
        create: bool,
        prepend: bool,
    ) -> OpResult<usize> {
        let mut lock = self.inner.lock().unwrap();

        match lock.data.get_mut(key.as_str()) {
            Some(v) => {
                if let Value::Array(arr) = &mut v.value {
                    if prepend {
                        arr.prepend(value);
                    } else {
                        arr.append(value);
                    }
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

    pub fn lrange(&self, key: String, start: i32, end: i32) -> OpResult<Value> {
        let lock = self.inner.lock().unwrap();
        if let Some(ValueCell {
            value: Value::Array(arr),
            ..
        }) = lock.data.get(key.as_str())
        {
            if arr.is_null_or_empty() {
                return Ok(Value::Array(Array::new_empty()));
            }

            let start2 = if start >= 0 {
                start as usize
            } else {
                let s = start.abs();
                if arr.len() < (s as usize) {
                    // [a, b, c] => start=-5, reset start to 0
                    0
                } else {
                    arr.len() - (-1 * start) as usize
                }
            };

            let end2 = if end >= 0 {
                end as usize
            } else {
                arr.len() - (-1 * end) as usize
            };

            if end2 < start2 {
                return Ok(Value::Array(Array::new_empty()));
            }

            let arr2 = arr
                .iter()
                .skip(start2)
                .take(end2 - start2 + 1)
                .map(|x| x.to_owned())
                .collect::<Array>();
            Ok(Value::Array(arr2))
        } else {
            Ok(Value::Array(Array::new_empty()))
        }
    }

    /// Get the count of elements in an array specified by `key`.
    ///
    /// * If `key` not present in storage, return `Err(OpError::KeyAbsent)`.
    /// * If the value corresponded to `key` is not an array, return `Err(OpError::TypeMismatch)`.
    pub fn array_get_length(&self, key: impl AsRef<str>) -> OpResult<usize> {
        let lock = self.inner.lock().unwrap();

        if let Some(ValueCell { value, .. }) = lock.data.get(key.as_ref()) {
            if let Value::Array(arr) = value {
                Ok(arr.len())
            } else {
                Err(OpError::TypeMismatch)
            }
        } else {
            Err(OpError::KeyAbsent)
        }
    }

    /// Remove the first `count` elements from array with `key`.
    ///
    /// * If `key` not present in storage, return `Err(OpError::KeyAbsent)`.
    /// * If the value corresponded to `key` is not an array, return `Err(OpError::TypeMismatch)`.
    pub fn array_pop_front(&self, key: impl AsRef<str>, count: Option<usize>) -> OpResult<Value> {
        let mut lock = self.inner.lock().unwrap();

        if let Some(ValueCell { value, .. }) = lock.data.get_mut(key.as_ref()) {
            if let Value::Array(arr) = value {
                if arr.is_empty() {
                    return Ok(Value::BulkString(BulkString::null()));
                }

                match count {
                    Some(c) => {
                        // Take amount of elements.
                        let mut ret = Array::new_empty();
                        for _ in 0..c {
                            match arr.pop_front() {
                                Some(v) => {
                                    ret.push_back(v);
                                }
                                None => {
                                    /* No element left */
                                    break;
                                }
                            }
                        }
                        Ok(Value::Array(ret))
                    }
                    None => {
                        // Take all elements.
                        Ok(arr.pop_front().unwrap())
                    }
                }
            } else {
                Err(OpError::TypeMismatch)
            }
        } else {
            Err(OpError::KeyAbsent)
        }
    }
}
