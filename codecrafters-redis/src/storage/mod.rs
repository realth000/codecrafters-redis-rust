use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_redis::{Array, SimpleError, Value};
use tokio::sync::{oneshot, Notify};

use stream::Stream;

mod stream;

pub use stream::StreamId;
pub(crate) type OpResult<T> = Result<T, OpError>;

pub(crate) enum OpError {
    /// No such key in storage.
    KeyAbsent,

    /// Failed to operate on diffrent type.
    TypeMismatch,

    /// Stream id is less or equal to the last id.
    TooSmallStreamId,

    /// Stream id should be greater than "0-0".
    InvalidStreamId,
}

impl OpError {
    /// Build the message to return according to current error.
    pub fn to_message(self) -> Value {
        let e = match self {
            OpError::KeyAbsent => {
                SimpleError::with_prefix("KEYNOTFOUND", "key not found in storage")
            }
            OpError::TypeMismatch => SimpleError::with_prefix(
                "WRONGTYPE",
                "Operation against a key holding the wrong kind of value",
            ),
            OpError::InvalidStreamId => {
                SimpleError::with_prefix("ERR", "The ID specified in XADD must be greater than 0-0")
            }
            OpError::TooSmallStreamId => SimpleError::with_prefix(
                "ERR",
                "The ID specified in XADD is equal or smaller than the target stream top item",
            ),
        };

        Value::SimpleError(e)
    }
    /// Build the message to return according to current error.
    pub fn to_message_bytes(self) -> Vec<u8> {
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

pub(crate) struct LpopBlockedHandle {
    pub notify: Arc<Notify>,
}

pub(crate) struct LpopBlockedTask {
    key: String,
    handle: Arc<LpopBlockedHandle>,
    sender: oneshot::Sender<Value>,
}

impl LpopBlockedTask {
    pub fn new(key: String) -> (Self, oneshot::Receiver<Value>) {
        let (sender, recver) = oneshot::channel::<Value>();
        let handle = Arc::new(LpopBlockedHandle {
            notify: Arc::new(Notify::new()),
        });

        let s = Self {
            key,
            handle,
            sender,
        };
        (s, recver)
    }

    pub fn clone_handle(&self) -> Arc<LpopBlockedHandle> {
        self.handle.clone()
    }
}

#[derive(Clone)]
pub(crate) struct Storage {
    inner: Arc<Mutex<StorageInner>>,
    lpop_blocked_task: Arc<Mutex<Vec<LpopBlockedTask>>>,
}

struct StorageInner {
    data: HashMap<String, ValueCell>,
    stream: HashMap<String, Stream>,
}

impl StorageInner {
    fn get_next_seq_id(&self, key: impl AsRef<str>, time_id: u128) -> u128 {
        self.stream
            .get(key.as_ref())
            .map_or_else(|| 0, |s| s.get_next_seq_id(time_id))
    }
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StorageInner {
                data: HashMap::new(),
                stream: HashMap::new(),
            })),
            lpop_blocked_task: Arc::new(Mutex::new(vec![])),
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

    /// Insert elements to the list specified by `key`.
    ///
    /// If key not present and `create` is true, create a new list.
    ///
    /// Set `prepend` to true if want to prepend `value` before the head of current element.
    ///
    /// ## Returns
    ///
    /// * `Some(v)` if saved successfully, return the current count of elements.
    /// * `None` if list not exists and `create` is false, nothing performed in this situaion.
    pub fn insert_list(
        &self,
        key: String,
        mut value: Array,
        create: bool,
        prepend: bool,
    ) -> OpResult<usize> {
        let mut lock = self.inner.lock().unwrap();

        // Count of elements that gave to BLPOP tasks.
        // Elements are sent to those tasks first, then save in list.
        // But we should return the orignal count of elements to the
        // client gives us `value`, use this count to balance it.
        let mut interupted_count = 0;
        let mut lpop_lock = self.lpop_blocked_task.lock().unwrap();
        loop {
            if value.is_empty() {
                break;
            }
            match lpop_lock.iter().position(|task| task.key == key) {
                Some(pos) => {
                    // Find a task waiting for current list.
                    let v = value.pop_front().unwrap(); // Not empty for sure.
                    let task_to_feed = lpop_lock.remove(pos);
                    task_to_feed.sender.send(v).unwrap();
                    task_to_feed.handle.notify.notify_one();
                    interupted_count += 1;
                }
                None => {
                    // No one in the blocked task queue is waiting for
                    // current `key` list, break and go ahead.
                    break;
                }
            }
        }

        match lock.data.get_mut(key.as_str()) {
            Some(v) => {
                if let Value::Array(arr) = &mut v.value {
                    if prepend {
                        arr.prepend(value);
                    } else {
                        arr.append(value);
                    }
                    Ok(arr.len() + interupted_count)
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
                Ok(count + interupted_count)
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
    pub fn array_pop_front(
        &self,
        key: impl AsRef<str>,
        count: Option<usize>,
    ) -> OpResult<Option<Value>> {
        let mut lock = self.inner.lock().unwrap();

        if let Some(ValueCell { value, .. }) = lock.data.get_mut(key.as_ref()) {
            if let Value::Array(arr) = value {
                if arr.is_empty() {
                    return Ok(None);
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
                        Ok(Some(Value::Array(ret)))
                    }
                    None => {
                        // Take the first element.
                        Ok(Some(arr.pop_front().unwrap()))
                    }
                }
            } else {
                Err(OpError::TypeMismatch)
            }
        } else {
            Err(OpError::KeyAbsent)
        }
    }

    pub fn lpop_add_block_task(&mut self, task: LpopBlockedTask) {
        let mut lock = self.lpop_blocked_task.lock().unwrap();
        lock.push(task);
    }

    /// Get the type of value specified by `key`
    ///
    /// If key not present, return `OpError::KeyAbsent`.
    pub fn get_value_type(&self, key: impl AsRef<str>) -> OpResult<&'static str> {
        let lock = self.inner.lock().unwrap();
        match lock.data.get(key.as_ref()).map(|cell| cell.live_value()) {
            Some(LiveValue::Live(v)) => Ok(v.simple_name()),
            Some(LiveValue::Expired) | Some(LiveValue::Absent) | None => {
                if lock.stream.contains_key(key.as_ref()) {
                    Ok("stream")
                } else {
                    // Expired.
                    Err(OpError::KeyAbsent)
                }
            }
        }
    }

    pub fn stream_add_value(
        &mut self,
        key: String,
        stream_id: StreamId,
        value: Vec<Value>,
    ) -> OpResult<StreamId> {
        let mut lock = self.inner.lock().unwrap();
        let (time_id, seq_id) = match stream_id {
            StreamId::Value { time_id, seq_id } => (time_id, seq_id),
            StreamId::Auto => (
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                0,
            ),
            StreamId::PartialAuto(time_id) => {
                let mut seq_id = lock.get_next_seq_id(key.as_str(), time_id);
                if time_id == 0 && seq_id == 0 {
                    seq_id = 1;
                }
                (time_id, seq_id)
            }
        };
        match lock.stream.get_mut(key.as_str()) {
            Some(s) => s.add_entry(time_id, seq_id, value),
            None => {
                let mut s = Stream::new();
                let ret = s.add_entry(time_id, seq_id, value);
                lock.stream.insert(key, s);
                ret
            }
        }
    }
}
