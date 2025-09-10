use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_redis::{Array, Integer, SimpleError, SimpleString, Value};
use tokio::{net::TcpStream, sync::oneshot};

use stream::Stream;

mod stream;

pub use stream::StreamId;

use crate::{error::ServerResult, replication::ReplicationState};

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

    /// Not a valid integer in storage, or the value is out of range.
    ///
    /// Similar to `TypeMismatch` but more specific to integer related process.
    InvalidInteger,
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
            OpError::InvalidInteger => {
                SimpleError::with_prefix("ERR", "value is not an integer or out of range")
            }
        };

        Value::SimpleError(e)
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

enum LiveValueRef<'a> {
    /// Value exists and is alive.
    Live(&'a mut Value),

    /// Value exists but is expired.
    Expired,
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

    fn live_value_mut(&mut self) -> LiveValueRef<'_> {
        match self.expiration {
            Some(d) => {
                if d > SystemTime::now() {
                    LiveValueRef::Live(&mut self.value)
                } else {
                    // Expired.
                    LiveValueRef::Expired
                }
            }
            None => LiveValueRef::Live(&mut self.value),
        }
    }
}

pub(crate) struct LpopBlockedTask {
    key: String,
    sender: oneshot::Sender<Value>,
}

impl LpopBlockedTask {
    pub fn new(key: String) -> (Self, oneshot::Receiver<Value>) {
        let (sender, recver) = oneshot::channel::<Value>();

        let s = Self { key, sender };
        (s, recver)
    }
}

/// Target stream listening to.
#[derive(Debug)]
pub(crate) struct XreadBlockedTarget {
    /// Key of the string.
    key: String,

    start_time_id: u64,

    start_seq_id: u64,

    only_new_entry: bool,
}

impl XreadBlockedTarget {
    /// Build a target that specified with entry id.
    pub fn with_id(key: String, start_time_id: u64, start_seq_id: u64) -> Self {
        Self {
            key,
            start_time_id,
            start_seq_id,
            only_new_entry: false,
        }
    }

    /// Build a target that only excepting new entries.
    pub fn with_new_entry(key: String) -> Self {
        Self {
            key,
            start_time_id: 0,
            start_seq_id: 0,
            only_new_entry: true,
        }
    }
}

/// A blocked XREAD task.
///
/// Each instance indicates that a redis client is using XREAD to waiting
/// for incoming data, waiting FOREVER.
pub(crate) struct XreadBlockedTask {
    /// Each XREAD command can listen to multiple streams, each stream is a
    /// single `XreadBlockedTarget`.
    ///
    /// Once any of the `targets` are feeded with data, the listening process
    /// shall be done.
    targets: Vec<XreadBlockedTarget>,

    /// The channel to send data back once any of the `targets` are feeded.
    ///
    /// Send back the target name and the corresponding value.
    sender: oneshot::Sender<(Vec<String>, Value)>,
}

impl XreadBlockedTask {
    pub fn new(
        targets: Vec<XreadBlockedTarget>,
        sender: oneshot::Sender<(Vec<String>, Value)>,
    ) -> Self {
        Self { targets, sender }
    }

    /// Find all streams in current task that accept the incoming data with
    /// `start_time_id` and `start_seq_id`.
    ///
    /// Return the name of all those streams.
    fn extract_target_waiting_for_id(
        &mut self,
        key: &str,
        start_time_id: u64,
        start_seq_id: u64,
    ) -> Vec<String> {
        self.targets
            .extract_if(.., |task| {
                !task.only_new_entry
                    && task.key == key
                    && task.start_time_id <= start_time_id
                    && task.start_seq_id <= start_seq_id
            })
            .map(|x| x.key.clone())
            .collect::<Vec<_>>()
    }

    /// Find all streams in current task that only accept data saved in new entry.
    ///
    /// Return the name of all those streams.
    fn extract_target_waiting_for_new_entry(&mut self, key: &str) -> Vec<String> {
        self.targets
            .extract_if(.., |task| task.only_new_entry && task.key == key)
            .map(|x| x.key.clone())
            .collect::<Vec<_>>()
    }
}

#[derive(Clone)]
pub(crate) struct Storage {
    inner: Arc<Mutex<StorageInner>>,
    lpop_blocked_task: Arc<Mutex<Vec<LpopBlockedTask>>>,
    xread_blocked_task: Arc<Mutex<Vec<XreadBlockedTask>>>,
    replication: Arc<Mutex<ReplicationState>>,
}

struct StorageInner {
    data: HashMap<String, ValueCell>,
    stream: HashMap<String, Stream>,
}

impl StorageInner {
    fn get_next_seq_id(&self, key: impl AsRef<str>, time_id: u64) -> u64 {
        self.stream
            .get(key.as_ref())
            .map_or_else(|| 0, |s| s.get_next_seq_id(time_id))
    }
}

impl Storage {
    pub fn new(master: Option<(Ipv4Addr, u16)>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StorageInner {
                data: HashMap::new(),
                stream: HashMap::new(),
            })),
            lpop_blocked_task: Arc::new(Mutex::new(vec![])),
            xread_blocked_task: Arc::new(Mutex::new(vec![])),
            replication: Arc::new(Mutex::new(ReplicationState::new(master))),
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
                    .as_millis() as u64,
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

        let ret = match lock.stream.get_mut(key.as_str()) {
            Some(s) => s.add_entry(time_id, seq_id, value.clone()),
            None => {
                let mut s = Stream::new();
                let ret = s.add_entry(time_id, seq_id, value.clone());
                lock.stream.insert(key.clone(), s);
                ret
            }
        };

        if let Ok((ret, saved_in_new_entry)) = ret {
            // Feed all waiting XREAD tasks.
            // Return the value to all XREAD tasks.
            // ref: https://redis.io/docs/latest/commands/xread/#how-multiple-clients-blocked-on-a-single-stream-are-served
            let mut feed_lock = self.xread_blocked_task.lock().unwrap();
            let mut removed_id = None;
            for (idx, task) in feed_lock.iter_mut().rev().enumerate() {
                let mut target_tasks = task.extract_target_waiting_for_id(&key, time_id, seq_id);
                if saved_in_new_entry {
                    println!(
                        "[storage] stream: checking data in new entry for key {} in task {:?}",
                        key, task.targets
                    );
                    target_tasks.append(&mut task.extract_target_waiting_for_new_entry(&key));
                }
                if target_tasks.is_empty() {
                    continue;
                }

                removed_id = Some((idx, target_tasks));
                break;
            }

            if let Some((idx, target_tasks)) = removed_id {
                let task = feed_lock.remove(idx);
                let values_with_id = Value::Array(Array::with_values(vec![
                    Value::SimpleString(SimpleString::new(format!("{}-{}", time_id, seq_id))),
                    Value::Array(Array::with_values(value.clone())),
                ]));
                task.sender.send((target_tasks, values_with_id)).unwrap();
            }
            Ok(ret)
        } else {
            Err(ret.unwrap_err())
        }
    }

    pub fn stream_get_range(&self, key: String, start: StreamId, end: StreamId) -> OpResult<Value> {
        let lock = self.inner.lock().unwrap();
        match lock.stream.get(key.as_str()) {
            Some(s) => s.get_range(start, end),
            None => Err(OpError::KeyAbsent),
        }
    }

    pub fn xread_add_block_task(&mut self, task: XreadBlockedTask) {
        let mut lock = self.xread_blocked_task.lock().unwrap();
        lock.push(task);
    }

    pub fn integer_increase(&mut self, key: String) -> OpResult<Value> {
        let mut lock = self.inner.lock().unwrap();
        match lock
            .data
            .get_mut(key.as_str())
            .map(|cell| cell.live_value_mut())
        {
            Some(LiveValueRef::Live(value)) => match value {
                Value::Integer(integer) => {
                    integer.increase(1);
                    Ok(Value::Integer(integer.to_owned()))
                }
                _ => Err(OpError::InvalidInteger),
            },
            Some(LiveValueRef::Expired) | None => {
                let value = Value::Integer(Integer::new(1));
                // Insert new value.
                lock.data.insert(
                    key,
                    ValueCell {
                        value: value.clone(),
                        expiration: None,
                    },
                );

                Ok(value)
            }
        }
    }

    pub(crate) fn info(&self) -> Value {
        let lock = self.replication.lock().unwrap();
        lock.info()
    }

    pub(crate) async fn replica_handshake(&self, port: u16) -> ServerResult<()> {
        let lock = self.replication.lock().unwrap();
        lock.handshake(port).await
    }

    pub(crate) fn replica_master_id(&self) -> String {
        let lock = self.replication.lock().unwrap();
        lock.id()
    }

    pub(crate) async fn replica_sync(&mut self, args: Array) -> ServerResult<()> {
        let mut lock = self.replication.lock().unwrap();
        lock.sync_command(args).await
    }

    pub(crate) fn set_replica(&mut self, socket: TcpStream) {
        let mut lock = self.replication.lock().unwrap();
        lock.set_replica(socket);
    }
}
