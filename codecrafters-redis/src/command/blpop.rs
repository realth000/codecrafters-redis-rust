use std::{sync::WaitTimeoutResult, time::Duration};

use serde_redis::{Array, BulkString, SimpleError, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{LpopBlockedTask, OpError, Storage},
};

pub(super) async fn handle_blpop_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command BLPOP");
    conn.log("BLPOP");

    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "BLPOP",
            args: args.clone(),
        })?;

    if args.is_empty() {
        let value = Value::SimpleError(SimpleError::with_prefix("EARG", "empty list args"));
        conn.write_value(&value).await?;
        return Ok(());
    }

    let block_duration = match args.pop_front_bulk_string() {
        Some(s) if s.as_str() == "0" => None,
        Some(s) => match s.parse::<f64>() {
            Ok(v) => Some(Duration::from_secs_f64(v)),
            Err(e) => {
                let value = Value::SimpleError(SimpleError::with_prefix(
                    "EARG",
                    format!("faied to parse timeout duration: {e}"),
                ));
                conn.write_value(&value).await?;
                return Ok(());
            }
        },
        None => todo!(),
    };

    args.pop_front_bulk_string().and_then(|s| {
        if s == "0" {
            None
        } else {
            s.parse::<f64>()
                .ok()
                .map(|d| Duration::from_secs((d * 1000.0) as u64))
        }
    });

    let content = match storage.array_pop_front(key.clone(), None) {
        Ok(Some(v)) => v,
        Ok(None) | Err(OpError::KeyAbsent) => {
            // No value in list, block here.
            let task = LpopBlockedTask::new(key.clone());
            let handle = task.clone_handle();
            storage.lpop_add_block_task(task);
            let mut lock = handle.lock.lock().unwrap();
            conn.log(format!(
                "BLPOP: value not present, blocking connection for {block_duration:?}"
            ));
            let mut wait_result: Option<Value> = None;
            match block_duration {
                Some(d) => {
                    // Waiting for some time.
                    let mut timeout_result: WaitTimeoutResult;
                    loop {
                        (lock, timeout_result) = handle.condvar.wait_timeout(lock, d).unwrap();
                        if timeout_result.timed_out() {
                            // Timeout.
                            conn.log("BLPOP: block timeout");
                            break;
                        }

                        if lock.is_some() {
                            // Waited the result.
                            wait_result = lock.take();
                            break;
                        }
                    }
                }
                None => {
                    // Waiting forever.
                    loop {
                        lock = handle.condvar.wait(lock).unwrap();
                        if lock.is_some() {
                            wait_result = lock.take();
                            break;
                        }
                    }
                }
            };

            Value::Array(Array::with_values(vec![
                Value::BulkString(BulkString::new(key)),
                wait_result.unwrap_or_else(|| Value::BulkString(BulkString::null())),
            ]))
        }
        Err(e) => e.to_message(),
    };

    conn.log(format!(
        ">>> BLPOP resp: {}",
        String::from_utf8(serde_redis::to_vec(&content).unwrap()).unwrap()
    ));
    conn.write_value(&content).await
}
