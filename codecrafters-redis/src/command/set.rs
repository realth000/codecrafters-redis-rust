use std::time::Duration;

use serde_redis::{Array, Integer, SimpleString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_set_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command SET");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "SET",
            args: args.clone(),
        })?;
    let value = match args.pop_front().unwrap() {
        Value::SimpleString(s) => match s.value().parse::<i64>() {
            Ok(v) => Value::Integer(Integer::new(v)),
            _ => Value::SimpleString(s),
        },
        Value::BulkString(b) => match b
            .clone()
            .take()
            .and_then(|x| String::from_utf8(x).ok())
            .and_then(|x| x.parse::<i64>().ok())
        {
            Some(v) => Value::Integer(Integer::new(v)),
            _ => Value::BulkString(b),
        },
        v => v,
    };
    conn.log(format!("SET {key:?}={value:?}"));

    // Duration till expire. None value means never expire.
    let mut duration = None;
    match args.pop_front_bulk_string() {
        Some(v) => match v.to_lowercase().as_str() {
            "px" => {
                duration = args
                    .pop_front_bulk_string()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| ServerError::InvalidArgs {
                        cmd: "SET",
                        args: args.clone(),
                    })
                    .map(|d| Some(Duration::from_millis(d)))?
            }

            _ => {
                return Err(ServerError::InvalidArgs {
                    cmd: "SET",
                    args: args.clone(),
                })
            }
        },
        None => { /* No more args */ }
    }

    storage.insert(key, value, duration);
    let value = Value::SimpleString(SimpleString::new("OK"));
    conn.write_value(value).await
}
