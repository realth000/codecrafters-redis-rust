use serde_redis::{Array, BulkString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{Storage, StreamId},
};

pub(super) async fn handle_xadd_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command XADD");

    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XADD",
            args: args.clone(),
        })?;

    let stream_id = args
        .pop_front_bulk_string()
        .and_then(|id| {
            if id == "*" {
                return Some(StreamId::Auto);
            }
            match id.split_once('-') {
                Some((raw_time_id, raw_seq_id)) => {
                    match (raw_time_id.parse::<u64>(), raw_seq_id.parse::<u64>()) {
                        (Ok(time_id), Ok(seq_id)) => Some(StreamId::new(time_id, seq_id)),
                        (Ok(time_id), Err(..)) if raw_seq_id == "*" => {
                            Some(StreamId::PartialAuto(time_id))
                        }
                        _ => None,
                    }
                }
                None => None,
            }
        })
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XADD",
            args: args.clone(),
        })?;

    let mut values = Array::new_empty();

    while let Some(v) = args.pop_front_bulk_string() {
        values.push_back(Value::BulkString(BulkString::new(v)));
    }

    if values.is_empty() || values.len() % 2 != 0 {
        return Err(ServerError::InvalidArgs {
            cmd: "XADD",
            args: args.clone(),
        });
    }

    conn.log(format!("XADD: key={key}, id={stream_id:?}"));
    let value = match storage.stream_add_value(key, stream_id, values.take().unwrap()) {
        Ok(v) => Value::BulkString(v.to_bulk_string()),
        Err(e) => e.to_message(),
    };

    conn.write_value(value).await
}
