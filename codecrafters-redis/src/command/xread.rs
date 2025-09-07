use serde_redis::{Array, BulkString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{Storage, StreamId},
};

fn parse_stream_id_exclusive(value: String) -> Option<StreamId> {
    match value.split_once('-') {
        Some((raw_time_id, raw_seq_id)) => {
            match (raw_time_id.parse::<u64>(), raw_seq_id.parse::<u64>()) {
                (Ok(time_id), Ok(seq_id)) => Some(StreamId::new(time_id, seq_id + 1)),
                (Ok(time_id), Err(..)) if raw_seq_id == "*" => {
                    Some(StreamId::PartialAuto(time_id + 1))
                }
                _ => None,
            }
        }
        None => value
            .parse::<u64>()
            .ok()
            .map(|id| StreamId::PartialAuto(id)),
    }
}

pub(super) async fn handle_xread_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command XREAD");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XREAD",
            args: args.clone(),
        })?;
    let _streams = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XREAD",
            args: args.clone(),
        })?;
    let start = args
        .pop_front_bulk_string()
        .and_then(|s| parse_stream_id_exclusive(s))
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XREAD",
            args: args.clone(),
        })?;

    let end = StreamId::Auto;
    conn.log(format!("XREAD {start:?}..={end:?}"));

    let value = storage
        .stream_get_range(key.clone(), start, end)
        .map_err(|x| x.to_message())
        .unwrap();

    let value = Value::Array(Array::with_values(vec![
        Value::BulkString(BulkString::new(key)),
        value,
    ]));

    conn.write_value(&value).await?;
    Ok(())
}
