use serde_redis::Array;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{Storage, StreamId},
};

fn parse_stream_id(value: String) -> Option<StreamId> {
    match value.split_once('-') {
        Some((raw_time_id, raw_seq_id)) => {
            match (raw_time_id.parse::<u64>(), raw_seq_id.parse::<u64>()) {
                (Ok(time_id), Ok(seq_id)) => Some(StreamId::new(time_id, seq_id)),
                (Ok(time_id), Err(..)) if raw_seq_id == "*" => Some(StreamId::PartialAuto(time_id)),
                _ => None,
            }
        }
        None => value
            .parse::<u64>()
            .ok()
            .map(|id| StreamId::PartialAuto(id)),
    }
}

pub(super) async fn handle_xrange_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command XRANGE");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XRANGE",
            args: args.clone(),
        })?;
    let start = args
        .pop_front_bulk_string()
        .and_then(|s| {
            if s == "-" {
                Some(StreamId::Auto)
            } else {
                parse_stream_id(s)
            }
        })
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XRANGE",
            args: args.clone(),
        })?;

    let end = args
        .pop_front_bulk_string()
        .and_then(|s| {
            if s == "+" {
                Some(StreamId::Auto)
            } else {
                parse_stream_id(s)
            }
        })
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XRANGE",
            args: args.clone(),
        })?;

    conn.log(format!("XRANGE {start:?}..={end:?}"));

    let value = storage
        .stream_get_range(key, start, end)
        .map_err(|x| x.to_message())
        .unwrap();

    conn.write_value(&value).await?;
    Ok(())
}
