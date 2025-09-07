use serde_redis::{Array, BulkString, SimpleError, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{Storage, StreamId},
};

fn parse_stream_id(value: String) -> Option<StreamId> {
    match value.split_once('-') {
        Some((raw_time_id, raw_seq_id)) => {
            match (raw_time_id.parse::<u64>(), raw_seq_id.parse::<u64>()) {
                (Ok(time_id), Ok(seq_id)) => Some(StreamId::new(time_id, seq_id + 1)),
                _ => None,
            }
        }
        None => None,
    }
}

pub(super) async fn handle_xread_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command XREAD");
    // Read the "streams" argument after "XREAD".
    let _streams = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XREAD",
            args: args.clone(),
        })?;

    let mut stream_names = vec![];
    let mut stream_ids = vec![];

    while !args.is_empty() {
        let s = args
            .pop_front_bulk_string()
            .ok_or_else(|| ServerError::InvalidArgs {
                cmd: "XREAD",
                args: args.clone(),
            })?;

        // Simple distinguish stream names and stream keys by the delimiter.
        if s.contains("-") {
            let id = parse_stream_id(s).ok_or_else(|| ServerError::InvalidArgs {
                cmd: "XREAD",
                args: args.clone(),
            })?;
            stream_ids.push(id);
        } else {
            stream_names.push(s);
        }
    }

    if stream_ids.len() != stream_names.len() {
        let content = Value::SimpleError(SimpleError::with_prefix(
            "EARGS",
            "stream name and stream keys have different count",
        ));
        conn.write_value(&content).await?;
        return Ok(());
    }

    let end = StreamId::Auto;

    let queries = stream_names.into_iter().zip(stream_ids).collect::<Vec<_>>();

    let mut results = vec![]; // Value::Array(Array::with_values());

    for query in queries {
        conn.log(format!("XREAD key={}, {:?}..={:?}", query.0, query.1, end));
        let v = storage
            .stream_get_range(query.0.clone(), query.1, end.clone())
            .map_err(|x| x.to_message())
            .unwrap();

        let arr = Value::Array(Array::with_values(vec![
            Value::BulkString(BulkString::new(query.0)),
            v,
        ]));
        results.push(arr);
    }

    let value = Value::Array(Array::with_values(results));

    conn.write_value(&value).await?;
    Ok(())
}
