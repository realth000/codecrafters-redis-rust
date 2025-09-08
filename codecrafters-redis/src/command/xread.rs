use std::time::Duration;

use serde_redis::{Array, BulkString, SimpleError, Value};
use tokio::sync::oneshot;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{Storage, StreamId, XreadBlockedTarget, XreadBlockedTask},
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
    let subcommand = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "XREAD",
            args: args.clone(),
        })?;

    let mut block_duration = None;

    if subcommand == "block" {
        // Run in block mode.
        let d = args
            .pop_front_bulk_string()
            .and_then(|x| x.parse::<u64>().ok())
            .ok_or_else(|| ServerError::InvalidArgs {
                cmd: "XREAD",
                args: args.clone(),
            })?;
        block_duration = Some(d);

        // Read the "streams" argument after "XREAD".
        let _stream = args
            .pop_front_bulk_string()
            .ok_or_else(|| ServerError::InvalidArgs {
                cmd: "XREAD",
                args: args.clone(),
            })?;
    }

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

    let mut query_result = vec![];

    match block_duration {
        Some(0) => {
            // Block till notify.
            let block_targets = queries
                .iter()
                .map(|q| {
                    let (time_id, seq_id) = match q.1 {
                        StreamId::Value { time_id, seq_id } => (time_id, seq_id),
                        StreamId::Auto | StreamId::PartialAuto(_) => {
                            unreachable!("Auto id shall not happen here")
                        }
                    };
                    XreadBlockedTarget::new(q.0.to_owned(), time_id, seq_id)
                })
                .collect::<Vec<_>>();
            let (sender, recver) = oneshot::channel::<(Vec<String>, Value)>();
            let block_task = XreadBlockedTask::new(block_targets, sender);
            storage.xread_add_block_task(block_task);
            match recver.await {
                Ok((keys, value)) => {
                    conn.log(format!(
                        "XREAD [block forever] received value for keys: {keys:?} = {value:?}"
                    ));
                    for key in keys.into_iter() {
                        let arr = Value::Array(Array::with_values(vec![
                            Value::BulkString(BulkString::new(key)),
                            Value::Array(Array::with_values(vec![value.clone()])),
                        ]));
                        query_result.push(arr);
                    }
                }
                Err(e) => {
                    conn.log(format!(
                        "failed to receive the result for forever blocking task: {e:?}"
                    ));
                    return Ok(());
                }
            }
        }
        _ => {
            // Wait for some time, or do not wait.
            if let Some(v) = block_duration {
                // Wait for some time.
                tokio::time::sleep(Duration::from_millis(v)).await;
            }

            for query in queries {
                conn.log(format!("XREAD key={}, {:?}..={:?}", query.0, query.1, end));
                let v = storage
                    .stream_get_range(query.0.clone(), query.1, end.clone())
                    .map_err(|x| x.to_message())
                    .unwrap();

                if let Value::Array(arr) = &v {
                    if arr.is_empty() {
                        continue;
                    }
                }

                let arr = Value::Array(Array::with_values(vec![
                    Value::BulkString(BulkString::new(query.0)),
                    v,
                ]));
                query_result.push(arr);
            }
        }
    }

    let value = if query_result.is_empty() {
        Value::Array(Array::null())
    } else {
        Value::Array(Array::with_values(query_result))
    };

    conn.write_value(&value).await
}
