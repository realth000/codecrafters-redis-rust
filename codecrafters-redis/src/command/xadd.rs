use serde_redis::{Array, BulkString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
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

    let (time_id, seq_id) = args
        .pop_front_bulk_string()
        .and_then(|id| {
            let computed_ids = id
                .split_once('-')
                .map(|(time_id, seq_id)| (time_id.parse::<u32>(), seq_id.parse::<u32>()));
            match computed_ids {
                Some((Ok(v1), Ok(v2))) => Some((v1, v2)),
                _ => None,
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

    conn.log(format!(
        "XADD: key={key}, time_id={time_id}, seq_id={seq_id}"
    ));
    let value = match storage.stream_add_value(key, time_id, seq_id, values.take().unwrap()) {
        Ok(v) => Value::BulkString(v.to_bulk_string()),
        Err(e) => e.to_message(),
    };

    conn.write_value(&value).await
}
