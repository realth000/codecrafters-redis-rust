use serde_redis::{Array, BulkString, Integer, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{OpError, Storage},
};

pub(super) async fn handle_lpop_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command LPOP");
    conn.log("LPOP");

    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "LPOP",
            args: args.clone(),
        })?;

    let count: Option<usize>;

    if !args.is_empty() {
        count = args
            .pop_front_bulk_string()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| ServerError::InvalidArgs {
                cmd: "LRANGE",
                args: args.clone(),
            })
            .map(Some)?;
    } else {
        count = None;
    }

    let value = match storage.array_pop_front(key, count) {
        Ok(Some(v)) => v,
        Ok(None) => Value::BulkString(BulkString::null()),
        Err(e) => match e {
            OpError::KeyAbsent => Value::Integer(Integer::new(0)),
            _ => e.to_message(),
        },
    };

    conn.write_value(value).await
}
