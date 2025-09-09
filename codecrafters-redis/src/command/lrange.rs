use serde_redis::Array;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_lrange_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command LRANGE");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "LRANGE",
            args: args.clone(),
        })?;
    let start = args
        .pop_front_bulk_string()
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "LRANGE",
            args: args.clone(),
        })?;

    let end = args
        .pop_front_bulk_string()
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "LRANGE",
            args: args.clone(),
        })?;

    conn.log(format!("LRANGE {start:?}..={end:?}"));

    let value = storage
        .lrange(key, start, end)
        .map_err(|x| x.to_message())
        .unwrap();

    conn.write_value(value).await
}
