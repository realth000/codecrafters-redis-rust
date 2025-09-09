use serde_redis::{Array, BulkString, Value};
use tokio::io::AsyncWriteExt;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_get_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command GET");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "GET",
            args: args.clone(),
        })?;

    let value = match storage.get(&key) {
        Some(value) => match value {
            Value::Integer(i) => Value::BulkString(BulkString::new(i.value().to_string())),
            _ => value,
        },
        None => Value::BulkString(BulkString::null()),
    };
    conn.log(format!("GET {key:?}={value:?}"));
    let content = serde_redis::to_vec(&value).map_err(ServerError::SerdeError)?;
    conn.stream
        .write(&content)
        .await
        .map_err(ServerError::IoError)?;
    Ok(())
}
