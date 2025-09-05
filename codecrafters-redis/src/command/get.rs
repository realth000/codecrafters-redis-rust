use serde_redis::{Array, Null, Value};
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
    let key = match args.pop() {
        Some(Value::BulkString(mut s)) => {
            String::from_utf8(s.take().unwrap()).map_err(ServerError::FromUtf8Error)?
        }
        _ => {
            return Err(ServerError::InvalidArgs {
                cmd: "GET",
                args: args,
            })
        }
    };

    let value = storage.get(&key).unwrap_or_else(|| Value::Null(Null));
    conn.log(format!("GET {key:?}={value:?}"));
    let content = serde_redis::to_vec(&value).map_err(ServerError::SerdeError)?;
    conn.stream
        .write(&content)
        .await
        .map_err(ServerError::IoError)?;
    Ok(())
}
