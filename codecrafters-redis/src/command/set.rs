use serde_redis::{Array, SimpleString, Value};
use tokio::io::AsyncWriteExt;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_set_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command SET");
    let key = match args.pop_front() {
        Some(Value::BulkString(mut s)) => {
            String::from_utf8(s.take().unwrap()).map_err(ServerError::FromUtf8Error)?
        }
        _ => {
            return Err(ServerError::InvalidArgs {
                cmd: "SET",
                args: args,
            })
        }
    };
    let value = args.pop_front().unwrap();
    conn.log(format!("SET {key:?}={value:?}"));

    storage.insert(key, value);
    let msg2 = SimpleString::new("OK");
    let content = serde_redis::to_vec(&msg2).map_err(ServerError::SerdeError)?;
    conn.stream
        .write(&content)
        .await
        .map_err(ServerError::IoError)?;
    Ok(())
}
