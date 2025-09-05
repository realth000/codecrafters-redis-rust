use serde_redis::{Array, BulkString, Value};
use tokio::io::AsyncWriteExt;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
};

pub(super) async fn handle_echo_command(conn: &mut Conn<'_>, mut args: Array) -> ServerResult<()> {
    conn.log("run command ECHO");
    match args.pop() {
        Some(Value::BulkString(mut s)) if !s.is_null() => {
            let msg = s.take().unwrap();
            let msg2 = BulkString::new(msg);
            let content = serde_redis::to_vec(&msg2).map_err(ServerError::SerdeError)?;
            conn.stream
                .write(&content)
                .await
                .map_err(ServerError::IoError)?;
            conn.log(format!("ECHO {content:?}"));
            Ok(())
        }
        _ => Err(ServerError::InvalidArgs {
            cmd: "ECHO",
            args: args,
        }),
    }
}
