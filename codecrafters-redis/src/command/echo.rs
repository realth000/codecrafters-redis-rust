use serde_redis::{Array, BulkString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
};

pub(super) async fn handle_echo_command(conn: &mut Conn<'_>, mut args: Array) -> ServerResult<()> {
    conn.log("run command ECHO");
    match args.pop() {
        Some(Value::BulkString(mut s)) if !s.is_null() => {
            let msg = s.take().unwrap();
            let value = Value::BulkString(BulkString::new(msg));
            conn.log(format!("ECHO {value:?}"));
            conn.write_value(value).await?;
            Ok(())
        }
        _ => Err(ServerError::InvalidArgs {
            cmd: "ECHO",
            args: args,
        }),
    }
}
