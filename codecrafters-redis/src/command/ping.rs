use serde_redis::{SimpleString, Value};

use crate::{conn::Conn, error::ServerResult};

pub(super) async fn handle_ping_command(conn: &mut Conn<'_>) -> ServerResult<()> {
    conn.log("run command PONG");
    let value = Value::SimpleString(SimpleString::new("PONG"));
    conn.write_value(value).await
}
