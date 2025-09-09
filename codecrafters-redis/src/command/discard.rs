use serde_redis::{SimpleError, SimpleString, Value};

use crate::{conn::Conn, error::ServerResult};

pub(super) async fn handle_discard_command(conn: &mut Conn<'_>) -> ServerResult<()> {
    conn.log("run command DISCARD");
    let value = if conn.in_transaction() {
        conn.abort_transaction();
        Value::SimpleString(SimpleString::new("OK"))
    } else {
        Value::SimpleError(SimpleError::with_prefix("ERR", "DISCARD without MULTI"))
    };

    conn.write_value(value).await
}
