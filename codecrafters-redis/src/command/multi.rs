use serde_redis::{SimpleError, SimpleString, Value};

use crate::{conn::Conn, error::ServerResult, storage::Storage};

pub(super) async fn handle_multi_command(
    conn: &mut Conn<'_>,
    _storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command MULTI");
    let value = if conn.in_transaction() {
        Value::SimpleError(SimpleError::with_prefix("ETRANS", "alreay in transaction"))
    } else {
        conn.enter_transaction();
        Value::SimpleString(SimpleString::new("OK"))
    };
    conn.write_value(value).await
}
