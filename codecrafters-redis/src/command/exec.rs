use serde_redis::{Array, SimpleError, Value};

use crate::{conn::Conn, error::ServerResult, storage::Storage};

pub(super) async fn handle_exec_command(
    conn: &mut Conn<'_>,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command EXEC");
    let value = if conn.in_transaction() {
        let result = conn.commit_transaction(storage).await?;
        if result.is_empty() {
            // Return an empty array if the transaction is empty.
            Value::Array(Array::new_empty())
        } else {
            Value::Array(Array::with_values(result))
        }
    } else {
        Value::SimpleError(SimpleError::with_prefix("ERR", "EXEC without MULTI"))
    };

    conn.write_value(value).await
}
