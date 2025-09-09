use serde_redis::{SimpleString, Value};

use crate::{conn::Conn, error::ServerResult, storage::Storage};

pub(super) async fn handle_multi_command(
    conn: &mut Conn<'_>,
    _storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command MULTI");
    let value = Value::SimpleString(SimpleString::new("OK"));
    conn.write_value(&value).await
}
