use serde_redis::{Array, SimpleString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_type_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command TYPE");
    conn.log("TYPE");

    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "TYPE",
            args: args.clone(),
        })?;

    let name = storage.get_value_type(key).unwrap_or("none");
    let value = Value::SimpleString(SimpleString::new(name));

    conn.write_value(&value).await
}
