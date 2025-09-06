use serde_redis::{Array, Integer, SimpleError, SimpleString, Value};
use tokio::io::AsyncWriteExt;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_rpush_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command RPUSH");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "RPUSH",
            args: args.clone(),
        })?;

    let mut values = Array::new_empty();

    while let Some(v) = args.pop_front_bulk_string() {
        values.push_back(Value::SimpleString(SimpleString::new(v)));
    }

    conn.log(format!("RPUSH {key:?}={values:?}"));

    let content = if values.is_empty() {
        serde_redis::to_vec(&SimpleError::with_prefix("EARG", "empty list args")).unwrap()
    } else {
        match storage.insert_list(key, values, true, false) {
            Ok(v) => serde_redis::to_vec(&Value::Integer(Integer::new(v as i64))).unwrap(),
            Err(e) => e.to_message_bytes(),
        }
    };

    conn.stream
        .write(&content)
        .await
        .map_err(ServerError::IoError)?;
    Ok(())
}
