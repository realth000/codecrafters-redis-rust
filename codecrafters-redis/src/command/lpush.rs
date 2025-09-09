use serde_redis::{Array, Integer, SimpleError, SimpleString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_lpush_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command LPUSH");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "LPUSH",
            args: args.clone(),
        })?;

    let mut values = Array::new_empty();

    while let Some(v) = args.pop_front_bulk_string() {
        values.push_back(Value::SimpleString(SimpleString::new(v)));
    }

    conn.log(format!("RPUSH {key:?}={values:?}"));

    let value = if values.is_empty() {
        Value::SimpleError(SimpleError::with_prefix("EARG", "empty list args"))
    } else {
        match storage.insert_list(key, values, true, true) {
            Ok(v) => Value::Integer(Integer::new(v as i64)),
            Err(e) => e.to_message(),
        }
    };

    conn.write_value(value).await
}
