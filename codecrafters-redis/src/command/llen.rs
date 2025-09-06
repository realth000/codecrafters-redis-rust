use serde_redis::{Array, Integer, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::{OpError, Storage},
};

pub(super) async fn handle_llen_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command LLEN");
    conn.log("LLEN");

    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "LLEN",
            args: args.clone(),
        })?;

    let content = match storage.get_array_length(key) {
        Ok(v) => Value::Integer(Integer::new(v as i64)),
        Err(e) => match e {
            OpError::KeyAbsent => Value::Integer(Integer::new(0)),
            _ => e.to_message(),
        },
    };

    conn.write_value(&content).await
}
