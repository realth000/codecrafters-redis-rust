use serde_redis::{Array, SimpleString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
};

pub(super) async fn handle_replconf_command(
    conn: &mut Conn<'_>,
    mut args: Array,
) -> ServerResult<()> {
    conn.log("run command REPLCONF");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "REPLCONF",
            args: args.clone(),
        })?;

    let value = match key.as_str() {
        "listening-port" | "capa" => Value::SimpleString(SimpleString::new("OK")),
        v => {
            conn.log(format!("invalid argument {v}"));
            return Err(ServerError::InvalidArgs {
                cmd: "REPLCONF",
                args: args,
            });
        }
    };
    conn.write_value(value).await
}
