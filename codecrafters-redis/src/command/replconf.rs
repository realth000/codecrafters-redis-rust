use serde_redis::{Array, BulkString, SimpleString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    replication::ReplicationState,
};

pub(super) async fn handle_replconf_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    rep: ReplicationState,
) -> ServerResult<()> {
    conn.log("run command REPLCONF");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "REPLCONF",
            args: args.clone(),
        })?;

    let value = match key.to_lowercase().as_str() {
        "listening-port" | "capa" => Value::SimpleString(SimpleString::new("OK")),
        "getack" => Value::Array(Array::with_values(vec![
            Value::BulkString(BulkString::new("REPLCONF")),
            Value::BulkString(BulkString::new("ACK")),
            Value::BulkString(BulkString::new(rep.offset().to_string().as_bytes())),
        ])),
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
