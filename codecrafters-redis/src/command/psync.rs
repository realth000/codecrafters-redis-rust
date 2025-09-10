use serde_redis::{Array, SimpleString, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_psync_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command PSYNC");
    let master_id = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "PSYNC",
            args: args.clone(),
        })?;

    let offset = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "PSYNC",
            args: args.clone(),
        })?;

    conn.log(format!("PSYNC {master_id} {offset}"));

    let value = Value::SimpleString(SimpleString::new(format!(
        "FULLRESYNC {} 0",
        storage.replica_master_id()
    )));

    conn.write_value(value).await
}
