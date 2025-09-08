use serde_redis::Array;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

pub(super) async fn handle_incr_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command INCR");
    let key = args
        .pop_front_bulk_string()
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "INCR",
            args: args.clone(),
        })?;

    let value = match storage.integer_increase(key) {
        Ok(v) => v,
        Err(e) => e.to_message(),
    };

    conn.write_value(&value).await
}
