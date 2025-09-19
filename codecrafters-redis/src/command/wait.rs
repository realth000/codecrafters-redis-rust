use std::time::Duration;

use serde_redis::{Array, Integer, Value};

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
    replication::ReplicationState,
};

pub(super) async fn handle_wait_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    mut rep: ReplicationState,
) -> ServerResult<()> {
    conn.log("run command WAIT");

    let count = args
        .pop_front_bulk_string()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "WAIT",
            args: args.clone(),
        })?;

    let duration = args
        .pop_front_bulk_string()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| ServerError::InvalidArgs {
            cmd: "WAIT",
            args: args.clone(),
        })
        .map(|d| Duration::from_millis(d))?;

    conn.log(format!("[wait] count={count}, duration={duration:?}"));

    let replica_count = rep.replica_count(conn.id);
    let v = if replica_count >= count {
        conn.log(format!("[wait] replica count is {replica_count}"));
        let value = Value::Integer(Integer::new(replica_count as i64));
        conn.sync_value(value).await
    } else {
        conn.log("[wait] wait for duration");
        tokio::time::sleep(duration).await;
        conn.log("[wait] wait for duration end");
        let replica_count = rep.replica_count(conn.id);
        let value = Value::Integer(Integer::new(replica_count as i64));
        conn.sync_value(value).await
    };
    rep.replica_reset(conn.id);
    v
}
