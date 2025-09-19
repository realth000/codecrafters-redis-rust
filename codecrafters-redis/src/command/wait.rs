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
    rep: ReplicationState,
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

    conn.log(format!("wait with count={count}, duration={duration:?}"));

    let replica_count = rep.replica_count();
    if replica_count >= count {
        let value = Value::Integer(Integer::new(replica_count as i64));
        conn.sync_value(value).await
    } else {
        tokio::time::sleep(duration).await;
        let replica_count = rep.replica_count();
        let value = Value::Integer(Integer::new(replica_count as i64));
        conn.sync_value(value).await
    }
}
