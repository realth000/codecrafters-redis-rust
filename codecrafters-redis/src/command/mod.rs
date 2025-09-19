use serde_redis::{Array, SimpleError, SimpleString, Value};

use crate::{
    command::{
        blpop::handle_blpop_command, discard::handle_discard_command, echo::handle_echo_command,
        exec::handle_exec_command, get::handle_get_command, incr::handle_incr_command,
        info::handle_info_command, llen::handle_llen_command, lpop::handle_lpop_command,
        lpush::handle_lpush_command, lrange::handle_lrange_command, multi::handle_multi_command,
        ping::handle_ping_command, psync::handle_psync_command, replconf::handle_replconf_command,
        rpush::handle_rpush_command, set::handle_set_command, tipe::handle_type_command,
        xadd::handle_xadd_command, xrange::handle_xrange_command, xread::handle_xread_command,
    },
    conn::Conn,
    error::{ServerError, ServerResult},
    replication::ReplicationState,
    storage::Storage,
};

mod blpop;
mod discard;
mod echo;
mod exec;
mod get;
mod incr;
mod info;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod multi;
mod ping;
mod psync;
mod replconf;
mod rpush;
mod set;
mod tipe;
mod xadd;
mod xrange;
mod xread;

pub(crate) enum DispatchResult {
    /// Nothing special to do.
    None,

    /// Save the connection as replica connection.
    Replica,

    /// Current command need to be synced to replica.
    ///
    /// * If current redis instance is a replica node, apply that command on "myself",
    ///   now "myself" is the redis node that need need to be synced.
    /// * If current redis instance is a master node, record that this command should
    ///   send to all replica nodes that want to sync their data.
    ReplicaSync,
}

#[must_use]
pub(crate) async fn dispatch_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
    rep: ReplicationState,
) -> ServerResult<DispatchResult> {
    if args.is_null_or_empty() {
        return Err(ServerError::InvalidMessage("args is null or empty".into()));
    }

    if conn.in_transaction() {
        // In Transcation, record commands and wait for the `EXEC` command to execute.
        let ele = args.pop_front();
        match ele {
            Some(Value::BulkString(mut cmd)) => match cmd.take() {
                Some(cmd) => {
                    let cmd = String::from_utf8(cmd)
                        .map_err(|e| ServerError::InvalidCommand(format!("{e:?}")))?
                        .to_uppercase();
                    match cmd.as_str() {
                        "MULTI" => {
                            // Nested transaction is not allowed, `MULTI` can NOT be called
                            // within a transaction.
                            let value = Value::SimpleError(SimpleError::with_prefix(
                                "ETRANS",
                                "alreayd in transaction",
                            ));
                            conn.write_value(value).await?;
                            Ok(DispatchResult::None)
                        }
                        "EXEC" => {
                            // Execute all commands in transaction.
                            // This also leaves the transaction state for current connection.
                            handle_exec_command(conn, storage).await?;
                            Ok(DispatchResult::None)
                        }
                        "DISCARD" => {
                            handle_discard_command(conn).await?;
                            Ok(DispatchResult::None)
                        }
                        _ => {
                            conn.add_to_transaction(cmd, args);
                            let value = Value::SimpleString(SimpleString::new("QUEUED"));
                            conn.write_value(value).await?;
                            Ok(DispatchResult::None)
                        }
                    }
                }
                None => Err(ServerError::InvalidCommand(
                    "command is null BulkString".into(),
                )),
            },
            v => Err(ServerError::InvalidMessage(format!(
                "invalid command format: {v:?}"
            ))),
        }
    } else {
        let ele = args.pop_front();
        match ele {
            Some(Value::BulkString(mut cmd)) => match cmd.take() {
                Some(cmd) => {
                    let cmd = String::from_utf8(cmd)
                        .map_err(|e| ServerError::InvalidCommand(format!("{e:?}")))?
                        .to_uppercase();
                    match cmd.as_str() {
                        "MULTI" => {
                            if conn.in_transaction() {
                                let value = Value::SimpleError(SimpleError::with_prefix(
                                    "ETRANS",
                                    "alreayd in transaction",
                                ));
                                conn.write_value(value).await?;
                                Ok(DispatchResult::None)
                            } else {
                                handle_multi_command(conn, storage).await?;
                                Ok(DispatchResult::None)
                            }
                        }
                        "EXEC" => {
                            handle_exec_command(conn, storage).await?;
                            Ok(DispatchResult::None)
                        }
                        "DISCARD" => {
                            handle_discard_command(conn).await?;
                            Ok(DispatchResult::None)
                        }

                        "INFO" => {
                            // INFO command handles things more than about replication,
                            // but we only implement them for now.
                            handle_info_command(conn, rep).await?;
                            Ok(DispatchResult::None)
                        }
                        "REPLCONF" => {
                            handle_replconf_command(conn, args).await?;
                            Ok(DispatchResult::None)
                        }
                        "PSYNC" => {
                            handle_psync_command(conn, args, rep).await?;
                            Ok(DispatchResult::Replica)
                        }
                        v => dispatch_normal_command(conn, v, args, storage).await,
                    }
                }
                None => Err(ServerError::InvalidCommand(
                    "command is null BulkString".into(),
                )),
            },
            v => Err(ServerError::InvalidMessage(format!(
                "invalid command format: {v:?}"
            ))),
        }
    }
}

#[must_use]
pub(crate) async fn dispatch_normal_command(
    conn: &mut Conn<'_>,
    cmd: &str,
    args: Array,
    storage: &mut Storage,
) -> ServerResult<DispatchResult> {
    match cmd {
        "PING" => {
            handle_ping_command(conn).await?;
            Ok(DispatchResult::None)
        }
        "ECHO" => {
            handle_echo_command(conn, args).await?;
            Ok(DispatchResult::None)
        }
        "SET" => {
            handle_set_command(conn, args, storage).await?;
            Ok(DispatchResult::ReplicaSync)
        }
        "GET" => {
            handle_get_command(conn, args, storage).await?;
            Ok(DispatchResult::None)
        }
        "RPUSH" => {
            handle_rpush_command(conn, args, storage).await?;

            Ok(DispatchResult::ReplicaSync)
        }
        "LRANGE" => {
            handle_lrange_command(conn, args, storage).await?;
            Ok(DispatchResult::None)
        }
        "LPUSH" => {
            handle_lpush_command(conn, args, storage).await?;
            Ok(DispatchResult::ReplicaSync)
        }
        "LLEN" => {
            handle_llen_command(conn, args, storage).await?;
            Ok(DispatchResult::None)
        }
        "LPOP" => {
            handle_lpop_command(conn, args, storage).await?;
            Ok(DispatchResult::ReplicaSync)
        }
        "BLPOP" => {
            handle_blpop_command(conn, args, storage).await?;
            Ok(DispatchResult::ReplicaSync)
        }
        "TYPE" => {
            handle_type_command(conn, args, storage).await?;
            Ok(DispatchResult::None)
        }
        "XADD" => {
            handle_xadd_command(conn, args, storage).await?;
            Ok(DispatchResult::ReplicaSync)
        }
        "XRANGE" => {
            handle_xrange_command(conn, args, storage).await?;
            Ok(DispatchResult::None)
        }
        "XREAD" => {
            handle_xread_command(conn, args, storage).await?;
            Ok(DispatchResult::None)
        }
        "INCR" => {
            handle_incr_command(conn, args, storage).await?;
            Ok(DispatchResult::ReplicaSync)
        }
        v => Err(ServerError::InvalidCommand(v.to_string())),
    }
}
