use serde_redis::{Array, SimpleError, SimpleString, Value};

use crate::{
    command::{
        blpop::handle_blpop_command, discard::handle_discard_command, echo::handle_echo_command,
        exec::handle_exec_command, get::handle_get_command, incr::handle_incr_command,
        llen::handle_llen_command, lpop::handle_lpop_command, lpush::handle_lpush_command,
        lrange::handle_lrange_command, multi::handle_multi_command, ping::handle_ping_command,
        rpush::handle_rpush_command, set::handle_set_command, tipe::handle_type_command,
        xadd::handle_xadd_command, xrange::handle_xrange_command, xread::handle_xread_command,
    },
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

mod blpop;
mod discard;
mod echo;
mod exec;
mod get;
mod incr;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod multi;
mod ping;
mod rpush;
mod set;
mod tipe;
mod xadd;
mod xrange;
mod xread;

pub(crate) async fn dispatch_command(
    conn: &mut Conn<'_>,
    mut args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
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
                            Ok(())
                        }
                        "EXEC" => {
                            // Execute all commands in transaction.
                            // This also leaves the transaction state for current connection.
                            handle_exec_command(conn, storage).await
                        }
                        "DISCARD" => handle_discard_command(conn).await,
                        _ => {
                            conn.add_to_transaction(cmd, args);
                            let value = Value::SimpleString(SimpleString::new("QUEUED"));
                            conn.write_value(value).await?;
                            Ok(())
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
                                Ok(())
                            } else {
                                handle_multi_command(conn, storage).await
                            }
                        }
                        "EXEC" => handle_exec_command(conn, storage).await,
                        "DISCARD" => handle_discard_command(conn).await,
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

pub(crate) async fn dispatch_normal_command(
    conn: &mut Conn<'_>,
    cmd: &str,
    args: Array,
    storage: &mut Storage,
) -> ServerResult<()> {
    match cmd {
        "PING" => handle_ping_command(conn).await,
        "ECHO" => handle_echo_command(conn, args).await,
        "SET" => handle_set_command(conn, args, storage).await,
        "GET" => handle_get_command(conn, args, storage).await,
        "RPUSH" => handle_rpush_command(conn, args, storage).await,
        "LRANGE" => handle_lrange_command(conn, args, storage).await,
        "LPUSH" => handle_lpush_command(conn, args, storage).await,
        "LLEN" => handle_llen_command(conn, args, storage).await,
        "LPOP" => handle_lpop_command(conn, args, storage).await,
        "BLPOP" => handle_blpop_command(conn, args, storage).await,
        "TYPE" => handle_type_command(conn, args, storage).await,
        "XADD" => handle_xadd_command(conn, args, storage).await,
        "XRANGE" => handle_xrange_command(conn, args, storage).await,
        "XREAD" => handle_xread_command(conn, args, storage).await,
        "INCR" => handle_incr_command(conn, args, storage).await,
        v => Err(ServerError::InvalidCommand(v.to_string())),
    }
}
