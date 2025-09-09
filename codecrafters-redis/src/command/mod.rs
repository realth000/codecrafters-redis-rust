use serde_redis::{Array, Value};

use crate::{
    command::{
        blpop::handle_blpop_command, echo::handle_echo_command, get::handle_get_command,
        incr::handle_incr_command, llen::handle_llen_command, lpop::handle_lpop_command,
        lpush::handle_lpush_command, lrange::handle_lrange_command, multi::handle_multi_command,
        ping::handle_ping_command, rpush::handle_rpush_command, set::handle_set_command,
        tipe::handle_type_command, xadd::handle_xadd_command, xrange::handle_xrange_command,
        xread::handle_xread_command,
    },
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

mod blpop;
mod echo;
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
    storage: &mut Storage,
    mut args: Array,
    conn: &mut Conn<'_>,
) -> ServerResult<()> {
    if args.is_null_or_empty() {
        return Err(ServerError::InvalidMessage("args is null or empty".into()));
    }

    let ele = args.pop_front();
    match ele {
        Some(Value::BulkString(mut cmd)) => match cmd.take() {
            Some(cmd) => {
                match String::from_utf8(cmd)
                    .map_err(|e| ServerError::InvalidCommand(format!("{e:?}")))?
                    .to_uppercase()
                    .as_str()
                {
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
                    "MULTI" => handle_multi_command(conn, storage).await,
                    v => Err(ServerError::InvalidCommand(v.to_string())),
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
