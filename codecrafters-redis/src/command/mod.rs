use serde_redis::{Array, Value};

use crate::{
    command::{
        echo::handle_echo_command, get::handle_get_command, ping::handle_ping_command,
        set::handle_set_command,
    },
    conn::Conn,
    error::{ServerError, ServerResult},
    storage::Storage,
};

mod echo;
mod get;
mod ping;
mod set;

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
