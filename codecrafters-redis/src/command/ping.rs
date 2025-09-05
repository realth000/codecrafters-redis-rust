use serde_redis::SimpleString;
use tokio::io::AsyncWriteExt;

use crate::{
    conn::Conn,
    error::{ServerError, ServerResult},
};

pub(super) async fn handle_ping_command(conn: &mut Conn<'_>) -> ServerResult<()> {
    conn.log("run command PONG");
    let msg = serde_redis::to_vec(&SimpleString::new("PONG")).map_err(ServerError::SerdeError)?;
    conn.stream
        .write(&msg)
        .await
        .map_err(ServerError::IoError)?;
    Ok(())
}
