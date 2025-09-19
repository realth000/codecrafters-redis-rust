use crate::{conn::Conn, error::ServerResult, replication::ReplicationState};

pub(super) async fn handle_info_command(
    conn: &mut Conn<'_>,
    rep: ReplicationState,
) -> ServerResult<()> {
    conn.log("run command INFO");
    let value = rep.info();
    conn.write_value(value).await
}
