use crate::{conn::Conn, error::ServerResult, storage::Storage};

pub(super) async fn handle_info_command(
    conn: &mut Conn<'_>,
    storage: &mut Storage,
) -> ServerResult<()> {
    conn.log("run command INFO");
    let value = storage.info();
    conn.write_value(value).await
}
