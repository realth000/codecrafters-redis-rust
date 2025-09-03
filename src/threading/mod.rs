use std::net::TcpStream;

use tokio::sync::mpsc;

mod recver;
mod sender;

type ConnId = u32;

/// Actions transferred from `Recver` to `Sender`, notifying
/// new events and let `Sender` sends message back to clients
/// on the other side of connection.
enum Action {
    /// Receive new connection.
    IncomingConn(ConnId, TcpStream),

    /// Get a ping message.
    GetPing(ConnId),
}

pub(crate) async fn setup_connection() {
    let (mut sd, mut rv) = mpsc::channel::<Action>(4);

    let recver = recver::Recver::new(sd);
    let mut sender = sender::Sender::new(rv);
    tokio::spawn(move || sender.start());

    unimplemented!()
}
