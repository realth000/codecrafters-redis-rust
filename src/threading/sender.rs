use std::{collections::HashMap, net::TcpStream};

use tokio::{net::tcp::WriteHalf, sync::mpsc::Receiver};

use crate::threading::{Action, ConnId};

/// Sender holds writer of connetion and reply message to client.
pub(crate) struct Sender {
    /// The receiver side of connection.
    rv: Receiver<Action>,

    /// All handles to send message back.
    handles: HashMap<ConnId, TcpStream>,
}

impl Sender {
    pub(super) fn new(rv: Receiver<Action>) -> Self {
        Self {
            rv,
            handles: HashMap::new(),
        }
    }

    pub(crate) async fn start(&mut self) {
        // TODO: Event loop
    }
}
