use std::{
    collections::HashMap,
    net::{TcpListener, TcpStream},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::ReadHalf,
    sync::mpsc::Sender,
};

use crate::threading::{Action, ConnId};

/// Recver holds reader of connetion and receive messages from client.
pub(crate) struct Recver {
    /// The sender side of connection.
    sd: Sender<Action>,

    /// All handles to send message back.
    handles: HashMap<ConnId, TcpStream>,

    /// Current id.
    curr_id: ConnId,
}

impl Recver {
    pub(super) fn new(sd: Sender<Action>) -> Self {
        Self {
            sd,
            handles: HashMap::new(),
            curr_id: 0,
        }
    }

    pub(crate) async fn start(&mut self) {
        let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

        loop {
            let (stream, _) = listener.accept().unwrap();
            self.sd
                .send(Action::IncomingConn(
                    self.curr_id,
                    stream.try_clone().unwrap(),
                ))
                .await
                .unwrap();
        }
    }
}
