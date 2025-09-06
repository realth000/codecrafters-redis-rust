use serde::Serialize;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::error::{ServerError, ServerResult};

/// A connection between redis client instance.
#[derive(Debug)]
pub(crate) struct Conn<'a> {
    pub id: usize,
    pub stream: &'a mut TcpStream,
}

impl<'a> Conn<'a> {
    pub(crate) fn new(id: usize, stream: &'a mut TcpStream) -> Self {
        Self { id, stream }
    }

    pub(crate) fn log(&self, data: impl AsRef<str>) {
        println!("[{}] {}", self.id, data.as_ref())
    }

    pub(crate) async fn write_value<T: Serialize>(&mut self, value: &T) -> ServerResult<()> {
        let content = serde_redis::to_vec(value).map_err(ServerError::SerdeError)?;
        self.stream
            .write(&content)
            .await
            .map_err(ServerError::IoError)?;
        Ok(())
    }
}
