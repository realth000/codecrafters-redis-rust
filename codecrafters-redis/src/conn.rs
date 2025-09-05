use tokio::net::TcpStream;

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
}
