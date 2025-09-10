use std::io::{stdout, Write};

use serde_redis::{Array, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    command::dispatch_normal_command,
    error::{ServerError, ServerResult},
    storage::Storage,
    transaction::{Transaction, TransactionEvent},
};

/// A connection between redis client instance.
#[derive(Debug)]
pub(crate) struct Conn<'a> {
    pub id: usize,
    stream: &'a mut TcpStream,
    transaction: Transaction,
}

impl<'a> Conn<'a> {
    pub(crate) fn new(id: usize, stream: &'a mut TcpStream) -> Self {
        Self {
            id,
            stream,
            transaction: Transaction::new(),
        }
    }

    pub(crate) fn log(&self, data: impl AsRef<str>) {
        println!("[{}] {}", self.id, data.as_ref());
        stdout().flush().unwrap();
    }

    pub(crate) async fn read(&mut self, buf: &'_ mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }

    pub(crate) async fn write_bytes(&mut self, buf: &[u8]) -> ServerResult<()> {
        self.stream.write(buf).await.map_err(ServerError::IoError)?;
        Ok(())
    }

    pub(crate) async fn write_value(&mut self, value: Value) -> ServerResult<()> {
        if self.is_executing_transaction() {
            self.transaction.record_result(value);
            Ok(())
        } else {
            let content = serde_redis::to_vec(&value).map_err(ServerError::SerdeError)?;
            self.stream
                .write(&content)
                .await
                .map_err(ServerError::IoError)?;
            Ok(())
        }
    }

    /// Record command in transaction.
    ///
    /// ## Returns
    ///
    /// * If in a transaction, return true.
    /// * If outside a transaction, return false.
    pub(crate) fn add_to_transaction(&mut self, cmd: String, args: Array) -> bool {
        match &mut self.transaction {
            Transaction::None => false,
            Transaction::Pending(events) => {
                events.push(TransactionEvent::new(cmd, args));
                true
            }
            Transaction::Executing(..) => false,
        }
    }

    pub(crate) fn in_transaction(&self) -> bool {
        self.transaction.is_pending() || self.transaction.is_executing()
    }

    fn is_executing_transaction(&self) -> bool {
        self.transaction.is_executing()
    }

    pub(crate) fn enter_transaction(&mut self) -> bool {
        if self.transaction.is_pending() {
            return false;
        } else {
            self.transaction.start();
            true
        }
    }

    /// Get the results of transaction.
    pub(crate) async fn commit_transaction(
        &mut self,
        storage: &mut Storage,
    ) -> ServerResult<Vec<Value>> {
        let events = self.transaction.commit();
        // Transaction convert into executing state.

        for event in events {
            dispatch_normal_command(self, &event.cmd, event.args, storage).await?;
        }
        Ok(self.transaction.finish())
    }

    /// Abort a transaction, drop all recorded values.
    pub(crate) fn abort_transaction(&mut self) {
        self.transaction.abort();
    }
}
