use std::{error::Error, fmt::Display};

use serde_redis::RdError;

pub type ServerResult<T> = Result<T, ServerError>;

/// All errors as a redis server may respond.
#[derive(Debug)]
pub enum ServerError {
    /// Forwarding `std::io::Error`
    IoError(std::io::Error),

    /// The message is invalid, not following the correct structure.
    ///
    /// That is, the message should be an array with command as the
    /// first element.
    InvalidMessage(String),

    /// Command is invalid, the very first element in message array.
    InvalidCommand(String),

    /// Error when serializing or deserializing.
    SerdeError(RdError),
}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::IoError(e) => f.write_fmt(format_args!("io error: {e}")),
            ServerError::InvalidMessage(msg) => f.write_fmt(format_args!("invalid message: {msg}")),
            ServerError::InvalidCommand(cmd) => {
                f.write_fmt(format_args!("invalid command \"{cmd}\""))
            }
            ServerError::SerdeError(e) => f.write_fmt(format_args!(
                "error in serialization or deserialization: {e}"
            )),
        }
    }
}

impl Error for ServerError {}
