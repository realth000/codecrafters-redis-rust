mod decode;
mod encode;
mod error;
mod error_message;
mod simple_string;

pub use error_message::ErrorMessage;
pub use simple_string::SimpleString;

/// All supported data types used in redis protocol.
///
/// These values are used to transfer data between server and client.
#[derive(Debug, Clone)]
pub enum RdValue {
    String(SimpleString),
}
