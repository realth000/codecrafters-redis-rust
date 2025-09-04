mod bulk_string;
mod decode;
mod encode;
mod error;
mod error_message;
mod integer;
mod simple_string;
mod utils;

pub use bulk_string::BulkString;
pub use decode::from_bytes;
pub use encode::to_vec;
pub use error_message::ErrorMessage;
pub use simple_string::SimpleString;

/// All supported data types used in redis protocol.
///
/// These values are used to transfer data between server and client.
#[derive(Debug, Clone)]
pub enum RdValue {
    String(SimpleString),
    Error(ErrorMessage),
}
