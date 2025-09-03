use self::string::RdString;

mod decode;
mod encode;
mod error;
mod string;

/// All supported data types used in redis protocol.
///
/// These values are used to transfer data between server and client.
#[derive(Debug, Clone)]
pub enum RdValue {
    String(RdString),
}
