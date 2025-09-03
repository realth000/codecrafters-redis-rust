use serde::{Serialize, Serializer};

/// String type in redis protocol.
///
/// Simple strings are encoded as a plus (+) character, followed by a string.
///
/// The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
///
/// Simple strings transmit short, non-binary strings with minimal overhead.
#[derive(Debug, Clone)]
pub struct RdString(String);

impl Serialize for RdString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.sest
    }
}
