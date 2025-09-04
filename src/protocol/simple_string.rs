use serde::{de::Visitor, Deserialize, Serialize};

pub(super) const SIMPLE_STRING_TAG: &'static str = "::serde_redis::SimpleString";

/// String type in redis protocol.
///
/// Simple strings are encoded as a plus (+) character, followed by a string.
///
/// The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
///
/// Simple strings transmit short, non-binary strings with minimal overhead.
///
/// ```rust
/// use decode::from_bytes;
/// use encode::to_bytes;
///
/// assert_eq!(to_bytes::<String>(b"OK"), b"+OK\r\n");
/// assert_eq!(from_bytes::<String>(b"+OK\r\n"), "OK");
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct SimpleString(pub String);

struct SimpleStringVisitor;

impl<'de> Visitor<'de> for SimpleStringVisitor {
    type Value = SimpleString;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("redis simple string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        unimplemented!()
    }
}

impl<'de> Deserialize<'de> for SimpleString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_newtype_struct(SIMPLE_STRING_TAG, SimpleStringVisitor)
    }
}
