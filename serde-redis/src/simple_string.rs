use serde::{de::Visitor, Deserialize, Serialize};

/// String type in RESP.
///
/// Simple string must NOT contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
///
/// # Format
///
/// `+CONTENT\r\n`
///
/// # Example
///
/// ```rust
/// use serde_redis::{from_bytes, to_vec};
///
/// assert_eq!(to_vec("OK").unwrap(), b"+OK\r\n");
/// assert_eq!(from_bytes::<String>(b"+OK\r\n").unwrap(), "OK".to_string());
/// ```
#[derive(Debug, Clone)]
pub struct SimpleString(pub String);

impl SimpleString {
    pub fn new(v: impl Into<String>) -> Self {
        Self(v.into())
    }

    pub fn value(&self) -> &str {
        &self.0
    }
}

struct SimpleStringVisitor;

impl<'de> Visitor<'de> for SimpleStringVisitor {
    type Value = SimpleString;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("redis simple string")
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SimpleString(v))
    }
}

impl<'de> Deserialize<'de> for SimpleString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(SimpleStringVisitor)
    }
}

impl Serialize for SimpleString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.value())
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, to_vec};

    use super::*;

    #[test]
    fn test_decode_simple_string() {
        let s1 = SimpleString("I' am a simple string".into());
        let s2: SimpleString = from_bytes(b"+I' am a simple string\r\n").unwrap();
        assert_eq!(s1.value(), s2.value());
        let s3 = SimpleString("".into());
        let s4: SimpleString = from_bytes(b"+\r\n").unwrap();
        assert_eq!(s3.value(), s4.value());
    }

    #[test]
    fn test_encode_simple_string() {
        let s1 = SimpleString::new("I'm a simple string");
        let s2 = b"+I'm a simple string\r\n";
        assert_eq!(to_vec(&s1).unwrap().as_slice(), s2);

        let s3 = SimpleString::new("");
        let s4 = b"+\r\n";
        assert_eq!(to_vec(&s3).unwrap().as_slice(), s4);
    }
}
