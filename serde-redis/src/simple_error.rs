use serde::{de::Visitor, ser::SerializeStruct, Deserialize, Serialize};

pub(crate) const KEY_SIMPLE_ERROR: &'static str = "serde_redis::SimpleError";

/// Error message in redis protocol.
///
/// May have prefix - word in uppercase.
///
/// ## Format
///
/// * With prefix: `-PREFIX message\r\n`.
/// * Without prefix: `-Some message\r\n`
///
/// ## Example
///
/// ```rust
/// ```
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleError {
    /// Optional prefix of the error message.
    ///
    /// Prefix is the first part after '-' prefix and all letters in prefix are in UPPERCASE.
    ///
    /// Prefix is a convention used by redis rather, not part of protocol, so it's optional.
    ///
    /// When encoding and decoding prefix, it is guaranteed to be UPPERCASE, no matter the
    /// content is already UPPERCASE or not.
    prefix: Option<String>,

    /// The message in error.
    ///
    /// Note that this field works like simple string, can not hold CRLF as well.
    message: String,
}

impl SimpleError {
    pub fn new(prefix: Option<impl Into<String> + Sized>, message: impl Into<String>) -> Self {
        Self {
            prefix: prefix.map(|x| Into::<String>::into(x).to_uppercase()),
            message: message.into(),
        }
    }

    pub fn with_prefix(prefix: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
            message: message.into(),
        }
    }

    pub fn without_prefix(message: impl Into<String>) -> Self {
        Self {
            prefix: None,
            message: message.into(),
        }
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl<'de> Deserialize<'de> for SimpleError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(SimpleErrorVisitor)
    }
}

pub(crate) struct SimpleErrorVisitor;

impl<'de> Visitor<'de> for SimpleErrorVisitor {
    type Value = SimpleError;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("redis error message")
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.is_empty() {
            return Ok(SimpleError::without_prefix(""));
        }

        let space_pos = match v.find(|x| x == ' ') {
            Some(v) => v,
            None => return Ok(SimpleError::without_prefix(v)),
        };

        let has_prefix = if space_pos == 0 {
            false
        } else {
            if v.chars().take(space_pos).all(|x| 'A' <= x && x <= 'Z') {
                true
            } else {
                false
            }
        };

        if has_prefix {
            let (prefix, message) = v.split_at(space_pos);
            Ok(SimpleError::with_prefix(prefix, message.trim_start()))
        } else {
            Ok(SimpleError::without_prefix(v))
        }
    }
}

impl Serialize for SimpleError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct(KEY_SIMPLE_ERROR, 0 /* Length not matter*/)?;
        match &self.prefix {
            Some(v) => {
                s.serialize_field(KEY_SIMPLE_ERROR, format!("{v} ").as_str())?;
            }
            None => { /* Do nothing. */ }
        }
        s.serialize_field(KEY_SIMPLE_ERROR, &self.message)?;
        s.serialize_field(KEY_SIMPLE_ERROR, "\r\n")?;
        s.end()
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, to_vec};

    use super::*;

    #[test]
    fn test_decode_error_message() {
        let v0 = from_bytes::<SimpleError>(b"-error message\r\n").unwrap();
        assert!(v0.prefix.is_none());
        assert_eq!(v0.message.as_str(), "error message");
        let v1 = from_bytes::<SimpleError>(b"-Hello From Error Message\r\n").unwrap();
        assert!(v1.prefix.is_none());
        assert_eq!(v1.message.as_str(), "Hello From Error Message");
        let v1 = from_bytes::<SimpleError>(b"-ERR Msg\r\n").unwrap();
        assert_eq!(v1.prefix.unwrap().as_str(), "ERR");
        assert_eq!(v1.message.as_str(), "Msg");
    }

    #[test]
    fn test_encode_error_message() {
        let v0 = SimpleError::with_prefix("ERRKIND", "err message");
        assert_eq!(to_vec(&v0).unwrap(), b"-ERRKIND err message\r\n");
        let v1 = SimpleError::without_prefix("err message");
        assert_eq!(to_vec(&v1).unwrap(), b"-err message\r\n");
    }
}
