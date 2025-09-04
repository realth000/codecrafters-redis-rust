use serde::{de::Visitor, Deserialize};

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
#[derive(Debug, Clone)]
pub struct ErrorMessage {
    /// Optional prefix of the error message.
    ///
    /// Prefix is the first part after '-' prefix and all letters in prefix are in UPPERCASE.
    ///
    /// Prefix is a convention used by redis rather, not part of protocol, so it's optional.
    ///
    /// When encoding and decoding prefix, it is guaranteed to be UPPERCASE, no matter the
    /// content is already UPPERCASE or not.
    pub prefix: Option<String>,

    /// The message in error.
    ///
    /// Note that this field works like simple string, can not hold CRLF as well.
    pub message: String,
}

impl ErrorMessage {
    pub fn new(prefix: Option<impl Into<String> + Sized>, message: impl Into<String>) -> Self {
        Self {
            prefix: match prefix {
                Some(v) => Some(v.into()),
                None => None,
            },
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
}

impl<'de> Deserialize<'de> for ErrorMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(ErrorMessageVisitor)
    }
}

struct ErrorMessageVisitor;

impl<'de> Visitor<'de> for ErrorMessageVisitor {
    type Value = ErrorMessage;

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
            return Ok(ErrorMessage::without_prefix(""));
        }

        let space_pos = match v.find(|x| x == ' ') {
            Some(v) => v,
            None => return Ok(ErrorMessage::without_prefix(v)),
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
            Ok(ErrorMessage::with_prefix(prefix, message.trim_start()))
        } else {
            Ok(ErrorMessage::without_prefix(v))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::from_bytes;

    use super::*;

    #[test]
    fn test_decode_error_message() {
        let v0 = from_bytes::<ErrorMessage>(b"-error message\r\n").unwrap();
        assert!(v0.prefix.is_none());
        assert_eq!(v0.message.as_str(), "error message");
        let v1 = from_bytes::<ErrorMessage>(b"-Hello From Error Message\r\n").unwrap();
        assert!(v1.prefix.is_none());
        assert_eq!(v1.message.as_str(), "Hello From Error Message");
        let v1 = from_bytes::<ErrorMessage>(b"-ERR Msg\r\n").unwrap();
        assert_eq!(v1.prefix.unwrap().as_str(), "ERR");
        assert_eq!(v1.message.as_str(), "Msg");
    }
}
