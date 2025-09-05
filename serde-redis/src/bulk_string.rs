use serde::{de::Visitor, Deserialize, Serialize};

use crate::utils::bytes_to_num;

pub(super) const KEY_BULK_STRING_NULL: &'static str = "serde_redis::BulkString::Null";

/// Bulk string in RESP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkString(Option<Vec<u8>>);

impl BulkString {
    pub fn new(v: impl Into<Vec<u8>>) -> Self {
        Self(Some(v.into()))
    }

    pub const fn null() -> Self {
        Self(None)
    }

    pub fn value(&self) -> Option<&Vec<u8>> {
        self.0.as_ref()
    }

    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }
}

pub(crate) struct BulkStringVisitor;

impl<'de> Visitor<'de> for BulkStringVisitor {
    type Value = BulkString;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("bulk string, aka bytes array")
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() < 4 {
            // Null
            Ok(BulkString::null())
        } else {
            let len = bytes_to_num(&v[..4]) as usize;
            if v.len() != len + 4 {
                Err(serde::de::Error::custom(format!(
                    "invalid bulk string length produced by deserializer: expected {}, got {}",
                    len,
                    v.len() - 4
                )))
            } else {
                Ok(BulkString::new(v.into_iter().skip(4).collect::<Vec<u8>>()))
            }
        }
    }
}

impl<'de> Deserialize<'de> for BulkString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(BulkStringVisitor)
    }
}

impl Serialize for BulkString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.value() {
            Some(v) => serializer.serialize_bytes(v),
            None => serializer.serialize_newtype_struct(KEY_BULK_STRING_NULL, &()),
        }
    }
}

impl From<String> for BulkString {
    fn from(value: String) -> Self {
        Self::new(value.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, to_vec};

    use super::*;

    #[test]
    fn test_decode_bulk_string() {
        let v1 = BulkString::new(b"I' am the bulk string");
        let v2: BulkString = from_bytes(b"$21\r\nI' am the bulk string\r\n").unwrap();
        assert_eq!(v1.value().unwrap(), v2.value().unwrap());

        let v3 = BulkString::new(b"");
        let v4: BulkString = from_bytes(b"$0\r\n\r\n").unwrap();
        assert_eq!(v3.value().unwrap(), v4.value().unwrap());

        let v6: BulkString = from_bytes(b"$-1\r\n").unwrap();
        assert!(v6.is_null());
    }

    #[test]
    fn test_encode_bulk_string() {
        let v1 = BulkString::new(b"I' am the bulk string");
        assert_eq!(to_vec(&v1).unwrap(), b"$21\r\nI' am the bulk string\r\n");
        let v2 = BulkString::new(b"");
        assert_eq!(to_vec(&v2).unwrap(), b"$0\r\n\r\n");
        let v3 = BulkString::null();
        assert_eq!(to_vec(&v3).unwrap(), b"$-1\r\n");
    }
}
