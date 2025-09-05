use serde::{de::Visitor, Deserialize, Serialize};

/// Null type in RESP.
///
/// ## Format
///
/// `_\r\n`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Null;

pub(crate) struct NullVisitor;

impl<'de> Visitor<'de> for NullVisitor {
    type Value = Null;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("NULL value in redis")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Null)
    }
}

impl<'de> Deserialize<'de> for Null {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_unit(NullVisitor)
    }
}

impl Serialize for Null {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, to_vec};

    use super::*;

    #[test]
    fn decode_null() {
        let v1 = b"_\r\n";
        let _: Null = from_bytes(v1).unwrap();
    }

    #[test]
    fn encode_null() {
        let v1 = Null;
        assert_eq!(to_vec(&v1).unwrap(), b"_\r\n");
    }
}
