use serde::{de::Visitor, Deserialize, Serialize};

/// Integer type in RESP, base-10, 64-bit number.
///
/// ## Format
///
/// `:[<+|->]<value>\r\n`
#[derive(Debug, Clone)]
pub struct Integer(pub i64);

impl Integer {
    pub fn new(v: i64) -> Self {
        Self(v)
    }

    pub fn value(&self) -> i64 {
        self.0
    }
}

struct IntegerVisitor;

impl<'de> Visitor<'de> for IntegerVisitor {
    type Value = Integer;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("64-bit signed ineger with radix 10")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Integer(v))
    }
}

impl<'de> Deserialize<'de> for Integer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(IntegerVisitor)
    }
}

impl Serialize for Integer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i64(self.value())
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, to_vec};

    use super::*;

    #[test]
    fn test_decode_integer() {
        let v1: Integer = from_bytes(b":+1\r\n").unwrap();
        assert_eq!(v1.value(), 1);
        let v2: Integer = from_bytes(b":+987654321\r\n").unwrap();
        assert_eq!(v2.value(), 987654321);
        let v3: Integer = from_bytes(b":-1\r\n").unwrap();
        assert_eq!(v3.value(), -1);
        let v4: Integer = from_bytes(b":-987654321\r\n").unwrap();
        assert_eq!(v4.value(), -987654321);
        let v5: Integer = from_bytes(b":-0\r\n").unwrap();
        assert_eq!(v5.value(), 0);
        let v6: Integer = from_bytes(b":+0\r\n").unwrap();
        assert_eq!(v6.value(), 0);
    }

    #[test]
    fn test_encode_integer() {
        let v1 = Integer::new(1);
        assert_eq!(to_vec(&v1).unwrap().as_slice(), b":+1\r\n");
        let v2 = Integer::new(987654321);
        assert_eq!(to_vec(&v2).unwrap().as_slice(), b":+987654321\r\n");
        let v1 = Integer::new(-1);
        assert_eq!(to_vec(&v1).unwrap().as_slice(), b":-1\r\n");
        let v2 = Integer::new(-987654321);
        assert_eq!(to_vec(&v2).unwrap().as_slice(), b":-987654321\r\n");
        let v5 = Integer::new(0);
        assert_eq!(to_vec(&v5).unwrap().as_slice(), b":+0\r\n");
    }
}
