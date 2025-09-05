use serde::{de::Visitor, Deserialize, Deserializer};

use crate::Value;

/// Array in RESP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Array(pub Option<Vec<Value>>);

impl Array {
    pub fn new(v: Option<Vec<Value>>) -> Self {
        Self(v)
    }

    pub fn null() -> Self {
        Self(None)
    }

    pub fn with_values(values: impl Into<Vec<Value>>) -> Self {
        Self(Some(values.into()))
    }

    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }

    pub fn value(&self) -> Option<&Vec<Value>> {
        self.0.as_ref()
    }

    pub fn take(&mut self) -> Option<Vec<Value>> {
        self.0.take()
    }
}

pub(crate) struct ArrayVisitor;

impl<'de> Visitor<'de> for ArrayVisitor {
    type Value = Array;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("redis array (a list of values)")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut v = vec![];

        // FIXME: Remove the array hack.
        // First element string indicates is null array or not: null array is with empty string.
        if let Some(Value::SimpleString(flag)) = seq.next_element()? {
            if flag.value().is_empty() {
                return Ok(Array(None));
            }
        } else {
            // Shall not happen if do not forget it in the deserializer.
            unreachable!("expected flag before array content")
        }

        while let Some(ele) = seq.next_element()? {
            v.push(ele);
        }
        Ok(Array(Some(v)))
    }
}

impl<'de> Deserialize<'de> for Array {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ArrayVisitor)
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, BulkString, Integer, SimpleError, SimpleString};

    use super::*;

    #[test]
    fn test_decode_array() {
        {
            // Single element.
            let v1 = b"*1\r\n+Ok\r\n";
            let mut v2: Array = from_bytes(v1).unwrap();
            assert!(!v2.is_null());
            let mut v2v = v2.take().unwrap();
            assert_eq!(v2v.len(), 1);
            assert_eq!(
                v2v.pop().unwrap(),
                Value::SimpleString(SimpleString::new("Ok"))
            );
        }

        {
            // Multiple element.
            let v1 = b"*2\r\n+Ok\r\n-E custom error\r\n";
            let mut v2: Array = from_bytes(v1).unwrap();
            assert!(!v2.is_null());
            let mut v2v = v2.take().unwrap();
            assert_eq!(v2v.len(), 2);
            assert_eq!(
                v2v.pop().unwrap(),
                Value::SimpleError(SimpleError::with_prefix("E", "custom error"))
            );
            assert_eq!(
                v2v.pop().unwrap(),
                Value::SimpleString(SimpleString::new("Ok"))
            );
        }

        {
            // Nested 2 array.
            let v00 = "*2\r\n:+123\r\n$2\r\n12\r\n";
            let v01 = "*2\r\n+Ok\r\n-E custom error\r\n";
            let v1 = format!("*2\r\n{v00}{v01}");
            let mut v2: Array = from_bytes(v1.as_bytes()).unwrap();
            assert!(!v2.is_null());
            let mut v2v = v2.take().unwrap();
            assert_eq!(v2v.len(), 2);

            assert_eq!(
                v2v.pop().unwrap(),
                Value::Array(Array::with_values(vec![
                    Value::SimpleString(SimpleString::new("Ok")),
                    Value::SimpleError(SimpleError::with_prefix("E", "custom error")),
                ]))
            );
            assert_eq!(
                v2v.pop().unwrap(),
                Value::Array(Array::with_values(vec![
                    Value::Integer(Integer::new(123)),
                    Value::BulkString(BulkString::new(b"12")),
                ]))
            );
        }

        {
            // Empty array.
            let v1 = b"*0\r\n";
            let mut v2: Array = from_bytes(v1).unwrap();
            assert!(!v2.is_null());
            let v2v = v2.take().unwrap();
            assert_eq!(v2v.len(), 0);
        }

        {
            // Null array.
            let v1 = b"*-1\r\n";
            let v2: Array = from_bytes(v1).unwrap();
            assert!(v2.is_null());
        }
    }
}
