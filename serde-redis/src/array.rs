use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Deserializer, Serialize};

use crate::Value;

/// Array in RESP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Array(Option<Vec<Value>>);

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
        self.value().is_none()
    }

    pub fn is_empty(&self) -> bool {
        self.value().is_some_and(|x| x.is_empty())
    }

    pub fn is_null_or_empty(&self) -> bool {
        self.is_null() || self.is_empty()
    }

    pub fn value(&self) -> Option<&Vec<Value>> {
        self.0.as_ref()
    }

    pub fn take(&mut self) -> Option<Vec<Value>> {
        self.0.take()
    }

    /// Pop the last element in Array.
    pub fn pop(&mut self) -> Option<Value> {
        self.0.as_mut().and_then(|x| x.pop())
    }

    /// Pop the first element in array.
    pub fn pop_front(&mut self) -> Option<Value> {
        if self.is_null_or_empty() {
            return None;
        }
        self.0.as_mut().map(|x| x.remove(0))
    }

    /// Try get the first element if it is BulkString, returns
    /// the bytes in it.
    pub fn pop_front_bulk_string_bytes(&mut self) -> Option<Vec<u8>> {
        if let Some(Value::BulkString(mut s)) = self.pop_front() {
            s.take()
        } else {
            None
        }
    }

    /// Try get the first element if it is BulkString, returns
    /// the UTF-8 String representation of bytes in it.
    pub fn pop_front_bulk_string(&mut self) -> Option<String> {
        self.pop_front_bulk_string_bytes()
            .and_then(|x| String::from_utf8(x).ok())
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

impl Serialize for Array {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.value() {
            Some(v) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for ele in v.iter() {
                    seq.serialize_element(ele)?;
                }
                seq.end()
            }
            None => {
                let seq = serializer.serialize_seq(Some(1))?;
                seq.end()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, to_vec, BulkString, Integer, SimpleError, SimpleString};

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

    #[test]
    fn test_encode_array() {
        let v1 = Array::with_values(vec![
            Value::SimpleString(SimpleString::new("ele1")),
            Value::SimpleString(SimpleString::new("ele2")),
        ]);
        assert_eq!(to_vec(&v1).unwrap(), b"*2\r\n+ele1\r\n+ele2\r\n");
        let v1 = Array::with_values(vec![
            Value::SimpleError(SimpleError::with_prefix("ERR", "err message")),
            Value::Array(Array::with_values(vec![
                Value::Integer(Integer::new(12321)),
                Value::BulkString(BulkString::new(b"I'm the Bulk String")),
            ])),
        ]);
        let s0 = "*2\r\n:+12321\r\n$19\r\nI'm the Bulk String\r\n";
        let s1 = format!("*2\r\n-ERR err message\r\n{s0}");
        assert_eq!(to_vec(&v1).unwrap(), s1.as_bytes());
    }
}
