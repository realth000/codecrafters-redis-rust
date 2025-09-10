mod array;
mod bulk_string;
mod decode;
mod encode;
mod error;
mod integer;
mod null;
mod simple_error;
mod simple_string;
mod utils;

const KEY_VALUE_ENUM: &'static str = "serde_redis::Value";

use serde::{de::Visitor, Deserialize, Serialize};

pub use array::Array;
pub use bulk_string::BulkString;
pub use decode::from_bytes;
pub use encode::to_vec;
pub use error::RdError;
pub use integer::Integer;
pub use null::Null;
pub use simple_error::SimpleError;
pub use simple_string::SimpleString;
pub use utils::num_to_bytes;

use crate::{
    array::ArrayVisitor, bulk_string::BulkStringVisitor, integer::IntegerVisitor,
    null::NullVisitor, simple_error::SimpleErrorVisitor, simple_string::SimpleStringVisitor,
};

/// All supported data types used in redis protocol.
///
/// These values are used to transfer data between server and client.
///
/// * [RESP protocol description](https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(Integer),
    BulkString(BulkString),
    Array(Array),
    Null(Null),
}

impl Value {
    pub fn simple_name(&self) -> &'static str {
        match self {
            Value::SimpleString(..) => "string",
            Value::SimpleError(..) => "error",
            Value::Integer(..) => "integer",
            Value::BulkString(..) => "string",
            Value::Array(..) => "list",
            Value::Null(..) => "null",
        }
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn visit_string<E>(self, mut v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // SimpleString or SimpleError

        // FIXME: Remove the string hack for Value.
        // We prepend a char to indicate the content type.
        if v.is_empty() {
            return Err(serde::de::Error::custom(
                "expected string type flag in string content",
            ));
        }
        match v.remove(0) {
            '+' => {
                // Simple string
                let v = SimpleStringVisitor {}.visit_string(v)?;
                Ok(Value::SimpleString(v))
            }
            '-' => {
                // Simple error
                let v = SimpleErrorVisitor {}.visit_string(v)?;
                Ok(Value::SimpleError(v))
            }
            v => Err(serde::de::Error::custom(format!(
                "unknown string type when parsing Value: {v}"
            ))),
        }
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // Integer

        let v = IntegerVisitor {}.visit_i64(v)?;
        Ok(Value::Integer(v))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // BulkString

        let v = BulkStringVisitor {}.visit_byte_buf(v)?;
        Ok(Value::BulkString(v))
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        // Array

        let v = ArrayVisitor {}.visit_seq(seq)?;
        Ok(Value::Array(v))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // Null

        let v = NullVisitor {}.visit_unit()?;
        Ok(Value::Null(v))
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("any supported RESP type")
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_enum(KEY_VALUE_ENUM, &[], ValueVisitor)
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::SimpleString(v) => v.serialize(serializer),
            Value::SimpleError(v) => v.serialize(serializer),
            Value::Integer(v) => v.serialize(serializer),
            Value::BulkString(v) => v.serialize(serializer),
            Value::Array(v) => v.serialize(serializer),
            Value::Null(v) => v.serialize(serializer),
        }
    }
}
