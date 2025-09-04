use std::io::{Cursor, Seek, SeekFrom};

use bytes::Buf;

use super::ErrorMessage;

use super::{
    error::{RdError, RdResult},
    RdValue,
};

trait Foresee: Seek + Buf {
    /// Get current position.
    fn pos(&self) -> u64;

    /// Check if the next 1 byte is `ch`.
    ///
    /// ## Returns
    ///
    /// * `true` if next byte is `ch`, advance 1 byte.
    /// * `false` if next byte is not `ch`, does not change position.
    fn foresee(&mut self, ch: u8) -> bool {
        if !self.has_remaining() {
            return false;
        }
        if self.get_u8() != ch {
            let _ = self
                .seek_relative(-1)
                .expect("failed to restore position -1");
            return false;
        }

        true
    }

    /// Peek the next character and return it.
    ///
    /// If there is no character ahead, return `None`.
    ///
    /// Never advance current position.
    fn foresee_any(&mut self) -> Option<u8> {
        if !self.has_remaining() {
            return None;
        }

        let ch = self.get_u8();
        let _ = self.seek_relative(-1);
        Some(ch)
    }

    /// Check if the next 1 byte is b' '.
    ///
    /// ## Returns
    ///
    /// * `true` if next byte is b' ', advance 1 byte.
    /// * `false` if next byte is not b' ', does not change position.
    fn foresee_space(&mut self) -> bool {
        self.foresee(b' ')
    }

    /// Check if the next 2 bytes are b'\r\n'.
    ///
    /// ## Returns
    ///
    /// * `true` if next 2 bytes are b'\r\n', advance 2 bytes.
    /// * `false` if next 2 bytes are not b'\r\n', does not change position.
    fn foresee_crlf(&mut self) -> bool {
        if !self.foresee(b'\r') {
            return false;
        }
        let pos = self.pos();
        if !self.foresee(b'\n') {
            self.seek(SeekFrom::Start(pos))
                .expect("faield to restore position before '\r'");
            return false;
        }

        true
    }
}

impl Foresee for Cursor<&'_ [u8]> {
    fn pos(&self) -> u64 {
        self.position()
    }
}

trait Collectable: Foresee {
    fn collect_over(&mut self, ch: u8) -> Vec<u8> {
        let mut b = vec![];
        while !self.foresee(ch) && self.has_remaining() {
            b.push(self.get_u8());
        }
        b
    }

    fn collect_over_space(&mut self) -> Vec<u8> {
        let mut b = vec![];
        while !self.foresee_space() && self.has_remaining() {
            b.push(self.get_u8());
        }
        b
    }

    fn collect_over_crlf(&mut self) -> Vec<u8> {
        let mut b = vec![];
        while !self.foresee_crlf() && self.has_remaining() {
            b.push(self.get_u8());
        }
        b
    }
}

impl<'de> Collectable for Cursor<&'de [u8]> {}

enum ParseResult {
    Value(RdValue),
    End,
}

#[derive(Debug)]
struct RdDeserializer<'de> {
    cursor: Cursor<&'de [u8]>,
    input: &'de [u8],
}

impl<'de> RdDeserializer<'de> {
    fn from_bytes(data: &'de [u8]) -> Self {
        Self {
            cursor: Cursor::new(data),
            input: data,
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.cursor.foresee_any()
    }

    fn parse_simple_string(&mut self) -> RdResult<String> {
        if !self.cursor.foresee(b'+') {
            return Err(RdError::InvalidPrefix {
                pos: self.cursor.position(),
                ty: "String",
                expected: "+",
            });
        }

        let data = String::from_utf8(self.cursor.collect_over_crlf())
            .map_err(RdError::InvalidUtf8String)?;

        Ok(data)
    }

    fn parse_error_message(&mut self) -> RdResult<ErrorMessage> {
        if !self.cursor.foresee(b'-') {
            return Err(RdError::InvalidPrefix {
                pos: self.cursor.position(),
                ty: "ErrorMessage",
                expected: "-",
            });
        }

        let data = self.cursor.collect_over_crlf();
        if data.is_empty() {
            return Ok(ErrorMessage {
                prefix: None,
                message: String::new(),
            });
        }
        let it = data.into_iter().map(|x| x as char);
        let prefix = it
            .clone()
            .take_while(|x| x >= &'A' && x <= &'Z')
            .collect::<String>();

        let message = it.skip(prefix.len()).collect::<String>();

        Ok(ErrorMessage {
            prefix: (!prefix.is_empty()).then(|| prefix),
            message,
        })
    }
}

impl<'de, 'a> serde::de::Deserializer<'de> for &'a mut RdDeserializer<'de> {
    type Error = RdError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let ch = match self.peek() {
            Some(v) => v,
            None => return Err(RdError::EOF),
        };

        match ch {
            b'+' => visitor.visit_string(self.parse_simple_string()?),
            b'-' => visitor.visit_string(self.parse_simple_string()?),
            v => Err(RdError::UnknownPrefix {
                pos: self.cursor.position(),
                prefix: v,
            }),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(RdError::UnsupportedPrimitiveType {
            curr: "str",
            replace: "SimpleString or BulkString",
        })
    }

    fn deserialize_string<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(RdError::UnsupportedPrimitiveType {
            curr: "String",
            replace: "SimpleString or BulkString",
        })
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        panic!("NAME: {name}")
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}

impl<'a, 'de: 'a> serde::de::IntoDeserializer<'de, RdError> for &'de mut RdDeserializer<'a> {
    type Deserializer = Self;
    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

pub fn from_bytes<'de, T>(s: &'de [u8]) -> Result<T, RdError>
where
    T: serde::de::Deserialize<'de>,
{
    serde::de::Deserialize::deserialize(&mut RdDeserializer::from_bytes(s))
}

#[cfg(test)]
mod test {
    use crate::protocol::SimpleString;

    use super::*;

    #[test]
    fn test_decode_string() {
        let s: SimpleString = from_bytes(b"+OK\r\n").unwrap();
        assert_eq!(s.0.as_str(), "OK");
    }
}
