use std::io::{Cursor, Read, Seek, SeekFrom};

use bytes::Buf;
use serde::de::SeqAccess;

use crate::{
    error::{RdError, RdResult},
    utils::bytes_to_num,
    KEY_VALUE_ENUM,
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

    /// Peek the next character with targets constrained.
    ///
    /// ## Returns
    ///
    /// * `Some(_)` if next byte is one of `vs`, advance 1 byte.
    /// * `None` if next byte is not in `vs`, does not change position.
    fn foresee_one_of(&mut self, vs: &[u8]) -> Option<u8> {
        if !self.has_remaining() {
            return None;
        }

        let ch = self.get_u8();
        if vs.contains(&ch) {
            Some(ch)
        } else {
            let _ = self.seek_relative(-1);
            None
        }
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

pub(super) enum ParseResult {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(/* Element count: */ i64),
}

#[derive(Debug)]
struct Decoder<'de> {
    cursor: Cursor<&'de [u8]>,
    input: &'de [u8],
}

impl<'de> Decoder<'de> {
    fn from_bytes(data: &'de [u8]) -> Self {
        Self {
            cursor: Cursor::new(data),
            input: data,
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.cursor.foresee_any()
    }

    fn parse_any(&mut self) -> RdResult<ParseResult> {
        let ch = match self.peek() {
            Some(v) => v,
            None => return Err(RdError::EOF),
        };

        match ch {
            b'+' => Ok(ParseResult::SimpleString(self.parse_simple_string()?)),
            b'-' => Ok(ParseResult::SimpleError(self.parse_simple_error()?)),
            b':' => {
                let _ = self.cursor.get_u8();

                Ok(ParseResult::Integer(self.parse_integer()?))
            }
            b'$' => Ok(ParseResult::BulkString(self.parse_bulk_string()?)),
            b'*' => {
                // TODO: Check invalid length.
                // Array.
                // Elements count.
                let pos = self.cursor.position();
                if self.cursor.foresee(b'-')
                    && self.cursor.foresee(b'1')
                    && self.cursor.foresee(b'\r')
                    && self.cursor.foresee(b'\n')
                {
                    Ok(ParseResult::Array(-1))
                } else {
                    self.cursor.set_position(pos);
                    let count = bytes_to_num(self.cursor.collect_over_crlf().as_slice());
                    // Have zero or more elements.
                    Ok(ParseResult::Array(count))
                }
            }
            v => Err(RdError::UnknownPrefix {
                pos: self.cursor.position(),
                prefix: v,
            }),
        }
    }

    fn parse_integer(&mut self) -> RdResult<i64> {
        let sign = match self.cursor.foresee_one_of(&[b'-', b'+']) {
            Some(v) => v,
            None => {
                return Err(RdError::InvalidPrefix {
                    pos: self.cursor.position(),
                    ty: "Integer",
                    expected: "+ or -",
                })
            }
        };
        let value = bytes_to_num(self.cursor.collect_over_crlf());
        match sign {
            b'-' => Ok(-1 * value),
            b'+' => Ok(value),
            _ => unreachable!("sign must be - or +"),
        }
    }

    fn parse_str(&mut self) -> RdResult<&'de str> {
        if !self.cursor.foresee(b'+') {
            return Err(RdError::InvalidPrefix {
                pos: self.cursor.position(),
                ty: "String",
                expected: "+",
            });
        }

        let start = self.cursor.position() as usize;
        while !self.cursor.foresee_crlf() {
            let _ = self.cursor.seek_relative(1);
        }
        let end = self.cursor.position() as usize - 2;

        let data = str::from_utf8(&self.input[start..end]).map_err(RdError::InvalidUtf8Str)?;

        Ok(data)
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

    fn parse_simple_error(&mut self) -> RdResult<String> {
        if !self.cursor.foresee(b'-') {
            return Err(RdError::InvalidPrefix {
                pos: self.cursor.position(),
                ty: "ErrorMessage",
                expected: "-",
            });
        }

        let data = String::from_utf8(self.cursor.collect_over_crlf())
            .map_err(RdError::InvalidUtf8String)?;
        Ok(data)
    }

    fn parse_bulk_string(&mut self) -> RdResult<Vec<u8>> {
        if !self.cursor.foresee(b'$') {
            return Err(RdError::InvalidPrefix {
                pos: self.cursor.position(),
                ty: "BulkString",
                expected: "$",
            });
        }

        let mut length = self.cursor.collect_over_crlf();

        // Null
        if length.len() == 2 && length[0] == b'-' && length[1] == b'1' {
            return Ok(vec![]);
        }

        // Empty
        if length.len() == 1 && length[0] == b'0' {
            return Ok(vec![0, 0, 0, 0]);
        }

        while length.len() < 4 {
            length.insert(0, 0);
        }

        let mut buf = vec![0u8; bytes_to_num(length.as_slice()) as usize];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|e| RdError::Custom(format!("failed to read bulk string: {e:?}")))?;

        if !self.cursor.foresee_crlf() {
            return Err(RdError::Unterminated {
                pos: self.cursor.position(),
                ty: "BulkString",
            });
        }

        let mut ret = Vec::with_capacity(4 + buf.len());
        ret.append(&mut length);
        ret.append(&mut buf);
        Ok(ret)
    }
}

impl<'de, 'a> serde::de::Deserializer<'de> for &'a mut Decoder<'de> {
    type Error = RdError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.parse_any()? {
            ParseResult::SimpleString(v) => visitor.visit_string(v),
            ParseResult::SimpleError(v) => visitor.visit_string(v),
            ParseResult::Integer(v) => visitor.visit_i64(v),
            ParseResult::BulkString(v) => visitor.visit_byte_buf(v),
            ParseResult::Array(count) => {
                if count == -1 {
                    // Null array.
                    visitor.visit_seq(Concatenated::null(self))
                } else {
                    // Have zero or more elements.
                    visitor.visit_seq(Concatenated::new(self, count as u32))
                }
            }
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
        self.deserialize_any(visitor)
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

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_any(visitor)
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
        self.deserialize_any(visitor)
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
        // Array.
        self.deserialize_any(visitor)
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
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if name == KEY_VALUE_ENUM {
            // Parse any value.
            match self.parse_any()? {
                // FIXME: Remove the string hack for Value.
                // We prepend a char to indicate the content type.
                ParseResult::SimpleString(mut v) => {
                    v.insert(0, '+');
                    visitor.visit_string(v)
                }
                ParseResult::SimpleError(mut v) => {
                    v.insert(0, '-');
                    visitor.visit_string(v)
                }
                ParseResult::Integer(v) => visitor.visit_i64(v),
                ParseResult::BulkString(items) => visitor.visit_byte_buf(items),
                ParseResult::Array(count) => {
                    if count == -1 {
                        // Null array.
                        visitor.visit_seq(Concatenated::null(self))
                    } else {
                        // Have zero or more elements.
                        visitor.visit_seq(Concatenated::new(self, count as u32))
                    }
                }
            }
        } else {
            todo!()
        }
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

/// Represents concatenated elements.
///
/// No seprateror between elements.
struct Concatenated<'a, 'de: 'a> {
    /// The deserializer.
    de: &'a mut Decoder<'de>,

    /// The count of elements concated together.
    count: u32,

    /// Flag indicating is pending the first element or not.
    first: bool,

    /// Flag indicating current array is null arary or not.
    is_null: bool,
}

impl<'a, 'de: 'a> Concatenated<'a, 'de> {
    fn new(de: &'a mut Decoder<'de>, element_count: u32) -> Self {
        Self {
            de,
            count: element_count,
            first: true,
            is_null: false,
        }
    }

    fn null(de: &'a mut Decoder<'de>) -> Self {
        Self {
            de,
            count: 0,
            first: true,
            is_null: true,
        }
    }
}

impl<'de, 'a> SeqAccess<'de> for Concatenated<'a, 'de> {
    type Error = RdError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if self.first {
            self.first = false;
            // FIXME: Remove the array hack.
            // Here we "insert" a simple string to indicate it is a null array or not.
            if self.is_null {
                let flag = seed.deserialize(&mut Decoder::from_bytes(b"+\r\n"))?;
                return Ok(Some(flag));
            } else {
                let flag = seed.deserialize(&mut Decoder::from_bytes(b"+1\r\n"))?;
                return Ok(Some(flag));
            }
        }

        if self.count <= 0 {
            // No more elements.
            return Ok(None);
        }

        let v = seed.deserialize(&mut *self.de)?;
        self.count -= 1;
        Ok(Some(v))
    }
}

// pub(super) struct Valued<'a, 'de: 'a> {
//     de: &'a mut Decoder<'de>,
//     value: ParseResult,
// }
//
// impl<'a, 'de: 'a> Valued<'a, 'de> {
//     fn new(de: &'a mut Decoder<'de>, value: ParseResult) -> Self {
//         Self { de, value }
//     }
// }
//
// impl<'de, 'a> EnumAccess<'de> for Valued<'a, 'de> {
//     type Error = RdError;
//
//     type Variant = Self;
//
//     fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
//     where
//         V: serde::de::DeserializeSeed<'de>,
//     {
//         let val = seed.deserialize(&mut *self.de)?;
//         Ok((val, self))
//     }
// }
//
// impl<'de, 'a> VariantAccess<'de> for Valued<'a, 'de> {
//     type Error = RdError;
//
//     fn unit_variant(self) -> Result<(), Self::Error> {
//         Err(serde::de::Error::custom(
//             "unit variant `{ value }` is not supported",
//         ))
//     }
//
//     fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
//     where
//         T: serde::de::DeserializeSeed<'de>,
//     {
//         Err(serde::de::Error::custom(
//             "type variant `{ key: value }` is not supported",
//         ))
//     }
//
//     fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         Err(serde::de::Error::custom(
//             "tuple variant `{ key: [value...] }` is not supported",
//         ))
//     }
//
//     fn struct_variant<V>(
//         self,
//         _fields: &'static [&'static str],
//         _visitor: V,
//     ) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         Err(serde::de::Error::custom(
//             "struct variant `{ name: { k: v, ... } }` is not supported",
//         ))
//     }
// }

impl<'a, 'de: 'a> serde::de::IntoDeserializer<'de, RdError> for &'de mut Decoder<'a> {
    type Deserializer = Self;
    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

pub fn from_bytes<'de, T>(s: &'de [u8]) -> Result<T, RdError>
where
    T: serde::de::Deserialize<'de>,
{
    serde::de::Deserialize::deserialize(&mut Decoder::from_bytes(s))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_decode_string() {
        let s: String = from_bytes(b"+OK\r\n").unwrap();
        assert_eq!(s.as_str(), "OK");
    }
}
