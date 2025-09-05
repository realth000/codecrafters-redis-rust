use crate::{
    bulk_string::KEY_BULK_STRING_NULL, simple_error::KEY_SIMPLE_ERROR, utils::num_to_bytes,
};

use super::error::{RdError, RdResult};

struct Encoder {
    output: Vec<u8>,
}

impl Encoder {
    fn save_raw(&mut self, mut v: Vec<u8>) {
        self.output.append(&mut v);
    }

    fn append_crlf(&mut self) {
        self.output.extend(b"\r\n");
    }

    fn encode_simple_string(&mut self, v: &[u8]) {
        self.output.push(b'+');
        self.output.extend_from_slice(v);
        self.append_crlf();
    }

    fn encode_integer(&mut self, v: i64) {
        self.output.push(b':');
        if v >= 0 {
            self.output.push(b'+');
        } else {
            self.output.push(b'-');
        }
        let mut value = num_to_bytes(v);
        self.output.append(&mut value);
        self.append_crlf();
    }

    fn encode_bulk_string(&mut self, v: Option<&[u8]>) {
        self.output.push(b'$');
        match v {
            Some(v) => {
                self.output.append(&mut num_to_bytes(v.len() as i64));
                self.append_crlf();
                self.output.extend_from_slice(v);
            }
            None => {
                self.output.extend(b"-1");
            }
        }
        self.append_crlf();
    }

    fn encode_array_prefix(&mut self, len: Option<usize>) {
        self.output.push(b'*');
        match len {
            Some(v) => self.output.append(&mut num_to_bytes(v as i64)),
            None => self.output.extend(b"-1"),
        }
        self.append_crlf();
    }

    fn encode_simple_error_prefix(&mut self) {
        self.output.push(b'-');
    }

    fn encode_null(&mut self) {
        self.output.extend(b"_");
        self.append_crlf();
    }
}

impl<'a> serde::ser::Serializer for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleStruct = Self;

    type SerializeTupleVariant = Self;

    type SerializeMap = Self;

    type SerializeStruct = Self;

    type SerializeStructVariant = Self;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.encode_integer(v);
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.encode_simple_string(&[v as u8]);
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.encode_simple_string(v.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.encode_bulk_string(Some(v));
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.encode_null();
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        if name == KEY_BULK_STRING_NULL {
            // Null bulk string.
            self.encode_bulk_string(None);
            Ok(())
        } else {
            todo!()
        }
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        // Array.
        self.encode_array_prefix(len);
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        if name == KEY_SIMPLE_ERROR {
            self.encode_simple_error_prefix();
            Ok(self)
        } else {
            todo!()
        }
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
}

impl<'a> serde::ser::SerializeSeq for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        // Element in array.
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        // Do nothing.
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTuple for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a> serde::ser::SerializeTupleStruct for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a> serde::ser::SerializeTupleVariant for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a> serde::ser::SerializeMap for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a> serde::ser::SerializeStruct for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        if key == KEY_SIMPLE_ERROR {
            let mut enc = PrimitiveEncoder::new();
            value
                .serialize(&mut enc)
                .inspect(|_| self.save_raw(enc.output))
        } else {
            todo!()
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeStructVariant for &'a mut Encoder {
    type Ok = ();

    type Error = RdError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

/// The serializer for handling premitive types.
///
/// Sometimes we use rust primitive types to carry sections
/// of data in RESP types. When serializing these sections
/// we want to keep the orignal byte content.
struct PrimitiveEncoder {
    output: Vec<u8>,
}

impl PrimitiveEncoder {
    fn new() -> Self {
        Self { output: vec![] }
    }
}

impl<'a> serde::ser::Serializer for &'a mut PrimitiveEncoder {
    type Ok = ();

    type Error = RdError;

    /* Following traits are not used. */
    type SerializeSeq = serde::ser::Impossible<(), Self::Error>;
    type SerializeTuple = serde::ser::Impossible<(), Self::Error>;
    type SerializeTupleStruct = serde::ser::Impossible<(), Self::Error>;
    type SerializeTupleVariant = serde::ser::Impossible<(), Self::Error>;
    type SerializeMap = serde::ser::Impossible<(), Self::Error>;
    type SerializeStruct = serde::ser::Impossible<(), Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<(), Self::Error>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.output.extend(v.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        todo!()
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
}

/// Convert to encoded bytes.
pub fn to_vec<T>(value: &T) -> RdResult<Vec<u8>>
where
    T: ?Sized + serde::ser::Serialize,
{
    let mut serializer = Encoder { output: Vec::new() };
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encode_str() {
        let d = to_vec("OK").unwrap();
        assert_eq!(d, b"+OK\r\n");
    }
}
