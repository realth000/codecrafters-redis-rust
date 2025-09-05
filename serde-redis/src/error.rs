use std::fmt::Display;

use serde::ser::StdError;

/// Packed result type serializing and deserializing redis protocol data.
pub(super) type RdResult<T> = std::result::Result<T, RdError>;

#[derive(Debug)]
pub enum RdError {
    IoError(std::io::Error),

    InvalidUtf8String(std::string::FromUtf8Error),

    InvalidUtf8Str(std::str::Utf8Error),

    /// Failed to deserialize.
    InvalidPrefix {
        /// Position where the prefix is incorrect.
        pos: u64,

        /// What type is expected to have.
        ty: &'static str,

        /// Expected prefix of the expected type.
        expected: &'static str,
    },

    /// Unknown prefix means previous process ends abnormally, or some type in the protocol
    /// that we do not support.
    UnknownPrefix {
        /// Position where the prefix is unknown.
        pos: u64,

        /// The prefix we get.
        prefix: u8,
    },

    /// Some primitive types are not supported when serializing and deserializing.
    UnsupportedPrimitiveType {
        /// Currentr using type, which is not supported.
        curr: &'static str,

        /// Use which type instead.
        replace: &'static str,
    },

    /// The trailing CRLF is missing in some position.
    Unterminated {
        /// Position where the type not terminated.
        pos: u64,

        /// What type in the current position.
        ty: &'static str,
    },

    /// The length section in sequence types is incorrect.
    ///
    /// Array, Map, Set and other types that have elements indicator section
    /// may encounter this error.
    ///
    /// Generally the length is rather a value greater than 0 or -1 (may be allowed).
    InvalidSeqLength {
        /// The position where length is incorrect.
        pos: u64,

        /// The type name.
        ty: &'static str,

        /// The invalid length value.
        value: i64,
    },

    EOF,

    /// Custom types of error.
    Custom(String),
}

impl Display for RdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdError::IoError(e) => f.write_fmt(format_args!("IO error: {e:?}")),
            RdError::InvalidUtf8String(e) => {
                f.write_fmt(format_args!("invalid utf8 string: {e:?}"))
            }
            RdError::InvalidUtf8Str(e) => f.write_fmt(format_args!("invalid utf8 str: {e:?}")),
            RdError::InvalidPrefix { pos, ty, expected } => f.write_fmt(format_args!(
                "expected prefix {expected} for type {ty} at pos {pos}"
            )),
            RdError::UnknownPrefix { pos, prefix } => {
                f.write_fmt(format_args!("Unknown type prefix {prefix} at {pos}"))
            }
            RdError::UnsupportedPrimitiveType { curr, replace } => f.write_fmt(format_args!(
                "unsupported primitive type {curr}, use {replace} instead"
            )),
            RdError::Unterminated { pos, ty } => {
                f.write_fmt(format_args!("unterminated {ty} at {pos}"))
            }
            RdError::InvalidSeqLength { pos, ty, value } => f.write_fmt(format_args!(
                "invalid length section value {value} for type {ty} at {pos}"
            )),
            RdError::EOF => f.write_str("EOF"),
            RdError::Custom(v) => f.write_str(v.as_str()),
        }
    }
}

impl StdError for RdError {
    fn source(&self) -> Option<&(dyn serde::ser::StdError + 'static)> {
        None
    }
}

impl serde::ser::Error for RdError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        RdError::Custom(msg.to_string())
    }
}

impl serde::de::Error for RdError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        RdError::Custom(msg.to_string())
    }
}
