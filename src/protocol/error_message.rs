/// Error message in redis protocol.
///
/// Error message has a '-' prefix, and other part works like string.
///
#[derive(Debug, Clone)]
pub struct ErrorMessage {
    /// Optional prefix of the error message.
    ///
    /// Prefix is the first part after '-' prefix and all letters in prefix are in UPPERCASE.
    ///
    /// Prefix is a convention used by redis rather, not part of protocol, so it's optional.
    ///
    /// When encoding and decoding prefix, it is guaranteed to be UPPERCASE, no matter the
    /// content is already UPPERCASE or not.
    pub prefix: Option<String>,

    /// The message in error.
    ///
    /// Note that this field works like simple string, can not hold CRLF as well.
    pub message: String,
}
