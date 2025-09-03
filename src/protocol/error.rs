use std::fmt::Display;

use serde::ser::StdError;

#[derive(Debug)]
pub enum RdError {
    /// Custom types of error.
    Custom(String),
}

impl Display for RdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
