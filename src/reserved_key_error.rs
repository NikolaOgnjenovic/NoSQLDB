use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct ReservedKeyError {
    pub message: String
}

impl fmt::Display for ReservedKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ReservedKeyError {}