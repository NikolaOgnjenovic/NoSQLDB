use std::error::Error;
use std::fmt;
use std::io;


#[derive(Debug)]
pub struct ReservedKeyError {
    pub message: String
}

impl fmt::Display for ReservedKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<ReservedKeyError> for io::Error {
    fn from(error: ReservedKeyError) -> Self {
        io::Error::new(io::ErrorKind::Other, error.message)
    }
}

impl Error for ReservedKeyError {}