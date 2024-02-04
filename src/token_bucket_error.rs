use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct TokenBucketError;

impl Display for TokenBucketError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Input rate limit exceeded. Please try again later.")
    }
}

impl From<TokenBucketError> for std::io::Error {
    fn from(error: TokenBucketError) -> Self {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("TokenBucketError: {}", error),
        )
    }
}

impl std::error::Error for TokenBucketError {}
