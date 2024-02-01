use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct TokenBucketError;

impl Display for TokenBucketError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Input rate limit exceeded. Please try again later.")
    }
}

impl std::error::Error for TokenBucketError {}