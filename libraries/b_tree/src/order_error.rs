use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct OrderError;

impl Display for OrderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "The BTree order should not be below 2")
    }
}

impl std::error::Error for OrderError {}
