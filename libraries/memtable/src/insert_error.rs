#[derive(Debug)]
pub struct InsertError;

impl std::fmt::Display for InsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: all memory tables used up, insert failed")
    }
}

impl std::error::Error for InsertError {}