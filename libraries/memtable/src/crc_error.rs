#[derive(Debug)]
pub struct CRCError(pub(crate) u32);

impl std::fmt::Display for CRCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CRC of record {} does not match", self.0)
    }
}

impl std::error::Error for CRCError {}