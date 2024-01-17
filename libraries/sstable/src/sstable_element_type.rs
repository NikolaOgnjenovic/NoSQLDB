/// Enum representing an SSTable element type used for reading and writing different kinds of data.
#[derive(PartialEq)]
pub(crate) enum SSTableElementType {
    Data,
    Index,
    Summary,
    BloomFilter,
    MerkleTree,
}
impl SSTableElementType {
    /// Get the numeric identifier associated with each SSTableElementType.
    pub(crate) fn get_id(&self) -> usize {
        match self {
            SSTableElementType::Data => 0,
            SSTableElementType::Index => 1,
            SSTableElementType::Summary => 2,
            SSTableElementType::BloomFilter => 3,
            SSTableElementType::MerkleTree => 4,
        }
    }
}