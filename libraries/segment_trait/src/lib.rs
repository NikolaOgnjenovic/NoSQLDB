/// Public trait that Memory table structures should implement.
pub trait SegmentTrait {
    fn insert(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]) -> bool;
    fn get(&self, key: &[u8]) -> MemoryEntry;
    fn empty(&mut self);
}

/// Public struct that SegmentTrait implementations return on get.
pub struct MemoryEntry {
    value: Box<[u8]>,
    tombstone: bool,
    timestamp: u128
}