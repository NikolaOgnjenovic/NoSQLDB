use crate::{MemoryEntry, TimeStamp};

/// Public trait that Memory table structures must implement.
pub trait SegmentTrait {
    /// Inserts a new key with the corresponding value and time stamp.
    /// Returns false if updating the value, otherwise true.
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool;
    /// Logically removes an element from the structure.
    /// Returns false if the key is present in MemTable, otherwise true and inserts new element with tombstone
    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool;
    /// Returns the value of some key if it exists.
    fn get(&self, key: &[u8]) -> Option<MemoryEntry>;
    /// Empties all inner elements of structure.
    fn empty(&mut self);
    /// Returns an iterator over the elements of the structure.
    fn iterator(&self) -> Box<dyn Iterator<Item = (Box<[u8]>, MemoryEntry)> + '_>;
    /// Returns the size in bytes of the object.
    fn byte_size(&self) -> usize;
}
