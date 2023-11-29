use crate::TimeStamp;

/// Public trait that Memory table structures must implement.
pub trait SegmentTrait {
    /// Inserts a new key with the corresponding value and time stamp.
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp);
    /// Logically removes an element from the structure.
    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool;
    /// Returns the value of some key if it exists.
    fn get(&self, key: &[u8]) -> Option<Box<[u8]>>;
    /// Returns bytes of the structure.
    fn serialize(&self) -> Box<[u8]>;
    /// Empties all inner elements of structure.
    fn empty(&mut self);
}
