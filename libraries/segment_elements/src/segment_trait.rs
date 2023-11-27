use crate::TimeStamp;

/// Public trait that Memory table structures should implement.
pub trait SegmentTrait {
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp);
    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool;
    fn get(&self, key: &[u8]) -> Option<Box<[u8]>>;
    fn empty(&mut self);
}
