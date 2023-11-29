use crate::TimeStamp;

/// Public struct that SegmentTrait implementations return on get.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryEntry {
    value: Box<[u8]>,
    tombstone: bool,
    timestamp: u128
}

impl MemoryEntry {
    pub fn from(value: &[u8], tombstone: bool, timestamp: u128) -> Self {
        MemoryEntry {
            value: Box::from(value),
            timestamp,
            tombstone
        }
    }

    pub fn get_value(&self) -> Box<[u8]> {
        self.value.clone()
    }

    pub fn get_tombstone(&self) -> bool {
        self.tombstone
    }

    pub fn get_timestamp(&self) -> u128 {
        self.timestamp
    }

    pub fn set_value(&mut self, value: &[u8]) { self.value = Box::from(value); }

    pub fn set_tombstone(&mut self, tombstone: bool) { self.tombstone = tombstone; }

    pub fn set_timestamp(&mut self, time_stamp: TimeStamp) { self.timestamp = time_stamp.get_time(); }
}

