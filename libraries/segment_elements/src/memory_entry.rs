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
}