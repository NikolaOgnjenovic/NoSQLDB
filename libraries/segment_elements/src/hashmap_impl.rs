use crate::hashmap_iterator::SortedHashMapIterator;
use crate::{MemoryEntry, SegmentTrait, TimeStamp};
use std::collections::HashMap;
pub struct MemEntryHashMap(HashMap<Box<[u8]>, MemoryEntry>);

impl SegmentTrait for MemEntryHashMap {
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool {
        let entry = MemoryEntry::from(value, false, time_stamp.get_time());
        self.0.insert(Box::from(key), entry).is_none()
    }

    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        if let Some(entry) = self.0.get_mut(key) {
            *entry = MemoryEntry::from(&[], true, time_stamp.get_time());
            false
        } else {
            let entry = MemoryEntry::from(&[], true, time_stamp.get_time());
            self.0.insert(Box::from(key), entry);
            true
        }
    }

    fn get(&self, key: &[u8]) -> Option<MemoryEntry> {
        self.0.get(key).cloned()
    }

    fn empty(&mut self) {
        self.0.clear();
    }

    fn iterator(&self) -> Box<dyn Iterator<Item = (Box<[u8]>, MemoryEntry)> + '_> {
        Box::new(SortedHashMapIterator::new(&self.0))
    }
}

impl MemEntryHashMap {
    pub fn new() -> Self {
        MemEntryHashMap(HashMap::new())
    }
}
