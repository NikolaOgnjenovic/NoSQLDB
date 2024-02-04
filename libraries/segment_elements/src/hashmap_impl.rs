use crate::hashmap_iterator::SortedHashMapIterator;
use crate::{MemoryEntry, SegmentTrait, TimeStamp};
use std::collections::HashMap;
pub struct MemEntryHashMap {
    inner_hashmap: HashMap<Box<[u8]>, MemoryEntry>,
}

impl SegmentTrait for MemEntryHashMap {
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool {
        let entry = MemoryEntry::from(value, false, time_stamp.get_time());
        self.inner_hashmap.insert(Box::from(key), entry).is_none()
    }

    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        if let Some(entry) = self.inner_hashmap.get_mut(key) {
            *entry = MemoryEntry::from(&[], true, time_stamp.get_time());
            false
        } else {
            let entry = MemoryEntry::from(&[], true, time_stamp.get_time());
            self.inner_hashmap.insert(Box::from(key), entry);
            true
        }
    }

    fn get(&self, key: &[u8]) -> Option<MemoryEntry> {
        self.inner_hashmap.get(key).cloned()
    }

    fn iterator(&self) -> Box<dyn Iterator<Item = (Box<[u8]>, MemoryEntry)> + '_> {
        Box::new(SortedHashMapIterator::new(&self.inner_hashmap))
    }
}

impl MemEntryHashMap {
    pub fn new() -> Self {
        MemEntryHashMap {
            inner_hashmap: HashMap::new(),
        }
    }
}
