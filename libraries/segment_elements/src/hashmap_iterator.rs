use crate::MemoryEntry;
use std::collections::HashMap;

pub struct SortedHashMapIterator<'a> {
    keys: Vec<&'a Box<[u8]>>,
    index: usize,
    hash_map: &'a HashMap<Box<[u8]>, MemoryEntry>,
}

impl<'a> SortedHashMapIterator<'a> {
    pub fn new(hash_map: &'a HashMap<Box<[u8]>, MemoryEntry>) -> Self {
        let mut keys: Vec<&Box<[u8]>> = hash_map.keys().collect();
        keys.sort();
        // keys.sort_by(|a, b| a.cmp(b));

        SortedHashMapIterator {
            keys,
            index: 0,
            hash_map,
        }
    }
}

impl<'a> Iterator for SortedHashMapIterator<'a> {
    type Item = (Box<[u8]>, MemoryEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&key) = self.keys.get(self.index) {
            self.index += 1;

            if let Some(entry) = self.hash_map.get(key) {
                Some((key.clone(), entry.clone()))
            } else {
                None
            }
        } else {
            None
        }
    }
}