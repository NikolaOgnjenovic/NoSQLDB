use std::error::Error;
use b_tree::BTree;
use db_config::{DBConfig, MemoryTableType};
use skip_list::SkipList;
use segment_elements::{MemoryEntry, TimeStamp};

pub(crate) struct MemoryTable {
    capacity: usize,
    len: usize,
    inner_mem: Box<dyn segment_elements::SegmentTrait + Send>
}

impl MemoryTable {
    pub(crate) fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let inner_mem: Box<dyn segment_elements::SegmentTrait + Send> = match dbconfig.memory_table_type {
            MemoryTableType::SkipList => Box::new(SkipList::new(dbconfig.skip_list_max_level)),
            MemoryTableType::HashMap => todo!(),
            MemoryTableType::BTree => Box::new(BTree::new(dbconfig.b_tree_order)?)
        };

        Ok(MemoryTable {
            inner_mem,
            capacity: dbconfig.memory_table_capacity,
            len: 0
        })
    }

    /// Inserts or updates a key value pair into the memory table. Returns true
    /// if the memory table capacity is reached.
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool {
        if self.inner_mem.insert(key, value, time_stamp) {
            self.len += 1;
        }

        self.len as f64 > 0.8 * self.capacity as f64
    }

    /// Logically removes a key value pair if it's present. If it isn't present, inserts a
    /// new entry with tombstone set to true.
    pub(crate) fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        if self.inner_mem.delete(key, time_stamp) {
            self.len += 1;
        }

        self.len as f64 > 0.8 * self.capacity as f64
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.inner_mem.get(key)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn iterator(&self) -> Box<dyn Iterator<Item = (Box<[u8]>, MemoryEntry)> + '_> {
        self.inner_mem.iterator()
    }

    pub(crate) fn calc_wal_size(&self) -> usize {
        self.inner_mem
            .iterator()
            .map(|(k, v)| k.len() + v.get_value().len() + 1 + 4 + 16 + 8 + 8)
            .sum()
        // 1 byte for tombstone, 4 bytes for CRC, 16 bytes for timestamp, 8 bytes for key_len, 8 bytes for value len
    }
}