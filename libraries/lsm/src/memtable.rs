use b_tree::BTree;
use db_config::{DBConfig, MemoryTableType};
use segment_elements::{MemEntryHashMap, MemoryEntry, TimeStamp};
use skip_list::SkipList;
use std::error::Error;

pub(crate) struct MemoryTable {
    capacity: usize,
    len: usize,
    inner_mem: Box<dyn segment_elements::SegmentTrait + Send>,
}

impl MemoryTable {
    pub(crate) fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let inner_mem: Box<dyn segment_elements::SegmentTrait + Send> =
            match dbconfig.memory_table_type {
                MemoryTableType::SkipList => Box::new(SkipList::new(dbconfig.skip_list_max_level)),
                MemoryTableType::HashMap => Box::new(MemEntryHashMap::new()),
                MemoryTableType::BTree => Box::new(BTree::new(dbconfig.b_tree_order)?),
            };

        Ok(MemoryTable {
            inner_mem,
            capacity: dbconfig.memory_table_capacity,
            len: 0,
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

    pub(crate) fn get(&self, key: &[u8]) -> Option<MemoryEntry> {
        self.inner_mem.get(key)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn iterator(&self) -> Box<dyn Iterator<Item = (Box<[u8]>, MemoryEntry)> + '_> {
        self.inner_mem.iterator()
    }

    /// Returns the WAL size of the current table.
    /// This means that for each element in the table, 20 bytes (8 for key len, 8 for value len and 4 for CRC)
    /// is added to the size.
    pub(crate) fn wal_size(&self) -> usize {
        20 * self.len + self.inner_mem.byte_size()
    }
}
