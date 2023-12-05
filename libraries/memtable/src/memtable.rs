use std::error::Error;
use b_tree::BTree;
use db_config::{DBConfig, MemoryTableType};
use skip_list::SkipList;
use segment_elements::TimeStamp;

pub(crate) struct MemoryTable {
    pub(crate) capacity: usize,
    pub(crate) len: usize,
    pub(crate) inner_mem: Box<dyn segment_elements::SegmentTrait + Send>,
}

impl MemoryTable {
    pub(crate) fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let inner_mem: Box<dyn segment_elements::SegmentTrait + Send> = match dbconfig.memory_table_type {
            MemoryTableType::SkipList => Box::new(SkipList::new(dbconfig.skip_list_max_level)),
            MemoryTableType::HashMap => unimplemented!(),
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

    pub(crate) fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        self.inner_mem.delete(key, time_stamp)
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.inner_mem.get(key)
    }

    pub(crate) fn flush(&mut self) -> Box<[u8]> {
        self.len = 0;
        let mem_bytes = self.inner_mem.serialize();
        self.inner_mem.empty();

        mem_bytes
    }
}