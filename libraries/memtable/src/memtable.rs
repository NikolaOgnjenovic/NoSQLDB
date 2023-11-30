use b_tree::{BTree, OrderError};
use skip_list::SkipList;
use segment_elements::TimeStamp;

pub(crate) struct MemoryTable<T: segment_elements::SegmentTrait> {
    pub(crate) capacity: usize,
    pub(crate) len: usize,
    pub(crate) inner_mem: T,
}

impl<T: segment_elements::SegmentTrait> MemoryTable<T> {
    /// Inserts or updates a key value pair into the memory table. Returns true
    /// if the memory table capacity is reached.
    pub fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool {
        if self.inner_mem.insert(key, value, time_stamp) {
            self.len += 1;
        }

        self.len == self.capacity
    }

    pub fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        self.inner_mem.delete(key, time_stamp)
    }

    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.inner_mem.get(key)
    }

    pub fn flush(&mut self) -> Box<[u8]> {
        self.len = 0;
        let mem_bytes = self.inner_mem.serialize();
        self.inner_mem.empty();

        mem_bytes
    }
}

impl MemoryTable<SkipList> {
    pub fn new(capacity: usize, max_level: usize) -> Self {
        MemoryTable {
            capacity,
            len: 0,
            inner_mem: SkipList::new(max_level),
        }
    }
}

impl MemoryTable<BTree> {
    pub fn new(capacity: usize, order: usize) -> Result<Self, OrderError> {
        Ok(MemoryTable {
            capacity,
            len: 0,
            inner_mem: BTree::new(order)?,
        })
    }
}
