use b_tree::{BTree, OrderError};
use skip_list::SkipList;
use segment_elements::TimeStamp;

pub struct MemoryTable<T: segment_elements::SegmentTrait> {
    capacity: usize,
    len: usize,
    inner_mem: T
}

impl<T: segment_elements::SegmentTrait> MemoryTable<T> {
    pub fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) {
        self.inner_mem.insert(key, value, time_stamp);

        self.len += 1;
        if self.len == self.capacity {
            // todo mempool swap, not implemented currently
        }
    }

    pub fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        self.inner_mem.delete(key, time_stamp)
    }

    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.inner_mem.get(key)
    }

    pub fn flush(&mut self) {
        self.len = 0;
        // todo flush (...)
        self.inner_mem.empty();
    }
}

impl MemoryTable<SkipList> {
    pub fn new(capacity: usize, max_level: usize) -> Self {
        MemoryTable {
            capacity,
            len: 0,
            inner_mem: SkipList::new(max_level)
        }
    }
}

impl MemoryTable<BTree> {
    pub fn new(capacity: usize, order: usize) -> Result<Self, OrderError> {
        Ok(MemoryTable {
            capacity,
            len: 0,
            inner_mem: BTree::new(order)?
        })
    }
}
