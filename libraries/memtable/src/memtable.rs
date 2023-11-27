pub struct MemoryTable<T: segment_trait::SegmentTrait> {
    capacity: usize,
    len: usize,
    inner_mem: T
}

impl<T: segment_trait::SegmentTrait> MemoryTable<T> {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.inner_mem.insert(key, value);

        self.len += 1;
        if self.len == self.capacity {
            // todo mempool swap, not implemented currently
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.inner_mem.delete(key)
    }

    pub fn get(&self, key: &[u8]) -> segment_trait::MemoryEntry {
        self.inner_mem.get(key)
    }

    pub fn flush(&mut self) {
        self.len = 0;
        // todo empty the table
        // todo for next class
    }
}
