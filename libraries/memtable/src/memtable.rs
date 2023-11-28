use std::fs::read_dir;
use std::io;
use std::path::{Path, PathBuf};
use b_tree::{BTree, OrderError};
use skip_list::SkipList;
use segment_elements::TimeStamp;
use crate::record_iterator::RecordIterator;

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

    /// Loads from every log file in the given directory.
    fn load_from_dir_generic(dir: &Path, mut table: MemoryTable<T>) -> io::Result<MemoryTable<T>> {
        let mut files = read_dir(dir)?
            .map(|file| file.unwrap().path())
            .filter(|file| file.extension().unwrap() == ".log")
            .collect::<Vec<PathBuf>>();
        files.sort();

        for file in files.iter() {
            for entry in RecordIterator::new(file)?.into_iter() {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        eprintln!("{}", e);
                        continue
                    }
                };

                if entry.tombstone {
                    table.delete(&entry.key, TimeStamp::Custom(entry.timestamp));
                } else {
                    table.insert(&entry.key, &entry.value.unwrap(), TimeStamp::Custom(entry.timestamp));
                }
            }
        }

        Ok(table)
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

    pub fn load_from_dir(dir: &Path) -> io::Result<MemoryTable<SkipList>> {
        // todo make cap and max level variable
        let table = MemoryTable::<SkipList>::new(100, 100);
        MemoryTable::load_from_dir_generic(dir, table)
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

    pub fn load_from_dir(dir: &Path) -> io::Result<MemoryTable<BTree>> {
        // todo make cap and order variable
        let table = MemoryTable::<BTree>::new(100, 100).unwrap();
        MemoryTable::load_from_dir_generic(dir, table)
    }
}
