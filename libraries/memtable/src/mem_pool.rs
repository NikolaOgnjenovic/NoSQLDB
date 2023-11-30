use std::fs::read_dir;
use std::io;
use std::path::{Path, PathBuf};
use b_tree::{BTree, OrderError};
use segment_elements::TimeStamp;
use skip_list::SkipList;
use crate::insert_error::InsertError;
use crate::memtable::MemoryTable;
use crate::record_iterator::RecordIterator;

pub struct MemoryPool<T: segment_elements::SegmentTrait> {
    active_memory_table: MemoryTable<T>,
    inactive_memory_tables: Vec<MemoryTable<T>>,
}

impl<T: segment_elements::SegmentTrait> MemoryPool<T> {
    pub fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> Result<(), InsertError> {
        if self.active_memory_table.insert(key, value, time_stamp) {

        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        // todo what if the key is not in current memtable
        self.active_memory_table.delete(key, time_stamp)
    }

    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        // todo should keys be retrieved only from the read write memtable
        self.active_memory_table.get(key)
    }

    /// Loads from every log file in the given directory.
    // todo add low water mark wal logs removal index
    fn load_from_dir_generic(dir: &Path, mut table: MemoryPool<T>) -> io::Result<MemoryPool<T>> {
        let mut files = read_dir(dir)?
            .map(|dir_entry| dir_entry.unwrap().path())
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

    fn flush_concurrent(&mut self, table: MemoryTable<T>) {

    }
}

// todo make minimum number of memtables 2
impl MemoryPool<BTree> {
    pub fn new(num_memory_tables: usize, memory_table_capacity: usize, order: usize) -> Result<Self, OrderError> {
        let mut temp_pool = Self {
            active_memory_table: MemoryTable::<BTree>::new(memory_table_capacity, order)?,
            inactive_memory_tables: Vec::with_capacity(num_memory_tables),
        };

        for _ in 0..num_memory_tables {
            temp_pool.inactive_memory_tables.push(MemoryTable::<BTree>::new(memory_table_capacity, order)?);
        }
        Ok(temp_pool)
    }

    pub fn load_from_dir(dir: &Path) -> io::Result<MemoryPool<BTree>> {
        // todo make cap and order variable
        let table = MemoryPool::<BTree>::new(5, 100, 100).unwrap();
        MemoryPool::load_from_dir_generic(dir, table)
    }
}

impl MemoryPool<SkipList> {
    pub fn new(num_memory_tables: usize, memory_table_capacity: usize, order: usize) -> Self {
        let mut temp_pool = Self {
            active_memory_table: MemoryTable::<SkipList>::new(memory_table_capacity, order),
            inactive_memory_tables: Vec::with_capacity(num_memory_tables),
        };

        for _ in 0..num_memory_tables {
            temp_pool.inactive_memory_tables.push(MemoryTable::<SkipList>::new(memory_table_capacity, order));
        }

        temp_pool
    }

    pub fn load_from_dir(dir: &Path) -> io::Result<MemoryPool<SkipList>> {
        // todo make cap and max level variable
        let table = MemoryPool::<SkipList>::new(5, 100, 100);
        MemoryPool::load_from_dir_generic(dir, table)
    }
}