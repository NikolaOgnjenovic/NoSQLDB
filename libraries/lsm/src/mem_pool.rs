mod record_iterator;

use crate::mem_pool::record_iterator::RecordIterator;
use crate::memtable::MemoryTable;
use db_config::DBConfig;
use segment_elements::{MemoryEntry, TimeStamp};
use std::collections::VecDeque;
use std::error::Error;
use std::path::Path;

pub(crate) struct MemoryPool {
    read_write_table: MemoryTable,
    read_only_tables: VecDeque<MemoryTable>,
    config: DBConfig,
}

impl MemoryPool {
    pub(crate) fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        Ok(MemoryPool {
            config: dbconfig.clone(),
            read_only_tables: VecDeque::with_capacity(dbconfig.memory_table_pool_num),
            read_write_table: MemoryTable::new(dbconfig)?,
        })
    }

    /// Inserts the key with the corresponding value in the read write memory table.
    pub(crate) fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        time_stamp: TimeStamp,
    ) -> Option<MemoryTable> {
        if self.read_write_table.insert(key, value, time_stamp) {
            return self.swap();
        }

        None
    }

    /// Logically deletes an element in-place, and updates the number of elements if
    /// the deletion is "adding" a new element.
    pub(crate) fn delete(
        &mut self,
        key: &[u8],
        time_stamp: TimeStamp,
    ) -> Option<MemoryTable> {
        if self.read_write_table.delete(key, time_stamp) {
            return self.swap();
        }

        None
    }

    /// Tries to retrieve key's data from all memory tables currently loaded in memory.
    /// Does not go into on-disk structures.
    pub(crate) fn get(&self, key: &[u8]) -> Option<MemoryEntry> {
        if let Some(memory_entry) = self.read_write_table.get(key) {
            return Some(memory_entry);
        }

        for table in &self.read_only_tables {
            if table.is_empty() {
                return None;
            }

            if let Some(memory_entry) = table.get(key) {
                return Some(memory_entry);
            }
        }

        None
    }

    /// Swaps the current read write memory table with a new one. Checks if the number of read only
    /// memory tables exceeds the capacity, and flushes the last one if necessary.
    fn swap(&mut self) -> Option<MemoryTable> {
        // unwrap allowed because any error would have been cleared in the pool creation
        // unchecked unwrap allows faster performance as it doesn't do any runtime checks
        let old_read_write = std::mem::replace(&mut self.read_write_table, unsafe {
            MemoryTable::new(&self.config).unwrap_unchecked()
        });

        self.read_only_tables.push_front(old_read_write);
        if self.read_only_tables.len() == self.config.memory_table_pool_num {
            // unwrap allowed because if condition will never be true when unwrap can panic
            let to_be_flushed = unsafe { self.read_only_tables.pop_back().unwrap_unchecked() };
            return Some(to_be_flushed);
        }

        None
    }

    pub(crate) fn get_all_tables(&self) -> Vec<Vec<(Box<[u8]>, MemoryEntry)>> {
        let mut memory_tables = Vec::new();
        memory_tables.push(self.read_write_table.iterator().collect());
        for memory_table in &self.read_only_tables {
            memory_tables.push(memory_table.iterator().collect());
        }

        memory_tables
    }

    /// Loads from every log file in the given directory.
    pub(crate) fn load_from_dir(config: &DBConfig) -> Result<(MemoryPool, Vec<MemoryTable>), Box<dyn Error>> {
        let mut pool = MemoryPool::new(config)?;
        let mut to_be_flushed = vec![];

        for entry in RecordIterator::new(Path::new(&config.write_ahead_log_dir))? {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    eprintln!("{}", e);
                    continue;
                }
            };

            if entry.tombstone {
                if pool
                    .read_write_table
                    .delete(&entry.key, TimeStamp::Custom(entry.timestamp))
                {
                    if let Some(table) = pool.swap() {
                        to_be_flushed.push(table);
                    }
                }
            } else {
                if pool.read_write_table.insert(
                    &entry.key,
                    &entry.value.unwrap(),
                    TimeStamp::Custom(entry.timestamp),
                ) {
                    if let Some(table) = pool.swap() {
                        to_be_flushed.push(table);
                    }
                }
            }
        }

        Ok((pool, to_be_flushed))
    }
}
