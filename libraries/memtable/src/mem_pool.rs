use std::collections::VecDeque;
use std::error::Error;
use std::{io, rc};
use std::path::Path;
use threadpool::ThreadPool;
use segment_elements::TimeStamp;
use crate::memtable::MemoryTable;
use crate::record_iterator::RecordIterator;
use db_config::DBConfig;

pub struct MemoryPool {
    read_write_table: MemoryTable,
    read_only_tables: VecDeque<MemoryTable>,
    config: DBConfig,
    thread_pool: ThreadPool,
}

impl MemoryPool {
    pub fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        Ok(MemoryPool {
            config: dbconfig.clone(),
            read_only_tables: VecDeque::with_capacity(dbconfig.memory_table_pool_num),
            read_write_table: MemoryTable::new(dbconfig)?,
            thread_pool: ThreadPool::new(100)
        })
    }

    /// Inserts the key with the corresponding value in the read write memory table.
    pub fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> io::Result<()> {
        if self.read_write_table.insert(key, value, time_stamp, true)? {
            self.swap();
        }

        Ok(())
    }

    /// Logically deletes an element in-place, and updates the number of elements if
    /// the delete is "adding" a new element.
    pub fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> io::Result<()> {
        if self.read_write_table.delete(key, time_stamp, true)? {
            self.swap();
        }

        Ok(())
    }

    /// Tries to retrieve key's data from all memory tables currently loaded in memory.
    /// Does not go into on-disk structures.
    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        if self.read_write_table.is_empty() {
            return None;
        }

        if let Some(data) = self.read_write_table.get(key) {
            return if !data.is_empty() {
                Some(data)
            } else {
                None
            }
        }

        for table in &self.read_only_tables {
            if table.is_empty() {
                return None;
            }

            if let Some(data) = table.get(key) {
                return if !data.is_empty() {
                    Some(data)
                } else {
                    None
                }
            }
        }

        None
    }

    /// Joins all threads that are writing memory tables. This is a blocking operation.
    pub fn join_concurrent_writes(&mut self) {
        self.thread_pool.join();
    }

    /// Swaps the current read write memory table with a new one. Checks if the number of read only
    /// memory tables exceeds the capacity, and flushes the last one if necessary.
    fn swap(&mut self) {
        // unwrap allowed because any error would have been cleared in the pool creation
        // unchecked unwrap allows faster performance as it doesn't do any runtime checks
        let old_read_write = std::mem::replace(
            &mut self.read_write_table, unsafe { MemoryTable::new(&self.config).unwrap_unchecked() }
        );

        self.read_only_tables.push_front(old_read_write);
        if self.read_only_tables.len() == self.config.memory_table_pool_num {
            // unwrap allowed because if condition will never be true when unwrap can panic
            let to_be_flushed = unsafe { self.read_only_tables.pop_back().unwrap_unchecked() };
            self.flush_concurrent(to_be_flushed);
        }
    }

    // fn flush_concurrent(&mut self, table: MemoryTable) {
    //     let density_move = self.config.summary_density;
    //
    //     self.thread_pool.execute(move || {
    //         // todo: LSM sturktura treba da pozove kreiranje nove sstabele i potencionalno da ona radi kompakcije i
    //         // todo mergeovanje ovde, a ako ne ovde onda se radi u main db strukturi
    //         println!("FLUSH");
    //
    //         match table.finalize() {
    //             Ok(_) => (),
    //             Err(e) => eprintln!("WAL couldn't be deleted. Error: {}", e)
    //         };
    //     });
    // }

    /// Loads from every log file in the given directory.
    pub fn load_from_dir(config: &DBConfig) -> Result<MemoryPool, Box<dyn Error>> {
        let mut pool = MemoryPool::new(config)?;

        for entry in RecordIterator::new(Path::new(&config.write_ahead_log_dir))? {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    eprintln!("{}", e);
                    continue
                }
            };

            if entry.tombstone {
                pool.read_write_table.delete(&entry.key, TimeStamp::Custom(entry.timestamp), false)?;
            } else {
                pool.read_write_table.insert(&entry.key, &entry.value.unwrap(), TimeStamp::Custom(entry.timestamp), false)?;
            }
        }

        Ok(pool)
    }
}