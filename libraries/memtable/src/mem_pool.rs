use std::collections::VecDeque;
use std::error::Error;
use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use segment_elements::TimeStamp;
use crate::insert_error::InsertError;
use crate::memtable::MemoryTable;
use crate::record_iterator::RecordIterator;
use db_config::DBConfig;

pub struct MemoryPool {
    read_write_table: MemoryTable,
    read_only_tables: VecDeque<MemoryTable>,
    config: DBConfig,
    join_handles: Vec<JoinHandle<()>>
}

impl MemoryPool {
    pub fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        Ok(MemoryPool {
            read_only_tables: VecDeque::with_capacity(dbconfig.memory_table_pool_num),
            config: dbconfig.clone(),
            read_write_table: MemoryTable::new(dbconfig)?,
            // todo handle 32000+ threads, potentially without join
            join_handles: Vec::new()
        })
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> Result<(), InsertError> {
        if self.read_write_table.insert(key, value, time_stamp) {
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

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        // todo
        self.read_write_table.delete(key, time_stamp)
    }

    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        if let Some(data) = self.read_write_table.get(key) {
            return Some(data);
        }

        for table in &self.read_only_tables {
            if let Some(data) = table.get(key) {
                return Some(data);
            }
        }

        None
    }

    fn flush_concurrent(&mut self, mut table: MemoryTable) {
        let handle = std::thread::spawn(move || {
            // todo SSTable::from should be called here

            let _serialized_data = table.flush();
        });

        self.join_handles.push(handle);
    }

    /// Loads from every log file in the given directory.
    // todo add low water mark wal logs removal index
    pub fn load_from_dir(dir: &Path, config: &DBConfig) -> Result<MemoryPool, Box<dyn Error>> {
        let mut files = read_dir(dir)?
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.extension().unwrap() == ".log")
            .collect::<Vec<PathBuf>>();

        files.sort();

        let mut pool = MemoryPool::new(config)?;

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
                    pool.delete(&entry.key, TimeStamp::Custom(entry.timestamp));
                } else {
                    pool.insert(&entry.key, &entry.value.unwrap(), TimeStamp::Custom(entry.timestamp))?;
                }
            }
        }

        Ok(pool)
    }
}