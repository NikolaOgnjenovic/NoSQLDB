use std::error::Error;
use std::fs::read_dir;
use std::path::{Path, PathBuf};
use segment_elements::TimeStamp;
use crate::insert_error::InsertError;
use crate::memtable::MemoryTable;
use crate::record_iterator::RecordIterator;
use db_config::DBConfig;

pub struct MemoryPool {
    read_write_table: MemoryTable,
    read_only_tables: Vec<MemoryTable>,
    config: DBConfig,
}

impl MemoryPool {
    pub fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let mut temp_pool = MemoryPool {
            read_only_tables: Vec::with_capacity(dbconfig.memory_table_pool_num),
            config: dbconfig.clone(),
            read_write_table: MemoryTable::new(dbconfig)?,
        };

        for _ in 0..dbconfig.memory_table_pool_num {
            temp_pool.read_only_tables.push(MemoryTable::new(dbconfig)?)
        }

        Ok(temp_pool)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> Result<(), InsertError> {
        if self.read_write_table.insert(key, value, time_stamp) {
            if self.read_only_tables.len() == self.config.memory_table_pool_num {

            }


        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        self.read_write_table.delete(key, time_stamp)
    }

    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        // todo should keys be retrieved only from the read write memtable - no, include all read only tables as well
        self.read_write_table.get(key)
    }

    /// Loads from every log file in the given directory.
    // todo add low water mark wal logs removal index
    fn load_from_dir(dir: &Path, config: &DBConfig) -> Result<MemoryPool, Box<dyn Error>> {
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

    fn flush_concurrent(&mut self, table: MemoryTable) {

    }
}