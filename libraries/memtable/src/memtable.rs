use std::error::Error;
use std::path::{Path, PathBuf};
use b_tree::BTree;
use db_config::{DBConfig, MemoryTableType};
use skip_list::SkipList;
use segment_elements::TimeStamp;
use write_ahead_log::WriteAheadLog;

pub(crate) struct MemoryTable {
    capacity: usize,
    len: usize,
    inner_mem: Box<dyn segment_elements::SegmentTrait + Send>,
    wal_dir: PathBuf,
    associated_wals: Vec<WriteAheadLog>
}

impl<'a> MemoryTable {
    pub(crate) fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let inner_mem: Box<dyn segment_elements::SegmentTrait + Send> = match dbconfig.memory_table_type {
            MemoryTableType::SkipList => Box::new(SkipList::new(dbconfig.skip_list_max_level)),
            MemoryTableType::HashMap => todo!(),
            MemoryTableType::BTree => Box::new(BTree::new(dbconfig.b_tree_order)?)
        };

        let wal_dir= PathBuf::from(&dbconfig.write_ahead_log_dir);

        Ok(MemoryTable {
            inner_mem,
            capacity: dbconfig.memory_table_capacity,
            len: 0,
            associated_wals: vec![WriteAheadLog::new(Path::new(&wal_dir))?],
            wal_dir,
        })
    }

    /// Inserts or updates a key value pair into the memory table. Returns true
    /// if the memory table capacity is reached.
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool {
        // todo if last wal presao broj entrija, dodaj nov u vektor
        // todo if last wal presao velicinu, dodaj nov i splituj insert
        // todo proveri ovaj drugi unwrap
        self.associated_wals.last_mut().unwrap().insert(key, value, time_stamp).unwrap();
        if self.inner_mem.insert(key, value, time_stamp) {
            self.len += 1;
        }

        self.len as f64 > 0.8 * self.capacity as f64
    }

    /// Logically removes a key value pair if it's present. If it isn't present, inserts a
    /// new entry with tombstone set to true.
    pub(crate) fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        // todo if last wal presao broj entrija, dodaj nov u vektor
        // todo if last wal presao velicinu, dodaj nov i splituj insert
        // todo proveri ovaj drugi unwrap
        self.associated_wals.last_mut().unwrap().delete(key, time_stamp).unwrap();
        if self.inner_mem.delete(key, time_stamp) {
            self.len += 1;
        }

        self.len as f64 > 0.8 * self.capacity as f64
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.inner_mem.get(key)
    }
}