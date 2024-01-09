use std::error::Error;
use db_config::DBConfig;
use lru_cache::LRUCache;
use memtable::MemoryPool;
use segment_elements::TimeStamp;

pub struct DB {
    config: DBConfig,
    memory_pool: MemoryPool,
    cache: LRUCache
}

impl Default for DB {
    fn default() -> Self {
        let default_config = DBConfig::default();
        DB {
            cache: LRUCache::new(default_config.cache_max_size),
            memory_pool: MemoryPool::new(&default_config).unwrap(),
            config: default_config,
        }
    }
}

impl DB {
    pub fn build(config: DBConfig) -> Result<Self, Box<dyn Error>> {
        Ok(DB {
            cache: LRUCache::new(config.cache_max_size),
            memory_pool: MemoryPool::new(&config)?,
            config
        })
    }

    /// Reconstructs the last memory table from the WAL. Must be called when the program didn't end
    /// gracefully.
    pub fn reconstruct_from_wal(&mut self) {
        self.memory_pool = MemoryPool::load_from_dir(&self.config).unwrap_or_else(|e| {
            eprintln!("Error occurred: {}", e);
            eprintln!("Memory pool wasn't reconstructed.");
            // unwrap can be called because if a possible error existed, it would have manifested at the
            // DB::build() function
            MemoryPool::new(&self.config).unwrap()
        });
    }

    /// Inserts a new key value pair into the system.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.memory_pool.insert(key, value, TimeStamp::Now);
    }

    /// Removes the value that's associated to the given key.
    pub fn delete(&mut self, key: &[u8]) {
        self.memory_pool.delete(key, TimeStamp::Now);
    }

    /// Retrieves the data that is associated to a given key.
    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        if let Some(value) = self.memory_pool.get(key) {
            return Some(value);
        }

        // todo sstable get, komplikovano

        todo!()
    }

    /// Should be called before the program exit to gracefully finish all memory tables writes,
    /// SStable merges and compactions.
    pub fn shut_down(&mut self) {
        self.memory_pool.join_concurrent_writes();
        // todo join sstable LSM merge-ove i kompakcije
    }
}