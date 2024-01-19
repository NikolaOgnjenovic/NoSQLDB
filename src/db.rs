use std::error::Error;
use db_config::DBConfig;
use lru_cache::LRUCache;
use memtable::MemoryPool;
use segment_elements::TimeStamp;
use bloom_filter::BloomFilter;
use count_min_sketch::CMSketch;
use hyperloglog::HLL;
use simhash::{hamming_distance};
use crate::reserved_key_error::ReservedKeyError;
use crate::probabilistic_type_error::ProbabilisticTypeError;

pub struct DB {
    config: DBConfig,
    memory_pool: MemoryPool,
    cache: LRUCache,
    // System reserved key prefixes for probabilistic data structures and the token bucket
    reserved_key_prefixes: [&'static [u8]; 5]
}

impl Default for DB {
    fn default() -> Self {
        let default_config = DBConfig::default();
        DB {
            cache: LRUCache::new(default_config.cache_max_size),
            memory_pool: MemoryPool::new(&default_config).unwrap(),
            config: default_config,
            reserved_key_prefixes: ["bl00m_f1lt3r/".as_bytes(), "c0unt_m1n_$k3tch/".as_bytes(), "hyp3r_l0g_l0g/".as_bytes(), "$1m_ha$h/".as_bytes(), "t0k3n_buck3t/".as_bytes()]
        }
    }
}

impl DB {
    pub fn build(config: DBConfig) -> Result<Self, Box<dyn Error>> {
        Ok(DB {
            cache: LRUCache::new(config.cache_max_size),
            memory_pool: MemoryPool::new(&config)?,
            config,
            reserved_key_prefixes: ["bl00m_f1lt3r/".as_bytes(), "c0unt_m1n_$k3tch/".as_bytes(), "hyp3r_l0g_l0g/".as_bytes(), "$1m_ha$h/".as_bytes(), "t0k3n_buck3t/".as_bytes()]
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
    pub fn insert(&mut self, key: &[u8], value: &[u8], check_reserved_prefixes: bool) -> Result<(), Box<dyn Error>> {
        if check_reserved_prefixes {
            for forbidden_key_prefix in self.reserved_key_prefixes {
                if key.starts_with(forbidden_key_prefix) {
                    return Err(Box::new(ReservedKeyError {
                        message: format!("Cannot insert key with system reserved prefix {}.", String::from_utf8_lossy(forbidden_key_prefix))
                    }));
                }
            }
        }

        self.memory_pool.insert(key, value, TimeStamp::Now)?;
        Ok(())
    }

    /// Removes the value that's associated to the given key.
    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        self.memory_pool.delete(key, TimeStamp::Now)
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

    /// Gets the value Bloom filter associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to retrieve the Bloom Filter.
    ///
    /// # Returns
    ///
    /// An `Option` containing the value associated with the key, or `None` if the key is not present.
    pub fn bloom_filter_get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.reserved_get(key, 0)
    }

    /// Inserts a new value into the Bloom Filter associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to insert the value.
    /// * `value` - The value to insert into the Bloom Filter.
    ///
    /// # Returns
    ///
    /// Result indicating success or an error wrapped in a `Box<dyn Error>`.
    pub fn bloom_filter_insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 0);
        let bf_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let mut bloom_filter = match BloomFilter::deserialize(&bf_bytes) {
            Ok(filter) => filter,
            Err(err) => {
                return Err(Box::new(err));
            }
        };

        bloom_filter.add(value);

        self.insert(&combined_key, bloom_filter.serialize().as_ref(), false)
    }

    /// Checks if the given value is likely present in the Bloom Filter associated with the key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to check the presence of the value.
    /// * `value` - The value to check for in the Bloom Filter.
    ///
    /// # Returns
    ///
    /// Result indicating whether the value is likely present (`Ok(true)`) or not present (`Ok(false)`),
    /// or an error wrapped in a `Box<dyn Error>`.
    pub fn bloom_filter_contains(&mut self, key: &[u8], value: &[u8]) -> Result<bool, Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 0);
        let bf_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let bloom_filter = match BloomFilter::deserialize(&bf_bytes) {
            Ok(filter) => filter,
            Err(err) => {
                return Err(Box::new(err));
            }
        };

        Ok(bloom_filter.contains(value))
    }

    /// Gets the Count-Min Sketch associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to retrieve the Count-Min Sketch.
    ///
    /// # Returns
    ///
    /// An `Option` containing the value associated with the key, or `None` if the key is not present.
    pub fn count_min_sketch_get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.reserved_get(key, 1)
    }

    /// Increases the count associated with the given value in the Count-Min Sketch.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to increase the count of the value.
    /// * `value` - The value for which to increment the count.
    ///
    /// # Returns
    ///
    /// Result indicating success or an error wrapped in a `Box<dyn Error>`.
    pub fn count_min_sketch_increase_count(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 1);
        let cms_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let mut count_min_sketch = CMSketch::deserialize(&cms_bytes);

        count_min_sketch.increase_count(value);

        self.insert(&combined_key, count_min_sketch.serialize().as_ref(), false)
    }

    /// Gets the count associated with the given value in the Count-Min Sketch.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to retrieve the count of the value.
    /// * `value` - The value for which to get the count.
    ///
    /// # Returns
    ///
    /// Result containing the count associated with the value or an error wrapped in a `Box<dyn Error>`.
    pub fn count_min_sketch_get_count(&mut self, key: &[u8], value: &[u8]) -> Result<u64, Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 1);
        let cms_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let count_min_sketch = CMSketch::deserialize(&cms_bytes);

        Ok(count_min_sketch.get_count(&value))
    }


    /// Gets the value associated with the given key in the HyperLogLog.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to retrieve the HyperLogLog.
    ///
    /// # Returns
    ///
    /// An `Option` containing the value associated with the key, or `None` if the key is not present.
    pub fn hyperloglog_get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.reserved_get(key, 2)
    }

    /// Increases the count associated with the given value in the HyperLogLog.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to increase the count of the value.
    /// * `value` - The value for which to increment the count.
    ///
    /// # Returns
    ///
    /// Result indicating success or an error wrapped in a `Box<dyn Error>`.
    pub fn hyperloglog_increase_count(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 2);
        let hll_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let mut hyperloglog = HLL::deserialize(&hll_bytes);

        hyperloglog.add_to_count(&value);

        self.insert(&combined_key, hyperloglog.serialize().as_ref(), false)
    }

    /// Gets the count estimated by the HyperLogLog.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to retrieve the estimated count.
    ///
    /// # Returns
    ///
    /// Result containing the estimated count or an error wrapped in a `Box<dyn Error>`.
    pub fn hyperloglog_get_count(&mut self, key: &[u8]) -> Result<u64, Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 2);
        let hll_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let hyperloglog = HLL::deserialize(&hll_bytes);

        Ok(hyperloglog.get_count())
    }

    /// Calculates the Hamming distance between two strings using the SimHash algorithm.
    ///
    /// # Arguments
    ///
    /// * `data1` - The first string for Hamming distance calculation.
    /// * `data2` - The second string for Hamming distance calculation.
    ///
    /// # Returns
    ///
    /// The Hamming distance between the two strings.
    pub fn sim_hash_calculate_hamming_distance(&self, data1: &str, data2: &str) -> u8 {
        hamming_distance(data1, data2)
    }

    /// Gets the value associated with the given key for a probabilistic data structure of the given index.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to retrieve the value.
    /// * `index` - The index representing the type of probabilistic data structure.
    ///
    /// # Returns
    ///
    /// An `Option` containing the value associated with the key, or `None` if the key is not present.
    fn reserved_get(&self, key: &[u8], index: usize) -> Option<Box<[u8]>> {
        let combined_key = self.get_combined_key(key, index);

        if let Some(value) = self.get(&combined_key) {
            return Some(value);
        }

        None
    }

    /// Combines the given key with a reserved key prefix based on the specified index.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to be combined.
    /// * `index` - The index representing the type of probabilistic data structure.
    ///
    /// # Returns
    ///
    /// The combined key, which includes a reserved key prefix and the provided key.
    fn get_combined_key(&self, key: &[u8], index: usize) -> Vec<u8> {
        let mut combined_key = self.reserved_key_prefixes[index].to_vec();
        combined_key.extend_from_slice(key);

        combined_key
    }

    /// Gets the serialized bytes of a probabilistic data structure associated with the combined key.
    ///
    /// # Arguments
    ///
    /// * `combined_key` - The combined key for which to retrieve the probabilistic data structure bytes.
    ///
    /// # Returns
    ///
    /// Result containing the serialized bytes of the probabilistic data structure or an error
    /// wrapped in a `Box<dyn Error>`.
    fn get_probabilistic_ds_bytes(&self, combined_key: &[u8]) -> Result<Box<[u8]>, Box<dyn Error>> {
        match self.get(combined_key) {
            Some(bytes) => Ok(bytes),
            None => {
                Err(Box::new(ProbabilisticTypeError {
                    message: format!("Failed to get probabilistic structure with combined key {}.", String::from_utf8_lossy(combined_key))
                }))
            }
        }
    }
}