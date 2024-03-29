use bloom_filter::BloomFilter;
use count_min_sketch::CMSketch;
use db_config::DBConfig;
use hyperloglog::HLL;
use lsm::{Paginator, LSM};
use segment_elements::TimeStamp;
use simhash::hamming_distance;
use std::error::Error;
use std::fs::create_dir_all;
use std::path::Path;
use token_bucket::token_bucket::TokenBucket;

use crate::token_bucket_error::TokenBucketError;
use crate::ProbabilisticTypeError;
use crate::ReservedKeyError;

pub struct DB {
    config: DBConfig,
    lsm: LSM,
    // System reserved key prefixes for probabilistic data structures and the token bucket
    reserved_key_prefixes: [&'static [u8]; 5],
}

impl DB {
    pub fn build(config: DBConfig) -> Result<Self, Box<dyn Error>> {
        let lsm = if Path::new(&config.write_ahead_log_dir).exists() {
            match LSM::load_from_dir(&config) {
                Ok(lsm) => lsm,
                Err(e) => {
                    eprintln!("Error occurred: {}", e);
                    eprintln!("Memory pool wasn't reconstructed.");
                    LSM::new(&config)?
                }
            }
        } else {
            LSM::new(&config)?
        };

        create_dir_all(&config.sstable_dir)?;
        create_dir_all(&config.write_ahead_log_dir)?;

        Ok(DB {
            lsm,
            config,
            reserved_key_prefixes: [
                "bl00m_f1lt3r/".as_bytes(),
                "c0unt_m1n_$k3tch/".as_bytes(),
                "hyp3r_l0g_l0g/".as_bytes(),
                "$1m_ha$h/".as_bytes(),
                "t0k3n_buck3t/state".as_bytes(),
            ],
        })
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        self.system_insert(key, value, true)
    }

    /// Inserts a new key value pair into the system.
    fn system_insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        check_reserved_prefixes: bool,
    ) -> Result<(), Box<dyn Error>> {
        if key != "t0k3n_buck3t/state".as_bytes() {
            if self.token_bucket_take()? {
                if check_reserved_prefixes {
                    for forbidden_key_prefix in self.reserved_key_prefixes {
                        if key.starts_with(forbidden_key_prefix) {
                            return Err(Box::new(ReservedKeyError {
                                message: format!(
                                    "Cannot insert key with system reserved prefix {}.",
                                    String::from_utf8_lossy(forbidden_key_prefix)
                                ),
                            }));
                        }
                    }
                }
            } else {
                return Err(From::from(TokenBucketError));
            }
        }

        self.lsm.insert(key, value, TimeStamp::Now)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), Box<dyn Error>> {
        self.system_delete(key, true)
    }

    /// Removes the value that's associated to the given key.
    fn system_delete(
        &mut self,
        key: &[u8],
        check_reserved_prefixes: bool,
    ) -> Result<(), Box<dyn Error>> {
        if self.token_bucket_take()? {
            if check_reserved_prefixes {
                for forbidden_key_prefix in &self.reserved_key_prefixes {
                    if key.starts_with(forbidden_key_prefix) {
                        return Err(Box::new(ReservedKeyError {
                            message: format!(
                                "Cannot insert key with system reserved prefix {}.",
                                String::from_utf8_lossy(forbidden_key_prefix)
                            ),
                        }));
                    }
                }
            }
            self.lsm.delete(key, TimeStamp::Now)?;
            Ok(())
        } else {
            Err(From::from(TokenBucketError))
        }
    }

    /// Retrieves the data that is associated to a given key.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Box<dyn Error>> {
        self.system_get(key, true)
    }

    fn system_get(
        &mut self,
        key: &[u8],
        check_reserved_prefixes: bool,
    ) -> Result<Option<Box<[u8]>>, Box<dyn Error>> {
        if key == "t0k3n_buck3t/state".as_bytes() {
            self.lsm.get(key).map_err(|err| From::from(err))
        } else {
            match self.token_bucket_take() {
                Ok(true) => {
                    if check_reserved_prefixes {
                        for forbidden_key_prefix in &self.reserved_key_prefixes {
                            if key.starts_with(forbidden_key_prefix) {
                                return Err(ReservedKeyError {
                                    message: format!(
                                        "Cannot insert key with system reserved prefix {}.",
                                        String::from_utf8_lossy(forbidden_key_prefix)
                                    ),
                                }
                                .into());
                            }
                        }
                    }
                    self.lsm.get(key).map_err(|err| From::from(err))
                }
                _ => Err(From::from(TokenBucketError)),
            }
        }
    }

    /// Should be called before the program exit to gracefully finish all memory tables writes,
    /// SStable merges and compactions.
    pub fn shut_down(self) {
        self.lsm.finalize();
    }

    /// Creates a Bloom filter for the specified key, then returns its serialized representation.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to identify the probabilistic structure.
    /// * `probability` - Optional probability parameter for creating a new Bloom filter.
    ///           If `None`, it will be loaded from the database configuration.
    /// * `cap` - Optional capacity parameter for creating a new Bloom filter.
    ///           If `None`, it will be loaded from the database configuration.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized representation of the Bloom filter or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// Returns a Boxed Error if there is an issue creating or serializing the Bloom filter.
    pub fn bloom_filter_create(
        &mut self,
        key: &[u8],
        probability: Option<f64>,
        cap: Option<usize>,
    ) -> Result<Box<[u8]>, Box<dyn Error>> {
        let probability = probability.unwrap_or(self.config.bloom_filter_probability);
        let cap = cap.unwrap_or(self.config.bloom_filter_cap);
        let bloom_filter = BloomFilter::new(probability, cap);

        let combined_key = self.get_combined_key(key, 0);
        self.system_insert(&combined_key, bloom_filter.serialize().as_ref(), false)?;

        self.get_probabilistic_ds_bytes(&combined_key)
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
    pub fn bloom_filter_get(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Box<dyn Error>> {
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

        self.system_insert(&combined_key, bloom_filter.serialize().as_ref(), false)
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
    pub fn bloom_filter_contains(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, Box<dyn Error>> {
        return if self.token_bucket_take()? {
            let combined_key = self.get_combined_key(key, 0);
            let bf_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

            let bloom_filter = match BloomFilter::deserialize(&bf_bytes) {
                Ok(filter) => filter,
                Err(err) => return Err(Box::new(err)),
            };

            Ok(bloom_filter.contains(value))
        } else {
            Err(From::from(TokenBucketError))
        };
    }

    /// Creates a count-min sketch for the specified key, then returns its serialized representation.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to identify the probabilistic structure.
    /// * `probability` - Optional probability parameter for creating a new count-min sketch.
    ///           If `None`, it will be loaded from the database configuration.
    /// * `tolerance` - Optional tolerance parameter for creating a new count-min sketch.
    ///           If `None`, it will be loaded from the database configuration.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized representation of the count-min sketch or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// Returns a Boxed Error if there is an issue creating or serializing the count-min sketch.
    pub fn count_min_sketch_create(
        &mut self,
        key: &[u8],
        probability: Option<f64>,
        tolerance: Option<f64>,
    ) -> Result<Box<[u8]>, Box<dyn Error>> {
        let probability = probability.unwrap_or(self.config.count_min_sketch_probability);
        let tolerance = tolerance.unwrap_or(self.config.count_min_sketch_tolerance);
        let count_min_sketch = CMSketch::new(probability, tolerance);

        let combined_key = self.get_combined_key(key, 1);
        self.system_insert(&combined_key, count_min_sketch.serialize().as_ref(), false)?;

        self.get_probabilistic_ds_bytes(&combined_key)
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
    pub fn count_min_sketch_get(
        &mut self,
        key: &[u8],
    ) -> Result<Option<Box<[u8]>>, Box<dyn Error>> {
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
    pub fn count_min_sketch_increase_count(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 1);
        let cms_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let mut count_min_sketch = CMSketch::deserialize(&cms_bytes);

        count_min_sketch.increase_count(value);

        self.system_insert(&combined_key, count_min_sketch.serialize().as_ref(), false)
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
    pub fn count_min_sketch_get_count(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<u64, Box<dyn Error>> {
        if self.token_bucket_take()? {
            let combined_key = self.get_combined_key(key, 1);
            let cms_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

            let count_min_sketch = CMSketch::deserialize(&cms_bytes);

            Ok(count_min_sketch.get_count(&value))
        } else {
            Err(From::from(TokenBucketError))
        }
    }

    /// Creates a hyperloglog for the specified key, then returns its serialized representation.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to identify the probabilistic structure.
    /// * `precision` - Optional precision parameter for creating a new hyperloglog.
    ///           If `None`, it will be loaded from the database configuration.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized representation of the hyperloglog or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// Returns a Boxed Error if there is an issue creating or serializing the hyperloglog.
    pub fn hyperloglog_create(
        &mut self,
        key: &[u8],
        precision: Option<u32>,
    ) -> Result<Box<[u8]>, Box<dyn Error>> {
        let precision = precision.unwrap_or(self.config.hyperloglog_precision);
        let hyperloglog = HLL::new(precision);

        let combined_key = self.get_combined_key(key, 2);
        self.system_insert(&combined_key, hyperloglog.serialize().as_ref(), false)?;

        self.get_probabilistic_ds_bytes(&combined_key)
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
    pub fn hyperloglog_get(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Box<dyn Error>> {
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
    pub fn hyperloglog_increase_count(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, 2);
        let hll_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

        let mut hyperloglog = HLL::deserialize(&hll_bytes);

        hyperloglog.add_to_count(&value);

        self.system_insert(&combined_key, hyperloglog.serialize().as_ref(), false)
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
        if self.token_bucket_take()? {
            let combined_key = self.get_combined_key(key, 2);
            let hll_bytes = self.get_probabilistic_ds_bytes(combined_key.as_slice())?;

            let hyperloglog = HLL::deserialize(&hll_bytes);

            Ok(hyperloglog.get_count())
        } else {
            Err(From::from(TokenBucketError))
        }
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
    fn reserved_get(
        &mut self,
        key: &[u8],
        index: usize,
    ) -> Result<Option<Box<[u8]>>, Box<dyn Error>> {
        let combined_key = self.get_combined_key(key, index);

        if let Some(value) = self.system_get(&combined_key, false)? {
            return Ok(Some(value));
        }

        Ok(None)
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
    fn get_probabilistic_ds_bytes(
        &mut self,
        combined_key: &[u8],
    ) -> Result<Box<[u8]>, Box<dyn Error>> {
        match self.system_get(combined_key, false)? {
            Some(bytes) => Ok(bytes),
            None => Err(Box::new(ProbabilisticTypeError {
                message: format!(
                    "Failed to get probabilistic structure with combined key {}.",
                    String::from_utf8_lossy(combined_key)
                ),
            })),
        }
    }

    /// Takes tokens from the token bucket, updating its state.
    ///
    /// This function controls the rate of operations by allowing or
    /// disallowing based on token availability.
    ///
    /// # Returns
    ///
    /// A result indicating whether tokens were successfully taken (`Ok(true)`)
    /// or if an error occurred (`Err`).
    pub fn token_bucket_take(&mut self) -> Result<bool, Box<dyn Error>> {
        let mut token_bucket = match self.system_get("t0k3n_buck3t/state".as_bytes(), false)? {
            Some(bytes) => TokenBucket::deserialize(&bytes),
            None => TokenBucket::new(
                self.config.token_bucket_capacity,
                self.config.token_bucket_refill_rate,
            ),
        };

        let token_taken = token_bucket.take(1);

        self.system_insert(
            "t0k3n_buck3t/state".as_bytes(),
            token_bucket.serialize().as_ref(),
            false,
        )?;

        Ok(token_taken)
    }

    pub fn get_paginator(&mut self) -> Paginator {
        Paginator::new(&mut self.lsm)
    }
}
