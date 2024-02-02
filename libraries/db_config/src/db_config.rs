use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Write};

/// Options for the implementation of memory table
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MemoryTableType {
    SkipList,
    HashMap,
    BTree,
}

/// Helper function to display MemoryTableType
impl fmt::Display for MemoryTableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryTableType::SkipList => write!(f, "SkipList"),
            MemoryTableType::HashMap => write!(f, "HashMap"),
            MemoryTableType::BTree => write!(f, "BTree"),
        }
    }
}

/// Options for the compaction algorithm type
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub enum CompactionAlgorithmType {
    SizeTiered,
    Leveled,
}

/// Helper function to display MemoryTableType
impl fmt::Display for CompactionAlgorithmType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactionAlgorithmType::SizeTiered => write!(f, "SizeTiered"),
            CompactionAlgorithmType::Leveled => write!(f, "Leveled"),
        }
    }
}

/// Configuration parameters
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(default)]
pub struct DBConfig {
    pub bloom_filter_probability: f64,
    pub bloom_filter_cap: usize,
    pub skip_list_max_level: usize,
    pub hyperloglog_precision: u32,
    pub write_ahead_log_dir: String,
    pub write_ahead_log_num_of_logs: usize,
    pub write_ahead_log_size: usize,
    pub b_tree_order: usize,
    pub memory_table_capacity: usize,
    pub memory_table_type: MemoryTableType,
    pub memory_table_pool_num: usize,
    pub summary_density: usize,
    pub index_density: usize,
    pub sstable_single_file: bool,
    pub sstable_dir: String,
    pub lsm_max_level: usize,
    pub lsm_max_per_level: usize,
    pub lsm_leveled_amplification: usize,
    pub compaction_enabled: bool,
    pub compaction_algorithm_type: CompactionAlgorithmType,
    pub cache_max_size: usize,
    pub token_bucket_capacity: usize,
    pub token_bucket_refill_rate: usize,
    pub use_compression: bool,
    pub use_variable_encoding: bool,
    pub compression_dictionary_path: String,
}

/// Default values for configuration parameters, used if properties are missing in JSON file
impl Default for DBConfig {
    fn default() -> DBConfig {
        DBConfig {
            bloom_filter_probability: 0.1,
            bloom_filter_cap: 1_000_000,
            skip_list_max_level: 10,
            hyperloglog_precision: 10,
            write_ahead_log_dir: "./wal/".to_string(),
            write_ahead_log_num_of_logs: 1000,
            write_ahead_log_size: 1048576,
            b_tree_order: 10,
            memory_table_capacity: 1000,
            memory_table_type: MemoryTableType::SkipList,
            memory_table_pool_num: 10,
            summary_density: 3,
            index_density: 2,
            sstable_single_file: false,
            sstable_dir: "./sstables/".to_string(),
            lsm_max_level: 3,
            lsm_max_per_level: 2,
            lsm_leveled_amplification: 10,
            compaction_enabled: true,
            compaction_algorithm_type: CompactionAlgorithmType::SizeTiered,
            cache_max_size: 1000,
            token_bucket_capacity: 5,
            token_bucket_refill_rate: 5,
            use_compression: false,
            use_variable_encoding: true,
            compression_dictionary_path: "./dictionary.bin".to_string(),
        }
    }
}

impl DBConfig {
    /// Creates new instance of configuration with default values
    pub fn new() -> Self {
        DBConfig {
            ..Default::default()
        }
    }

    /// Loads and returns configuration from JSON file from `file_path`
    pub fn load(file_path: &str) -> std::io::Result<DBConfig> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        match serde_json::from_reader(reader) {
            Ok(data) => Ok(data),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e)),
        }
    }

    /// Saves current configuration to `file_path` using JSON
    pub fn save(&self, file_path: &str) -> std::io::Result<()> {
        let json_data = serde_json::to_string_pretty(self)?;
        let mut file = File::create(file_path)?;
        file.write_all(json_data.as_bytes())
    }
}
