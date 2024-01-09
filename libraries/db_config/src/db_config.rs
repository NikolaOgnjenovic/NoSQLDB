use std::io::{BufReader, Error, ErrorKind, Write};
use serde::{Deserialize, Serialize};
use std::fs::File;


/// Options for the implementation of memory table
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MemoryTableType {
    SkipList,
    HashMap,  // todo, dodato na osnovu specifikacije?
    BTree,
}


// todo, novo gradivo, nije još implementirano, dodato na osnovu specifikacije
/// Options for the compression algorithm type
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum CompressionAlgorithmType {
    SizeTiered,
    Leveled,
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
    pub write_ahead_log_num_of_logs: usize,  // todo, dodato na osnovu specifikacije?
    pub write_ahead_log_size: usize,         // todo, dodato na osnovu specifikacije?
    pub b_tree_order: usize,
    pub memory_table_capacity: usize,
    pub memory_table_type: MemoryTableType,
    pub memory_table_pool_num: usize,
    pub summary_density: usize,
    pub sstable_single_file: bool,

    // todo, novo gradivo, nije još implementirano,
    // todo, dodato ono šta je poznato na osnovu specifikacije
    pub lsm_max_level: usize,
    pub compression_enabled: bool,
    pub compression_algorithm_type: CompressionAlgorithmType,
    pub cache_max_size: usize,
    pub token_bucket_num: usize,
    pub token_bucket_interval: usize,
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
            write_ahead_log_num_of_logs: 1000,  // todo, dodato na osnovu specifikacije?
            write_ahead_log_size: 1048576,      // todo, dodato na osnovu specifikacije?
            b_tree_order: 10,
            memory_table_capacity: 1000,
            memory_table_type: MemoryTableType::BTree,
            memory_table_pool_num: 10,
            summary_density: 10,

            // todo, novo gradivo, nije još implementirano,
            // todo, dodato ono šta je poznato na osnovu specifikacije
            sstable_single_file: false,
            lsm_max_level: 0,
            compression_enabled: false,
            compression_algorithm_type: CompressionAlgorithmType::SizeTiered,
            cache_max_size: 0,
            token_bucket_num: 0,
            token_bucket_interval: 0,
        }
    }
}


impl DBConfig {
    /// Creates new instance of configuration with default values
    pub fn new() -> Self {
        DBConfig { ..Default::default() }
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