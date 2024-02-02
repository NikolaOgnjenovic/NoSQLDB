use std::io;
use std::io::Write;
use std::path::Path;
use db_config::{DBConfig,MemoryTableType, CompactionAlgorithmType};
use clearscreen;
use enum_iterator::Sequence;
use inquire::{Confirm, Select};
use colored::Colorize;
use crate::impl_menu;
use crate::menus::{get_input_with_range, UserMenu};

#[derive(Sequence)]
enum CustomizeMenu {
    Back,
    BloomFilterProbability,
    BloomFilterCap,
    SkipListMaxLevel,
    HyperloglogPrecision,
    WriteAheadLogDir,
    WriteAheadLogNumOfLogs,
    WriteAheadLogSize,
    BTreeOrder,
    MemoryTableCapacity,
    MemoryTableType,
    MemoryTablePoolNum,
    IndexDensity,
    SummaryDensity,
    SSTableSingleFile,
    SSTableDir,
    LsmMaxLevel,
    LsmMaxPerLevel,
    CompactionEnabled,
    CompactionAlgorithmType,
    CacheMaxSize,
    TokenBucketNum,
    TokenBucketInterval,
    UseCompression,
    CompressionDictionaryPath,
}

impl_menu!(
    CustomizeMenu, "CUSTOMIZE CONFIGURATION",
    CustomizeMenu::Back, "Back".yellow().italic(),
    CustomizeMenu::BloomFilterProbability, "Bloom Filter Probability".blink(),
    CustomizeMenu::BloomFilterCap, "Bloom Filter Capacity".blink(),
    CustomizeMenu::SkipListMaxLevel, "Skip List Max Level".blink(),
    CustomizeMenu::HyperloglogPrecision, "Hyperloglog Precision".blink(),
    CustomizeMenu::WriteAheadLogDir, "Write Ahead Log Directory".blink(),
    CustomizeMenu::WriteAheadLogNumOfLogs, "Write Ahead Log Number of Logs".blink(),
    CustomizeMenu::WriteAheadLogSize, "Write Ahead Log Size".blink(),
    CustomizeMenu::BTreeOrder, "BTree Order".blink(),
    CustomizeMenu::MemoryTableCapacity, "Memory Table Capacity".blink(),
    CustomizeMenu::MemoryTableType, "Memory Table Type".blink(),
    CustomizeMenu::MemoryTablePoolNum, "Memory Table Pool Number".blink(),
    CustomizeMenu::SummaryDensity, "Summary Density".blink(),
    CustomizeMenu::SSTableSingleFile, "SSTable Single File".blink(),
    CustomizeMenu::SSTableDir, "SSTable Directory".blink(),
    CustomizeMenu::LsmMaxLevel, "LSM Max Level".blink(),
    CustomizeMenu::LsmMaxPerLevel, "LSM Max Per Level".blink(),
    CustomizeMenu::CompactionEnabled, "Compaction Enabled".blink(),
    CustomizeMenu::CompactionAlgorithmType, "Compaction Algorithm Type".blink(),
    CustomizeMenu::CacheMaxSize, "Cache Max Size".blink(),
    CustomizeMenu::TokenBucketNum, "Token Bucket Number".blink(),
    CustomizeMenu::TokenBucketInterval, "Token Bucket Interval".blink(),
    CustomizeMenu::UseCompression, "Use Compression".blink(),
    CustomizeMenu::CompressionDictionaryPath, "Compression Dictionary Path".blink()
);

pub fn customize_menu(dbconfig: &mut DBConfig) {
    clearscreen::clear().expect("Failed to clear screen.");
    loop {
        match CustomizeMenu::get_menu() {
            CustomizeMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                return
            }
            CustomizeMenu::BloomFilterProbability => {
                 clearscreen::clear().expect("Failed to clear screen.");
                loop {
                    print!("Enter new bloom filter probability: (0-1): ");
                    io::stdout().flush().unwrap();

                    let mut input = String::new();
                    io::stdin().read_line(&mut input).unwrap();

                    match input.trim().parse::<f64>() {
                        Ok(value) if value >= 0.0 && value <= 1.0 => {
                            dbconfig.bloom_filter_probability = value;
                            println!("Bloom filter probability changed to {}", value);
                            break;
                        }
                        _ => println!("Invalid input. Please enter a valid number between 0 and 1."),
                    }
                }
            }
            CustomizeMenu::BloomFilterCap => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range(
                    "Enter new bloom filter capacity:",
                    10,
                    3000000,
                );
                dbconfig.bloom_filter_cap = new_value;
                println!("Bloom filter capacity changed to {}", new_value);
                continue
            }
            CustomizeMenu::SkipListMaxLevel => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range(
                    "Enter new skip list max level:",
                    5,
                    15,
                );
                dbconfig.skip_list_max_level = new_value;
                println!("Skip list max level changed to {}", new_value);
            }
            CustomizeMenu::HyperloglogPrecision => {
                clearscreen::clear().expect("Failed to clear screen.");
                loop {
                    println!("Enter new hyperloglog precision: (1-100)");
                    print!("> ");
                    io::stdout().flush().unwrap();

                    let mut input = String::new();
                    io::stdin().read_line(&mut input).unwrap();

                    match input.trim().parse::<u32>() {
                        Ok(value) if value >= 1 && value <= 100 => {
                            dbconfig.hyperloglog_precision = value;
                            println!("HLL precision changed to {}", value);
                            break;
                        }
                        _ => println!("Invalid input. Please enter a valid positive integer between 1 and 100."),
                    }
                }
            }
            CustomizeMenu::WriteAheadLogDir => {
                clearscreen::clear().expect("Failed to clear screen.");
                println!("Enter new Write Ahead Log directory:");

                let mut input_path = String::new();
                io::stdin().read_line(&mut input_path).unwrap();
                let input_path = input_path.trim();

                if Path::new(input_path).exists() {
                    dbconfig.write_ahead_log_dir = input_path.to_string();
                    println!("Write Ahead Log directory changed to {}", input_path);
                } else {
                    println!("Error: Path does not exist.");
                }
            }
            CustomizeMenu::WriteAheadLogNumOfLogs => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new WAL num of logs: ", 500, 3000);
                dbconfig.write_ahead_log_num_of_logs = new_value;
                println!(" WAL num of logs changed to {}", new_value);
            }
            CustomizeMenu::WriteAheadLogSize => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new WAL size: ", 500000, 2000000);
                dbconfig.write_ahead_log_size = new_value;
                println!(" WAL size changed to {}", new_value);
            }
            CustomizeMenu::BTreeOrder => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new b tree order number: ", 5, 15);
                dbconfig.b_tree_order = new_value;
                println!("B tree order changed to {}", new_value);
            }
            CustomizeMenu::MemoryTableCapacity => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new Memory table capacity: ", 500, 3000);
                dbconfig.memory_table_capacity = new_value;
                println!("Mem table capacity changed to {}", new_value);
            }
            CustomizeMenu::MemoryTableType => {
                clearscreen::clear().expect("Failed to clear screen.");
                let options = vec![
                    "SkipList".to_string(),
                    "HashMap".to_string(),
                    "BTree".to_string(),
                ];

                let choice = Select::new("Select memory table type:", options)
                    .prompt();
                let choice_str = choice.as_ref().map(|s| s.as_str()).unwrap_or("Invalid Selection");
                let memory_table_type = match choice_str {
                    "SkipList" => MemoryTableType::SkipList,
                    "HashMap" => MemoryTableType::HashMap,
                    "BTree" => MemoryTableType::BTree,
                    _ => {
                        println!("Invalid selection");
                        continue
                    }
                };
                println!("Set memory table type to {}", memory_table_type);
                dbconfig.memory_table_type = memory_table_type;
            }
            CustomizeMenu::MemoryTablePoolNum => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new Memory table pool number: ", 5, 15);
                dbconfig.memory_table_pool_num = new_value;
                println!("Mem table pool num changed to {}", new_value);
            }
            CustomizeMenu::IndexDensity => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new index density: ", 1, 1000);
                dbconfig.index_density = new_value;
                println!("Index density changed to {}", new_value);
            }
            CustomizeMenu::SummaryDensity => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new summery density: ", 1, 1000);
                dbconfig.summary_density = new_value;
                println!("Summery density changed to {}", new_value);
            }
            CustomizeMenu::SSTableSingleFile => {
                clearscreen::clear().expect("Failed to clear screen.");
                let is_enabled = Confirm::new("Enable sstable single file?")
                    .with_default(false).prompt();

                match is_enabled {
                    Ok(true) => {
                        dbconfig.sstable_single_file = true;
                        println!("SSTable single file feature enabled.")
                    }
                    Ok(false) => {
                        dbconfig.sstable_single_file = false;
                        println!("SSTable single file feature disabled.")
                    }
                    Err(_) => println!("Error, try again."),
                }
            }
            CustomizeMenu::SSTableDir => {
                clearscreen::clear().expect("Failed to clear screen.");
                println!("Enter new SSTable directory:");

                let mut input_path = String::new();
                io::stdin().read_line(&mut input_path).unwrap();
                let input_path = input_path.trim();

                if Path::new(input_path).exists() {
                    dbconfig.sstable_dir = input_path.to_string();
                    println!("SSTable directory changed to {}", input_path);
                } else {
                    println!("Error: Path does not exist.");
                }
            }
            CustomizeMenu::LsmMaxLevel => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new LSM max level: ", 0, 10);
                dbconfig.lsm_max_level = new_value;
                println!("LSM max level changed to {}", new_value);
            }
            CustomizeMenu::LsmMaxPerLevel => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new LSM max per level: ", 0, 10);
                dbconfig.lsm_max_per_level = new_value;
                println!("LSM max per level changed to {}", new_value);
            }
            CustomizeMenu::CompactionEnabled => {
                clearscreen::clear().expect("Failed to clear screen.");
                let is_enabled = Confirm::new("Enable compaction?")
                    .with_default(false)
                    .prompt();

                match is_enabled {
                    Ok(true) => println!("Compaction enabled."),
                    Ok(false) => println!("Compaction disabled."),
                    Err(_) => println!("Error, try again."),
                }
            }
            CustomizeMenu::CompactionAlgorithmType => {
                clearscreen::clear().expect("Failed to clear screen.");
                let compaction_algorithm_choices = vec![
                    "SizeTiered".to_string(),
                    "Leveled".to_string(),
                ];

                let choice = Select::new("Select compaction algorithm type:", compaction_algorithm_choices)
                    .prompt();

                let choice_str = choice.as_ref().map(|s| s.as_str()).unwrap_or("Invalid Selection");
                let compaction_algorithm_type= match choice_str {
                    "SizeTiered" => CompactionAlgorithmType::SizeTiered,
                    "Leveled" => CompactionAlgorithmType::Leveled,
                    _ => {
                        println!("Invalid selection");
                        continue
                    }
                };

                dbconfig.compaction_algorithm_type = compaction_algorithm_type;
                println!("Set compaction algorithm type to {}", compaction_algorithm_type);
            }
            CustomizeMenu::CacheMaxSize => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new Cache max size: ", 0, 10);
                dbconfig.cache_max_size = new_value;
                println!("Cache max size changed to {}", new_value);
            }
            CustomizeMenu::TokenBucketNum => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new Token bucket capacity: ", 1, 30);
                dbconfig.token_bucket_capacity = new_value;
                println!("Token bucket capacity changed to {}", new_value);
            }
            CustomizeMenu::TokenBucketInterval => {
                clearscreen::clear().expect("Failed to clear screen.");
                let new_value = get_input_with_range("Enter new Token bucket refill rate (tokens per second): ", 0, 100);
                dbconfig.token_bucket_refill_rate = new_value;
                println!("Token bucket refill rate changed to {}", new_value);
            }
            CustomizeMenu::UseCompression => {
                clearscreen::clear().expect("Failed to clear screen.");
                let is_enabled = Confirm::new("Enable compression?")
                    .with_default(false)
                    .prompt();

                match is_enabled {
                    Ok(true) => println!("Compression enabled."),
                    Ok(false) => println!("Compression disabled."),
                    Err(_) => println!("Error, try again."),
                }
            }
            CustomizeMenu::CompressionDictionaryPath => {
                clearscreen::clear().expect("Failed to clear screen.");
                println!("Enter new Compression dictionary directory:");

                let mut input_path = String::new();
                io::stdin().read_line(&mut input_path).unwrap();
                let input_path = input_path.trim();

                if Path::new(input_path).exists() {
                    dbconfig.compression_dictionary_path = input_path.to_string();
                    println!("Compression dictionary directory changed to {}", input_path);
                } else {
                    println!("Error: Path does not exist.");
                }
            }
        }
    }
}