use sstable::SSTable;

mod lsm;
mod mem_pool;
mod sstable;
mod memtable;

#[cfg(test)]
mod lsm_tests {

}

/// Tests must be run in sequentially.
#[cfg(test)]
mod mem_pool_tests {
    use std::fs;
    use std::fs::{read_dir, remove_file};
    use std::path::Path;
    use db_config::DBConfig;
    use segment_elements::TimeStamp;
    use crate::mem_pool::MemoryPool;

    #[test]
    fn test_async_wal_flush() {
        let mut config = DBConfig::default();
        config.memory_table_capacity = 100_000;
        config.memory_table_pool_num = 15;

        read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log")
            .for_each(|file| remove_file(file).unwrap());

        let mut mem_pool = MemoryPool::new(&config).unwrap();

        for i in 0..1_000_000u32 {
            mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        mem_pool.join_concurrent_writes();

        let load_mem_pool = MemoryPool::load_from_dir(&config).unwrap();

        for i in (0..1_000_000u32).rev() {
            assert_eq!(load_mem_pool.get(&i.to_ne_bytes()), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_size_cap() {
        let mut config = DBConfig::default();
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_size = 50;

        read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log")
            .for_each(|file| remove_file(file).unwrap());

        let mut mem_pool = MemoryPool::new(&config).unwrap();

        for i in 0..5u128 {
            mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        mem_pool.join_concurrent_writes();

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 50);
        }

        let load_mem_pool = MemoryPool::load_from_dir(&config).unwrap();

        for i in 0..5u128 {
            assert_eq!(load_mem_pool.get(&i.to_ne_bytes()), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_num_cap() {
        let mut config = DBConfig::default();
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_num_of_logs = 3;

        read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log")
            .for_each(|file| remove_file(file).unwrap());

        let mut mem_pool = MemoryPool::new(&config).unwrap();

        for i in 0..10u128 {
            mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        mem_pool.join_concurrent_writes();

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 69 * 3);
        }

        let load_mem_pool = MemoryPool::load_from_dir(&config).unwrap();

        for i in 0..10u128 {
            assert_eq!(load_mem_pool.get(&i.to_ne_bytes()), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_size_cap2() {
        let mut config = DBConfig::default();
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_size = 10;

        read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log")
            .for_each(|file| remove_file(file).unwrap());

        let mut mem_pool = MemoryPool::new(&config).unwrap();

        for i in 0..10u128 {
            mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        mem_pool.join_concurrent_writes();

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 10);
        }

        let load_mem_pool = MemoryPool::load_from_dir(&config).unwrap();

        for i in 0..10u128 {
            assert_eq!(load_mem_pool.get(&i.to_ne_bytes()), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_num_and_size_cap() {
        let mut config = DBConfig::default();
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_num_of_logs = 1;
        config.write_ahead_log_size = 10;

        read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log")
            .for_each(|file| remove_file(file).unwrap());

        let mut mem_pool = MemoryPool::new(&config).unwrap();

        for i in 0..10u128 {
            mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        mem_pool.join_concurrent_writes();

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 200);
        }

        let load_mem_pool = MemoryPool::load_from_dir(&config).unwrap();

        for i in 0..10u128 {
            assert_eq!(load_mem_pool.get(&i.to_ne_bytes()), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn wal_delete_on_flush_test() {
        let mut config = DBConfig::default();
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 10;
        config.write_ahead_log_num_of_logs = 1000;

        read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log")
            .for_each(|file| remove_file(file).unwrap());

        let mut mem_pool = MemoryPool::new(&config).unwrap();

        for i in 0..300000u128 {
            mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        mem_pool.join_concurrent_writes();

        assert_eq!(read_dir(&config.write_ahead_log_dir).unwrap()
                       .map(|dir_entry| dir_entry.unwrap().path())
                       .filter(|file| file.file_name().unwrap() != ".keep")
                       .filter(|file| file.extension().unwrap() == "log")
                       .count(), 10);
    }
}

#[cfg(test)]
mod sstable_tests {
    use std::fs::remove_dir_all;
    use std::path::Path;
    use super::*;
    use tempfile::TempDir;
    use segment_elements::{SegmentTrait, TimeStamp};
    use db_config::{DBConfig, MemoryTableType};
    use crate::memtable::MemoryTable;

    // Helper function to get default config and inner mem of memory type
    fn get_density_and_mem_table(mem_table_type: &MemoryTableType) -> (usize, usize, MemoryTable) {
        let mut db_config = DBConfig::default();
        db_config.memory_table_type = mem_table_type.clone();
        let mem_table = MemoryTable::new(&db_config).expect("Failed to create memory table");

        (db_config.summary_density, db_config.index_density, mem_table)
    }

    // Helper function to set up the test environment
    fn setup_test_environment(mem_table_type: &MemoryTableType) -> (TempDir, MemoryTable, usize, usize) {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let (summary_density, index_density, mem_table) = get_density_and_mem_table(mem_table_type);
        (temp_dir, mem_table, summary_density, index_density)
    }

    // Helper function to insert test data into the inner memory
    fn insert_test_data(mut mem_table: &mut MemoryTable, range: i32, multiplier: i32) {
        for i in 0..range {
            let key: i32 = i;
            let value: i32 = i * multiplier;
            let timestamp = TimeStamp::Now;
            mem_table.insert(&key.to_ne_bytes(), &value.to_ne_bytes(), timestamp, false).expect("Failed to insert into mem table");
        }
    }

    #[test]
    fn test_flushing() {
        let multiplier = 2;

        for range in (1..=100).step_by(50) {
            // todo: uncomment hashmap when implemented
            for mem_table_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
                check_flushed_table(true, &mem_table_type.clone(), range, multiplier);
                check_flushed_table(false, &mem_table_type.clone(), range, multiplier);
            }
        }
    }

    fn check_flushed_table(in_single_file: bool, mem_table_type: &MemoryTableType, range: i32, multiplier: i32) {
        let (temp_dir, mut mem_table, summary_density, index_density) = setup_test_environment(mem_table_type);
        insert_test_data(&mut mem_table, range, multiplier);

        // Create an SSTable and flush
        let mut sstable = SSTable::open(&temp_dir.path(), in_single_file).expect("Failed to open SSTable");
        sstable.flush(mem_table, summary_density, index_density, &mut None).expect("Failed to flush sstable");

        //Retrieve and validate data from the SSTable
        for i in 0..range {
            let key: i32 = i;
            let expected_value: i32 = i * multiplier;

            // Retrieve value from the SSTable
            if let Some(entry) = sstable.get(&key.to_ne_bytes(), index_density, &mut None) {
                // Get the value using the get_value method
                let actual_value_bytes: Box<[u8]> = entry.get_value();

                // Convert bytes to i32 (assuming i32 is 4 bytes)
                let mut actual_value_bytes_array: [u8; 4] = Default::default();
                actual_value_bytes_array.copy_from_slice(&actual_value_bytes[..4]);
                let actual_value: i32 = i32::from_ne_bytes(actual_value_bytes_array);

                // Assert that the values match
                assert_eq!(actual_value, expected_value);
            } else {
                // If the key is not found, fail the test
                panic!("{i}");
            }
        }
    }

    #[test]
    fn test_merkle() {
        let multiplier = 2;

        for range in (1..=100).step_by(50) {
            for mem_table_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
                check_merkle_tree(true, &mem_table_type.clone(), range, multiplier);
                check_merkle_tree(false, &mem_table_type.clone(), range, multiplier);
            }
        }
    }

    fn check_merkle_tree(in_single_file: bool, mem_table_type: &MemoryTableType, range: i32, multiplier: i32) {
        let (temp_dir, mut mem_table, summary_density, index_density) = setup_test_environment(mem_table_type);
        insert_test_data(&mut mem_table, range, multiplier);

        // Create an SSTable from the MemoryPool's inner_mem
        let mut sstable = SSTable::open(&temp_dir.path(), in_single_file).expect("Failed to open SSTable");
        sstable.flush(mem_table, summary_density, index_density, &mut None).expect("Failed to flush sstable");

        // Get the merkle tree from the SSTable
        let merkle_tree = sstable.get_merkle().expect("Failed to get merkle tree");

        // Check merkle tree against itself, expecting no differences
        let different_chunks_indices = sstable.check_merkle(&merkle_tree).expect("Failed to check merkle tree");
        assert!(different_chunks_indices.is_empty());
    }

    #[test]
    fn test_merge_sstables() {
        let multiplier = 2;

        for range in (1..=10).step_by(10) {
            // todo: uncomment hashmap when implemented
            for mem_table_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, multiplier, true);
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, multiplier, false);

                merge_sstables(vec![true, false], &mem_table_type.clone(), range, multiplier, true);
                merge_sstables(vec![true, false], &mem_table_type.clone(), range, multiplier, false);

                merge_sstables(vec![false, true], &mem_table_type.clone(), range, multiplier, true);
                merge_sstables(vec![false, true], &mem_table_type.clone(), range, multiplier, false);

                merge_sstables(vec![false, false], &mem_table_type.clone(), range, multiplier, true);
                merge_sstables(vec![false, false], &mem_table_type.clone(), range, multiplier, false);
            }
        }
    }

    fn merge_sstables(in_single_file: Vec<bool>, mem_table_type: &MemoryTableType, range: i32, multiplier: i32, merged_in_single_file: bool) {

        //contains paths to all sstables
        let mut sstable_paths = Vec::new();

        let (temp_dir, _, summary_density, index_density) = setup_test_environment(mem_table_type);

        //generate data for all sstables nad insert paths to sstable_paths
        for i in 0..in_single_file.len() {
            let (_, _, mut mem_table) = get_density_and_mem_table(mem_table_type);
            insert_test_data(&mut mem_table, range, multiplier * (i + 1) as i32);

            let sstable_path = temp_dir.path().join("sstable".to_string() + (i + 1).to_string().as_str());
            let mut sstable = SSTable::open(&sstable_path, in_single_file[i]).expect("Failed to open SSTable");

            sstable.flush(mem_table, summary_density, index_density, &mut None).expect("Failed to flush sstable");
            sstable_paths.push(sstable_path);
        }

        //convert pathbuf to path
        let sstable_paths:Vec<_> = sstable_paths.iter().map(|path_buf| path_buf.as_path()).collect();

        // Define the path for the merged SSTable
        let merged_sstable_path = temp_dir.path().join("merged_sstable");
        // Define the database configuration
        let db_config = DBConfig::default();

        // Merge the two SSTables
        SSTable::merge(sstable_paths.clone(), in_single_file, merged_sstable_path.as_path(), merged_in_single_file, summary_density, index_density, &mut None)
            .expect("Failed to merge SSTables");

        verify_merged_sstable(&merged_sstable_path, mem_table_type, index_density, range, multiplier, merged_in_single_file);
    }

    // Helper function to verify that the merged SSTable contains the correct data
    fn verify_merged_sstable(merged_sstable_path: &Path, mem_table_type: &MemoryTableType, index_density: usize, range: i32, multiplier: i32, merged_in_single_file: bool) {
        // Open an SSTable from the merged SSTable path
        let mut merged_sstable = SSTable::open(merged_sstable_path, merged_in_single_file)
            .expect("Failed to create merged SSTable");

        // Retrieve and validate data from the merged SSTable
        for i in 0..range {
            let key: i32 = i;
            let expected_value: i32 = i * multiplier * 2;

            // Retrieve value from the merged SSTable
            if let Some(entry) = merged_sstable.get(&key.to_ne_bytes(), index_density, &mut None) {
                // Get the value using the get_value method
                let actual_value_bytes: Box<[u8]> = entry.get_value();

                // Convert bytes to i32 (assuming i32 is 4 bytes)
                let mut actual_value_bytes_array: [u8; 4] = Default::default();
                actual_value_bytes_array.copy_from_slice(&actual_value_bytes[..4]);
                let actual_value: i32 = i32::from_ne_bytes(actual_value_bytes_array);

                // Assert that the values match
                assert_eq!(actual_value, expected_value);
            } else {
                // If the key is not found, fail the test
                remove_dir_all(merged_sstable_path).expect("Failed to remove all dirs");

                panic!("Key {:#?} not found in merged SSTable", key);
            }
        }
        remove_dir_all(merged_sstable_path).expect("Failed to remove all dirs");
    }
}
