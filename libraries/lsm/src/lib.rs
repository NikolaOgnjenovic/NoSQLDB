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
    use skip_list::SkipList;
    use b_tree::BTree;

    // Helper function to get default config and inner mem of memory type
    fn get_density_and_inner_mem(memory_type: &MemoryTableType) -> (usize, Box<dyn SegmentTrait + Send>) {
        let dbconfig = DBConfig::default();
        let inner_mem: Box<dyn SegmentTrait + Send> = match memory_type {
            MemoryTableType::SkipList => Box::new(SkipList::new(dbconfig.skip_list_max_level)),
            MemoryTableType::HashMap => todo!(),
            MemoryTableType::BTree => Box::new(BTree::new(dbconfig.b_tree_order).unwrap())
        };

        (dbconfig.summary_density, inner_mem)
    }

    // Helper function to set up the test environment
    fn setup_test_environment(memory_type: &MemoryTableType) -> (TempDir, Box<dyn SegmentTrait + Send>, usize) {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let (summary_density, inner_mem) = get_density_and_inner_mem(memory_type);
        (temp_dir, inner_mem, summary_density)
    }

    // Helper function to insert test data into the inner memory
    fn insert_test_data(inner_mem: &mut Box<dyn SegmentTrait + Send>, range: i32, multiplier: i32) {
        for i in 0..range {
            let key: i32 = i;
            let value: i32 = i * multiplier;
            let timestamp = TimeStamp::Now;
            inner_mem.insert(&key.to_ne_bytes(), &value.to_ne_bytes(), timestamp);
        }
    }

    // Helper function to create an SSTable from the inner memory
    fn create_sstable<'a>(base_path: &'a Path, inner_mem: &'a Box<dyn SegmentTrait + Send>, single_file: bool) -> SSTable<'a> {
        SSTable::new(base_path, inner_mem, single_file)
            .expect("Failed to create SSTable")
    }

    #[test]
    fn test_flushing() {
        let multiplier = 2;

        for range in (1..=1).step_by(50) {
            // todo: uncomment hashmap when implemented
            for memory_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
                check_flushed_table(true, &memory_type.clone(), range, multiplier);
                check_flushed_table(false, &memory_type.clone(), range, multiplier);
            }
        }
    }

    fn check_flushed_table(in_single_file: bool, memory_type: &MemoryTableType, range: i32, multiplier: i32) {
        let (temp_dir, mut inner_mem, summary_density) = setup_test_environment(memory_type);
        insert_test_data(&mut inner_mem, range, multiplier);

        // Create an SSTable from the MemoryPool's inner_mem
        let mut sstable = create_sstable(&temp_dir.path(), &inner_mem, in_single_file);
        sstable.flush(summary_density).expect("Failed to flush sstable");

        //Retrieve and validate data from the SSTable
        for i in 0..range {
            let key: i32 = i;
            let expected_value: i32 = i * multiplier;

            // Retrieve value from the SSTable
            if let Some(entry) = sstable.get(&key.to_ne_bytes()) {
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

        for range in (1..=1).step_by(50) {
            for memory_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
                check_merkle_tree(true, &memory_type.clone(), range, multiplier);
                check_merkle_tree(false, &memory_type.clone(), range, multiplier);
            }
        }
    }

    fn check_merkle_tree(in_single_file: bool, memory_type: &MemoryTableType, range: i32, multiplier: i32) {
        let (temp_dir, mut inner_mem, summary_density) = setup_test_environment(memory_type);
        insert_test_data(&mut inner_mem, range, multiplier);

        // Create an SSTable from the MemoryPool's inner_mem
        let mut sstable = create_sstable(&temp_dir.path(), &inner_mem, in_single_file);
        sstable.flush(summary_density).expect("Failed to flush sstable");

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
            for memory_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
                // merge_sstables(true, true, &memory_type.clone(), range, multiplier, true);
                // merge_sstables(true, true, &memory_type.clone(), range, multiplier, false);
                //
                // merge_sstables(true, false, &memory_type.clone(), range, multiplier, true);
                // merge_sstables(true, false, &memory_type.clone(), range, multiplier, false);
                //
                // merge_sstables(false, true, &memory_type.clone(), range, multiplier, true);
                // merge_sstables(false, true, &memory_type.clone(), range, multiplier, false);
                //
                // merge_sstables(false, false, &memory_type.clone(), range, multiplier, true);
                // merge_sstables(false, false, &memory_type.clone(), range, multiplier, false);

                merge_sstables_multiple(vec![true, true], &memory_type.clone(), range, multiplier, true);
                merge_sstables_multiple(vec![true, true], &memory_type.clone(), range, multiplier, false);

                merge_sstables_multiple(vec![true, false], &memory_type.clone(), range, multiplier, true);
                merge_sstables_multiple(vec![true, false], &memory_type.clone(), range, multiplier, false);

                merge_sstables_multiple(vec![false, true], &memory_type.clone(), range, multiplier, true);
                merge_sstables_multiple(vec![false, true], &memory_type.clone(), range, multiplier, false);

                merge_sstables_multiple(vec![false, false], &memory_type.clone(), range, multiplier, true);
                merge_sstables_multiple(vec![false, false], &memory_type.clone(), range, multiplier, false);
            }
        }
    }

    fn merge_sstables_multiple(in_single_file: Vec<bool>, memory_type: &MemoryTableType, range: i32, multiplier: i32, merged_in_single_file: bool) {

        //contains paths to all sstables
        let mut sstable_paths = Vec::new();

        let (temp_dir, mut inner_mem, summary_density) = setup_test_environment(memory_type);

        //generate data for all sstables nad insert paths to sstable_paths
        for i in 0..in_single_file.len() {
            let (_, mut inner_mem) = get_density_and_inner_mem(memory_type);
            insert_test_data(&mut inner_mem, range, multiplier * (i + 1) as i32);
            let sstable_path = temp_dir.path().join("sstable".to_string() + (i + 1).to_string().as_str());
            let mut sstable = create_sstable(&sstable_path, &inner_mem, in_single_file[i]);
            sstable.flush(summary_density).expect("Failed to flush sstable");
            sstable_paths.push(sstable_path);
        }

        //convert pathbuf to path
        let sstable_paths:Vec<_> = sstable_paths.iter().map(|path_buf| path_buf.as_path()).collect();

        // Define the path for the merged SSTable
        let merged_sstable_path = temp_dir.path().join("merged_sstable");
        // Define the database configuration
        let dbconfig = DBConfig::default();

        // Merge the two SSTables
        SSTable::merge_sstable_multiple(sstable_paths.clone() , in_single_file, merged_sstable_path.as_path(), merged_in_single_file, &dbconfig)
            .expect("Failed to merge SSTables");

        verify_merged_sstable(&merged_sstable_path, memory_type, range, multiplier, merged_in_single_file);
    }

    fn merge_sstables(sstable1_in_single_file: bool, sstable2_in_single_file: bool, memory_type: &MemoryTableType, range: i32, multiplier: i32, merged_in_single_file: bool) {
        // Generate data for the first SSTable
        let (temp_dir, mut inner_mem1, summary_density) = setup_test_environment(memory_type);
        insert_test_data(&mut inner_mem1, range, multiplier);
        let sstable1_path = temp_dir.path().join("sstable1");
        let mut sstable1 = create_sstable(&sstable1_path, &inner_mem1, sstable1_in_single_file);
        sstable1.flush(summary_density).expect("Failed to flush sstable");

        // Generate data * 2 for the second SSTable
        let (_, mut inner_mem2) = get_density_and_inner_mem(memory_type);
        insert_test_data(&mut inner_mem2, range, multiplier * 2);
        let sstable2_path = temp_dir.path().join("sstable2");
        let mut sstable2 = create_sstable(&sstable2_path, &inner_mem2, sstable2_in_single_file);
        sstable2.flush(summary_density).expect("Failed to flush sstable");

        // Define the path for the merged SSTable
        let merged_sstable_path = temp_dir.path().join("merged_sstable");
        // Define the database configuration
        let dbconfig = DBConfig::default();

        // Merge the two SSTables
        SSTable::merge_sstables(&sstable1_path, &sstable2_path, &merged_sstable_path, sstable1_in_single_file, sstable2_in_single_file, &dbconfig, merged_in_single_file)
            .expect("Failed to merge SSTables");

        // Verify the merged SSTable contains the correct data
        verify_merged_sstable(&merged_sstable_path, memory_type, range, multiplier, merged_in_single_file);
    }

    // Helper function to verify that the merged SSTable contains the correct data
    fn verify_merged_sstable(merged_sstable_path: &Path, memory_type: &MemoryTableType, range: i32, multiplier: i32, merged_in_single_file: bool) {
        let (_, inner_mem) = get_density_and_inner_mem(memory_type);

        // Create an SSTable from the merged SSTable path
        let mut merged_sstable = SSTable::new(merged_sstable_path, &inner_mem, merged_in_single_file)
            .expect("Failed to create merged SSTable");

        // Retrieve and validate data from the merged SSTable
        for i in 0..range {
            let key: i32 = i;
            let expected_value: i32 = i * multiplier * 2;

            // Retrieve value from the merged SSTable
            if let Some(entry) = merged_sstable.get(&key.to_ne_bytes()) {
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
