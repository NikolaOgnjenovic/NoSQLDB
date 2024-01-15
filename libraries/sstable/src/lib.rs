mod sstable;
mod sstable_element_type;

pub use sstable::SSTable;

#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;
    use std::path::{Path};
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

        // TODO: fix random explicit panic for btree
        for range in (10..=1000).step_by(50) {
            //for memory_type in &[MemoryTableType::SkipList] {
            for memory_type in &[MemoryTableType::BTree] {
                //for memory_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
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

        for range in (900..=1000).step_by(50) {
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

        for range in (0..=30).step_by(10) {
            // TODO: Fix key 0 not found starting from first case for btree
            //for memory_type in &[MemoryTableType::SkipList] {
            for memory_type in &[MemoryTableType::BTree] {
                //for memory_type in &[MemoryTableType::SkipList, /*MemoryTableType::HashMap,*/MemoryTableType::BTree] {
                merge_sstables(true, true, &memory_type.clone(), range, multiplier, true);
                merge_sstables(true, true, &memory_type.clone(), range, multiplier, false);

                merge_sstables(true, false, &memory_type.clone(), range, multiplier, true);
                merge_sstables(true, false, &memory_type.clone(), range, multiplier, false);

                merge_sstables(false, true, &memory_type.clone(), range, multiplier, true);
                merge_sstables(false, true, &memory_type.clone(), range, multiplier, false);

                merge_sstables(false, false, &memory_type.clone(), range, multiplier, true);
                merge_sstables(false, false, &memory_type.clone(), range, multiplier, false);
            }
        }
    }

    fn merge_sstables(sstable1_in_single_file: bool, sstable2_in_single_file: bool, memory_type: &MemoryTableType, range: i32, multiplier: i32, merged_in_single_file: bool) {
        let (temp_dir, mut inner_mem1, summary_density) = setup_test_environment(memory_type);

        // Generate data for the first SSTable
        insert_test_data(&mut inner_mem1, range, multiplier);
        //let sstable1_path = PathBuf::from(Path::new("../../data/sstable1"));
        let sstable1_path = temp_dir.path().join("sstable1");
        let mut sstable1 = create_sstable(&sstable1_path, &inner_mem1, sstable1_in_single_file);
        sstable1.flush(summary_density).expect("Failed to flush sstable");

        // Generate data * 2 for the second SSTable
        let (_, mut inner_mem2) = get_density_and_inner_mem(memory_type);
        insert_test_data(&mut inner_mem2, range, multiplier * 2);
        let sstable2_path = temp_dir.path().join("sstable2");
        //let sstable2_path = PathBuf::from(Path::new("../../data/sstable2"));

        let mut sstable2 = create_sstable(&sstable2_path, &inner_mem2, sstable2_in_single_file);
        sstable2.flush(summary_density).expect("Failed to flush sstable");

        // Define the path for the merged SSTable
        let merged_sstable_path = temp_dir.path().join("merged_sstable");
        //let merged_sstable_path = PathBuf::from(Path::new("../../data/merged"));
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
                remove_dir_all(merged_sstable_path);

                panic!("Key {:#?} not found in merged SSTable", key);
            }
        }
        remove_dir_all(merged_sstable_path);
        }
    }
