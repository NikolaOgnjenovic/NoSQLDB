mod sstable;

pub use sstable::SSTable;
use skip_list::SkipList;
use b_tree::BTree;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use segment_elements::{SegmentTrait, TimeStamp};
    use db_config::{DBConfig, MemoryTableType};

    #[test]
    fn test_insert_and_read_from_sstable() {
        let dbconfig = DBConfig::default();
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp directory");

        let mut inner_mem: Box<dyn SegmentTrait + Send> = match dbconfig.memory_table_type {
            MemoryTableType::SkipList => Box::new(SkipList::new(dbconfig.skip_list_max_level)),
            MemoryTableType::HashMap => unimplemented!(),
            MemoryTableType::BTree => Box::new(BTree::new(dbconfig.b_tree_order).unwrap())
        };

        // Insert some test data into the MemoryPool
        for i in 0..10 {
            let key: i32 = i;
            let value: i32 = i * 2;
            let timestamp = TimeStamp::Now;
            inner_mem.insert(&key.to_ne_bytes(), &value.to_ne_bytes(), timestamp);
        }

        // Create an SSTable from the MemoryPool's inner_mem
        let mut sstable = SSTable::new(temp_dir.path(), &inner_mem, false)
            .expect("Failed to create SSTable");
        sstable.flush(dbconfig.summary_density).expect("Failed to flush sstable");

        // Retrieve and validate data from the SSTable
        for i in 0..10 {
            let key: i32 = i;
            let expected_value: i32 = i * 2;

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
            }
        }
    }
}
