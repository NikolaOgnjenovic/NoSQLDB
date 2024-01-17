mod memtable;
mod record_iterator;
mod crc_error;
mod mem_pool;

pub use mem_pool::MemoryPool;

/// Tests must be run in sequentially.
#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::{read_dir, remove_file};
    use std::path::Path;
    use db_config::DBConfig;
    use segment_elements::TimeStamp;
    use crate::MemoryPool;

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