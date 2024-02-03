use sstable::SSTable;

mod lsm;
mod mem_pool;
mod sstable;
mod memtable;
mod paginator;
pub use lsm::LSM;

#[cfg(test)]
mod mem_pool_tests {
    use db_config::DBConfig;
    use segment_elements::TimeStamp;
    use crate::mem_pool::MemoryPool;

    #[test]
    fn test_string_input() {
        let mut db_config = DBConfig::new();
        db_config.memory_table_capacity = 100;
        db_config.memory_table_pool_num = 3000;

        let mut mem_pool = MemoryPool::new(&db_config).unwrap();

        let base_key = "test_key";
        let base_value = "test_value";

        let timestamp_custom = TimeStamp::Custom(123);

        for i in 0..100_000 {
            let key = format!("{}{}", base_key, i.to_string());
            let value = format!("{}{}", base_value, i.to_string());

            mem_pool.insert(key.as_bytes(), value.as_bytes(), timestamp_custom);
        }

        for i in 0..100_000 {
            let key = format!("{}{}", base_key, i.to_string());
            let value = format!("{}{}", base_value, i.to_string());

            let get_op = match mem_pool.get(key.as_bytes()) {
                Some(val) => val.get_value(),
                None => panic!("Get doesn't work")
            };

            println!("{i}");

            assert_eq!(
                value.as_bytes(),
                &*get_op
            );
        }
    }
}

#[cfg(test)]
mod lsm_tests {
    use std::{fs, io};
    use std::fs::{read_dir, remove_dir_all, remove_file};
    use super::*;
    use segment_elements::TimeStamp;
    use db_config::{CompactionAlgorithmType, DBConfig};
    use crate::lsm::{LSM, ScanType};

    fn prepare_dirs(dbconfig: &DBConfig) {
        match read_dir(&dbconfig.write_ahead_log_dir) {
            Ok(dir) => {
                dir.map(|dir_entry| dir_entry.unwrap().path())
                    .filter(|file| file.file_name().unwrap() != ".keep")
                    .filter(|file| file.extension().unwrap() == "log" || file.extension().unwrap() == "num")
                    .for_each(|file| remove_file(file).unwrap())
            }
            Err(_) => ()
        }

        match read_dir(&dbconfig.sstable_dir) {
            Ok(dir) => {
                dir.map(|dir_entry| dir_entry.unwrap().path())
                    .filter(|dir| dir.file_name().unwrap() != ".keep")
                    .for_each(|dir| remove_dir_all(dir).unwrap_or(()))
            },
            Err(_) => ()
        }
    }

    #[test]
    fn test_flushing() -> io::Result<()> {
        let mut db_config = DBConfig::default();
        db_config.memory_table_pool_num = 100;
        db_config.memory_table_capacity = 30;
        db_config.lsm_max_per_level = 3;
        db_config.lsm_max_level = 6;
        db_config.lsm_leveled_amplification = 5;
        db_config.sstable_single_file = true;
        db_config.compaction_algorithm_type = CompactionAlgorithmType::Leveled;
        let mut lsm = LSM::new(&db_config).expect("No such file or directory");
        for i in 0..8000usize {
            lsm.insert(&i.to_ne_bytes(), &i.to_ne_bytes(), TimeStamp::Now)?;
        }
        //fs::create_dir_all(&db_config.sstable_dir)?;
        list_files_and_folders(db_config.sstable_dir)?;
        Ok(())
    }

    fn list_files_and_folders(folder_path: String) -> io::Result<()> {
        let entries = fs::read_dir(folder_path)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_string_lossy();

            if path.is_file() {
                println!("File: {}", file_name);
            } else if path.is_dir() {
                println!("Folder: {}", file_name);
            } else {
                println!("Unknown: {}", file_name);
            }
        }
        Ok(())
    }

    #[test]
    fn test_scans() -> io::Result<()> {
        let mut db_config = DBConfig::default();
        db_config.memory_table_pool_num = 10;
        db_config.memory_table_capacity = 500;
        db_config.lsm_max_per_level = 5;
        db_config.sstable_single_file = false;
        db_config.compaction_algorithm_type = CompactionAlgorithmType::SizeTiered;

        prepare_dirs(&db_config);

        let mut lsm = LSM::new(&db_config).unwrap();
        for i in 0..20_000usize {
            if i % 2 == 0 {
                lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now)?;
            } else {
                lsm.delete(&i.to_ne_bytes(), TimeStamp::Now)?;
            }
            //lsm.insert(&i.to_ne_bytes(), &i.to_ne_bytes(), TimeStamp::Now)?;
        }

        let mut num = 0;
        //Range
        let mut lsm_iter = lsm.iter(Some(&80usize.to_ne_bytes()), Some(&160usize.to_ne_bytes()), None, ScanType::RangeScan)?;
        while let Some(inner) = lsm_iter.next() {
            if let Some(entry) = inner {
                println!("{:?}", entry.0);
                println!("{:?}", entry.1);
                num += 1;
            }
        }

        //assert_eq!(41, num);

        println!();
        println!();
        println!();

        // Prefix
        // let mut lsm_iter = lsm.iter(None, None, Some(&80usize.to_ne_bytes()), ScanType::PrefixScan)?;
        // while let Some(inner) = lsm_iter.next() {
        //     if let Some((entry)) = inner {
        //         println!("{:?}", entry.0);
        //         println!("{:?}", entry.1);
        //     }
        // }

        Ok(())
    }
}

#[cfg(test)]
mod paginator_tests {
    use std::fs::{create_dir_all, remove_dir_all};
    use db_config::{CompactionAlgorithmType, DBConfig};
    use segment_elements::TimeStamp;
    use crate::LSM;
    use crate::paginator::Paginator;

    #[test]
    fn test_prefix_scan() {
        let mut db_config = DBConfig::default();
        db_config.memory_table_pool_num = 2;
        db_config.memory_table_capacity = 10;
        db_config.lsm_max_per_level = 4;
        db_config.sstable_single_file = true;
        db_config.compaction_algorithm_type = CompactionAlgorithmType::SizeTiered;
        let mut lsm = LSM::new(&db_config).unwrap();

        let base_string = "AB";
        let base_bytes = base_string.as_bytes();
        let page_len = 10;
        let page_count = 3;
        for i in 0..page_count * page_len + 1 {
            let new_bytes = ((i as u64) + 1).to_ne_bytes();
            let combined_bytes: Vec<u8> = base_bytes.iter().cloned().chain(new_bytes.iter().cloned()).collect();

            //println!("Inserting key to LSM: {:#?}", combined_bytes);
            let time_stamp = TimeStamp::Now;
            lsm.insert(&combined_bytes, &combined_bytes, time_stamp).expect("Failed to insert into lsm");
        }

        println!("{:?}", lsm.get(&[65, 66, 20, 0, 0, 0, 0, 0]).expect(""));
        println!("{:?}", lsm.get(&[65, 66, 22, 0, 0, 0, 0, 0]).expect(""));
        println!("{:?}", lsm.get(&[65, 66, 24, 0, 0, 0, 0, 0]).expect(""));
        println!("{:?}", lsm.get(&[65, 66, 26, 0, 0, 0, 0, 0]).expect(""));
        println!("{:?}", lsm.get(&[65, 66, 29, 0, 0, 0, 0, 0]).expect(""));
        println!("{:?}", lsm.get(&[65, 66, 31, 0, 0, 0, 0, 0]).expect(""));
        // let mut iter = lsm.iter(Some(&[0]), Some(&[255]), None, RangeScan).expect("Failed to insert into lsm");
        // while let Some(entry) = iter.next() {
        //     println!("{:?}", entry.0);
        //     println!("{:?}", entry.1);
        // }
        // let mut iter = lsm.iter(None, None, Some(&[65, 66]), PrefixScan).expect("Failed to insert into lsm");
        // while let Some(entry) = iter.next() {
        //     println!("{:?}", entry.0);
        //     println!("{:?}", entry.1);
        // }

        let mut paginator = Paginator::new(&lsm);
        // Test pages
        for page_number in 0..page_count {
            let result_page = paginator.prefix_scan(base_bytes, page_number, page_len).expect("Failed to get pagination result");
            assert_eq!(result_page.len(), page_len);

            for (i, (key, _)) in result_page.iter().enumerate() {
                assert!(key.starts_with(base_bytes));

                let expected_value = ((page_number * page_len + i) as u64 + 1).to_ne_bytes();
                let expected_bytes: Vec<u8> = base_bytes.iter().cloned().chain(expected_value.iter().cloned()).collect();
                //println!("Expected: {:#?}, key: {:#?}", expected_bytes.into_boxed_slice(), key.clone());
                assert_eq!(expected_bytes.clone().into_boxed_slice(), key.clone());
                //println!("Works for {:#?}", expected_bytes);
            }
        }
    }

    #[test]
    fn test_range_scan_whole_range() {
        let mut db_config = DBConfig::default();
        db_config.memory_table_pool_num = 3;
        db_config.memory_table_capacity = 1000;
        db_config.lsm_max_per_level = 4;
        db_config.sstable_single_file = true;
        db_config.compaction_algorithm_type = CompactionAlgorithmType::SizeTiered;
        create_dir_all(db_config.sstable_dir.to_string()).expect("Failed to create sstable dirs");
        let mut lsm = LSM::new(&db_config).unwrap();

        // Insert elements into LSM
        let min_key: usize = 0;
        let max_key: usize = 1000;
        for i in min_key..=max_key {
            let i_bytes = i.to_ne_bytes();
            lsm.insert(&i_bytes, &i_bytes, TimeStamp::Now).expect("Failed to insert into lsm");
        }

        // Set min & max range for paginator range scan
        let mut paginator = Paginator::new(&lsm);
        let min_range: u64 = 50;
        let max_range: u64 = 100;
        let min_range_bytes = min_range.to_ne_bytes();
        let max_range_bytes = (max_range + 1).to_ne_bytes(); // Add 1 to include the upper bound

        // Test range scan from 50 to 100
        let result_page = paginator.range_scan(&min_range_bytes, &max_range_bytes, 0, (max_range - min_range + 1) as usize)
            .expect("Failed to get pagination result");

        assert_eq!(result_page.len(), (max_range - min_range + 1) as usize);

        for (i, (key, _)) in result_page.iter().enumerate() {
            assert!(key.as_ref() >= min_range_bytes.as_ref() && key.as_ref() <= max_range_bytes.as_ref());
            println!("Works for {:#?}", key);
        }

        remove_dir_all(db_config.sstable_dir).expect("Failed to remove sstable dirs");
    }

    #[test]
    fn test_range_scan_iter() {
        let mut db_config = DBConfig::default();
        db_config.memory_table_pool_num = 3;
        db_config.memory_table_capacity = 50;
        db_config.lsm_max_per_level = 4;
        db_config.sstable_single_file = true;
        db_config.compaction_algorithm_type = CompactionAlgorithmType::SizeTiered;
        db_config.use_variable_encoding = false;
        create_dir_all(db_config.sstable_dir.to_string()).expect("Failed to create sstable dirs");
        let mut lsm = LSM::new(&db_config).unwrap();

        // Insert elements into LSM
        let min_key: usize = 0;
        let max_key: usize = 300;
        for i in min_key..=max_key {
            let i_bytes = i.to_ne_bytes();
            lsm.insert(&i_bytes, &i_bytes, TimeStamp::Now).expect("Failed to insert into lsm");
        }

        // Set min & max range for paginator range scan
        // Wrap the Paginator in an Rc<RefCell<Paginator>>
        let mut paginator = Paginator::new(&lsm);
        let min_range: usize = 0;
        let max_range: usize = 26;
        let min_range_bytes = min_range.to_ne_bytes();
        let max_range_bytes = (max_range + 1).to_ne_bytes(); // Add 1 to include the upper bound

        // Assert that each key is 0 to 25 (in order) when calling range_iterate_next
        for i in 0..25usize {
            let i_bytes = i.to_ne_bytes();
            let result = paginator
                .range_iterate_next(&min_range_bytes, &max_range_bytes)
                .expect("Failed to iterate to next entry")
                .expect("Failed to get memory entry")
                .0
                .clone();
            let i_bytes = i.to_ne_bytes();
            assert_eq!(result.as_ref(), i_bytes.as_ref());
        }

        // Assert that going backwards retrieves 24..0
        for i in (0..25usize).rev() {
            let i_bytes = i.to_ne_bytes();
            let result = paginator
                .iterate_prev()
                .expect("Failed to iterate to previous entry")
                .expect("Failed to get memory entry")
                .0
                .clone();

            assert_eq!(&*result, i_bytes.as_ref());
        }

        paginator.iterate_stop();

        // Asser that each key is 0 to 25 (in order) when calling range_iterate_next
        for i in 0..25usize {
            let i_bytes = i.to_ne_bytes();
            let result = paginator
                .range_iterate_next(&min_range_bytes, &max_range_bytes)
                .expect("Failed to iterate to next entry")
                .expect("Failed to get memory entry")
                .0
                .clone();
            let i_bytes = i.to_ne_bytes();
            assert_eq!(&*result, i_bytes.as_ref());
        }
    }
}
      
mod lsm_wal_tests {
    use std::fs;
    use std::fs::{read_dir, remove_dir_all, remove_file};
    use std::path::Path;
    use db_config::DBConfig;
    use segment_elements::TimeStamp;
    use crate::LSM;

    fn prepare_dirs(dbconfig: &DBConfig) {
        match read_dir(&dbconfig.write_ahead_log_dir) {
            Ok(dir) => {
                dir.map(|dir_entry| dir_entry.unwrap().path())
                    .filter(|file| file.file_name().unwrap() != ".keep")
                    .filter(|file| file.extension().unwrap() == "log" || file.extension().unwrap() == "num")
                    .for_each(|file| remove_file(file).unwrap())
            }
            Err(_) => ()
        }

        match read_dir(&dbconfig.sstable_dir) {
            Ok(dir) => {
                dir.map(|dir_entry| dir_entry.unwrap().path())
                    .filter(|dir| dir.file_name().unwrap() != ".keep")
                    .for_each(|dir| remove_dir_all(dir).unwrap_or(()))
            },
            Err(_) => ()
        }
    }

    #[test]
    fn test_wal_reconstruction() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_reconstruction/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_reconstruction/";
        config.memory_table_capacity = 1000;
        config.write_ahead_log_num_of_logs = 1000;
        config.memory_table_pool_num = 20;

        prepare_dirs(&config);

        let mut lsm = LSM::new(&config).unwrap();

        for i in 0..20_000u32 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..20_000u32 {
            println!("{i}");
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }
  
    fn test_wal_size_cap() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_size_cap/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_size_cap/";
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_size = 50;

        prepare_dirs(&config);

         let mut lsm = LSM::new(&config).unwrap();

        for i in 0..5u128 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 50);
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..5u128 {
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_num_cap() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_num_cap/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_num_cap/";
        config.memory_table_capacity = 100;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_num_of_logs = 1;

        prepare_dirs(&config);

         let mut lsm = LSM::new(&config).unwrap();

        for i in 0..100u128 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 69 * 3);
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..100u128 {
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_size_cap2() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_size_cap2/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_size_cap2/";
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_size = 10;

        prepare_dirs(&config);

         let mut lsm = LSM::new(&config).unwrap();

        for i in 0..10u128 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 10);
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..10u128 {
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_num_and_size_cap() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_num_and_size_cap/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_num_and_size_cap/";
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 3;
        config.write_ahead_log_num_of_logs = 1;
        config.write_ahead_log_size = 10;

        prepare_dirs(&config);

         let mut lsm = LSM::new(&config).unwrap();

        for i in 0..10u128 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        for file in read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log") {
            assert!(fs::metadata(Path::new(&file)).unwrap().len() <= 200);
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..10u128 {
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_one_file_correct_reload() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_one_file_correct_reload/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_one_file_correct_reload/";
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 10;
        config.lsm_max_level = 100;
        config.write_ahead_log_num_of_logs = 300001;

        prepare_dirs(&config);

         let mut lsm = LSM::new(&config).unwrap();

        for i in 0..300000u128 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 300000u128 - 84..300000u128 {
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_delete_on_flush() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_delete_on_flush/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_delete_on_flush/";
        config.memory_table_capacity = 10;
        config.memory_table_pool_num = 10;
        config.write_ahead_log_size = 500;

        prepare_dirs(&config);

        let mut lsm = LSM::new(&config).unwrap();

        for i in 0..30000u128 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        assert!(read_dir(&config.write_ahead_log_dir).unwrap()
                       .map(|dir_entry| dir_entry.unwrap().path())
                       .filter(|file| file.file_name().unwrap() != ".keep")
                       .filter(|file| file.extension().unwrap() == "log")
                       .count() < 25);
    }

    #[test]
    fn test_wal_big_input() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_big_input/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_big_input/";
        config.memory_table_capacity = 13;
        config.memory_table_pool_num = 10;
        config.write_ahead_log_size = 5;

        prepare_dirs(&config);

         let mut lsm = LSM::new(&config).unwrap();

        let big_data = "老";

        for i in 0..100 {
            let big_input = big_data.repeat(i);
            lsm.insert(big_input.as_bytes(), &2_u128.to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..100 {
            let big_input = big_data.repeat(i);
            assert_eq!(load_lsm.get(&big_input.as_bytes()).unwrap(), Some(Box::from(2_u128.to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_small_input() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_small_input/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_small_input/";
        config.memory_table_capacity = 5;
        config.memory_table_pool_num = 1;
        config.write_ahead_log_num_of_logs = 100000;

        prepare_dirs(&config);

         let mut lsm = LSM::new(&config).unwrap();

        for i in 0..53u8 {
            println!("{i}");
            lsm.insert(&i.to_ne_bytes(), &1_u128.to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..53u8 {
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from(1_u128.to_ne_bytes())));
        }
    }

    #[test]
    fn test_wal_removal_after_config_change() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_removal_after_config_change/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_removal_after_config_change/";
        config.memory_table_capacity = 1000;
        config.memory_table_pool_num = 10;
        config.write_ahead_log_num_of_logs = 800;
        config.lsm_max_level = 6;
        config.write_ahead_log_size = 10000;

        prepare_dirs(&config);

        let mut lsm = LSM::new(&config).unwrap();

        for i in 0..3000u128 {
            lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        let mut config_changed = config.clone();
        config_changed.memory_table_capacity = 10;

        let mut lsm_changed = LSM::load_from_dir(&config_changed).expect("IO error");

        for i in 0..3000u128 {
            println!("{i}");
            assert_eq!(lsm_changed.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
        }

        assert!(read_dir(&config.write_ahead_log_dir).unwrap()
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| file.file_name().unwrap() != ".keep")
            .filter(|file| file.extension().unwrap() == "log")
            .count() < 3);
    }

    #[test]
    fn test_wal_only_delete_reconstruction() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_wal_only_delete_reconstruction/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_wal_only_delete_reconstruction/";
        config.memory_table_capacity = 1000;
        config.write_ahead_log_num_of_logs = 1000;
        config.memory_table_pool_num = 20;

        prepare_dirs(&config);

        let mut lsm = LSM::new(&config).unwrap();

        for i in 0..20_000u32 {
            lsm.delete(&i.to_ne_bytes(), TimeStamp::Now).expect("IO error");
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..20_000u32 {
            println!("{i}");
            assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), None);
        }
    }

    #[test]
    fn test_insert_delete_mixed_reconstruction() {
        let mut config = DBConfig::default();
        config.sstable_dir = "sstable_wal_test/".to_string();
        config.sstable_dir += "test_insert_delete_mixed_reconstruction/";
        config.write_ahead_log_dir = "wal_wal_test/".to_string();
        config.write_ahead_log_dir += "test_insert_delete_mixed_reconstruction/";
        config.memory_table_capacity = 1000;
        config.write_ahead_log_num_of_logs = 1000;
        config.memory_table_pool_num = 20;

        prepare_dirs(&config);

        let mut lsm = LSM::new(&config).unwrap();

        for i in 0..10000u32 {
            if i % 2 == 0 {
                lsm.delete(&i.to_ne_bytes(), TimeStamp::Now).expect("IO error");
                assert_eq!(lsm.get(&i.to_ne_bytes()).unwrap(), None);
            } else {
                println!("{i}");
                if i == 1601 {
                    println!();
                }
                lsm.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now).expect("IO error");
                assert_eq!(lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
            }
        }

        let mut load_lsm = LSM::load_from_dir(&config).expect("IO error");

        for i in 0..10000u32 {
            println!("{i}");
            if i % 2 == 0 {
                assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), None);
            } else {
                assert_eq!(load_lsm.get(&i.to_ne_bytes()).unwrap(), Some(Box::from((i * 2).to_ne_bytes())));
            }
        }
    }
}

#[cfg(test)]
mod sstable_tests {
    use std::fs::{create_dir_all, remove_dir_all};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use compression::CompressionDictionary;
    use segment_elements::TimeStamp;
    use db_config::{DBConfig, MemoryTableType};
    use crate::memtable::MemoryTable;
    use crate::sstable::SSTable;

    // Helper function to get default config and inner mem of memory type
    fn get_density_and_mem_table(mem_table_type: &MemoryTableType, use_compression: bool) -> (usize, usize, MemoryTable) {
        let mut db_config = DBConfig::default();
        db_config.use_compression = use_compression;
        db_config.memory_table_type = mem_table_type.clone();
        let mem_table = MemoryTable::new(&db_config).expect("Failed to create memory table");

        (db_config.summary_density, db_config.index_density, mem_table)
    }

    // Helper function to set up the test environment
    fn setup_test_environment(mem_table_type: &MemoryTableType, use_compresion: bool) -> (TempDir, MemoryTable, usize, usize) {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let (summary_density, index_density, mem_table) = get_density_and_mem_table(mem_table_type, use_compresion);
        (temp_dir, mem_table, summary_density, index_density)
    }

    // Helper function to insert test data into the inner memory
    fn insert_test_data(mem_table: &mut MemoryTable, range: i32) {
        for i in 0..range {
            let key = format!("test_key_{}", i);
            let value = format!("test_value_{}", i);
            let timestamp = TimeStamp::Now;
            mem_table.insert(key.as_bytes(), value.as_bytes(), timestamp);
        }
    }

    fn get_compression_dict(compression_dict_dir: &TempDir, use_compression: bool) -> Option<CompressionDictionary> {
        if use_compression {
            let dir_path = compression_dict_dir.path().join("compression_dict");
            let compression_dict_filename = dir_path.to_str().expect("Failed to unwrap compression dict filename");
            create_dir_all(&compression_dict_dir).expect("Failed to create compression dict dirs");

            Some(CompressionDictionary::load(compression_dict_filename).expect("Faileed to load compression dictionary"))
        } else {
            None
        }
    }

    #[test]
    fn test_flushing_uncompressed_no_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_flushed_table(true, &mem_table_type.clone(), range, false, false);
                check_flushed_table(false, &mem_table_type.clone(), range, false, false);
            }
        }
    }

    #[test]
    fn test_flushing_uncompressed_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_flushed_table(true, &mem_table_type.clone(), range, true, false);
                check_flushed_table(false, &mem_table_type.clone(), range, true, false);
            }
        }
    }

    #[test]
    fn test_flushing_compressed_no_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_flushed_table(true, &mem_table_type.clone(), range, false, true);
                check_flushed_table(false, &mem_table_type.clone(), range, false, true);
            }
        }
    }

    #[test]
    fn test_flushing_compressed_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_flushed_table(true, &mem_table_type.clone(), range, true, true);
                check_flushed_table(false, &mem_table_type.clone(), range, true, true);
            }
        }
    }

    fn check_flushed_table(in_single_file: bool, mem_table_type: &MemoryTableType, range: i32, use_variable_encoding: bool, use_compression: bool) {
        let (temp_dir, mut mem_table, summary_density, index_density) = setup_test_environment(mem_table_type, use_compression);
        insert_test_data(&mut mem_table, range);

        // Create an SSTable and flush
        let mut sstable = SSTable::open((&temp_dir.path()).to_path_buf(), in_single_file).expect("Failed to open SSTable");

        // Create compression dict if required
        let compression_dict_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let mut compression_dictionary = get_compression_dict(&compression_dict_dir, use_compression);

        sstable.flush(mem_table, summary_density, index_density, None, &mut compression_dictionary, use_variable_encoding).expect("Failed to flush sstable");

        // Retrieve and validate data from the SSTable
        for i in 0..range {
            let key = format!("test_key_{}", i);
            let expected_value = format!("test_value_{}", i);

            // Retrieve value from the SSTable
            if let Some(entry) = sstable.get(key.as_bytes(), index_density, &mut None, use_variable_encoding) {
                // Get the value using the get_value method
                let actual_value_bytes: Box<[u8]> = entry.get_value();

                // Assert that the values match
                assert_eq!(actual_value_bytes, expected_value.as_bytes().into());
            } else {
                // If the key is not found, fail the test
                if use_compression {
                    remove_dir_all(&compression_dict_dir).expect("Failed to remove compression dictionary dirs");
                }
                panic!("{i}");
            }
        }

        remove_dir_all(&compression_dict_dir).expect("Failed to remove compression dictionary dirs");
    }

    #[test]
    fn test_merkle_uncompressed_no_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_merkle_tree(true, &mem_table_type.clone(), range, false, false);
                check_merkle_tree(false, &mem_table_type.clone(), range, false, false);
            }
        }
    }

    #[test]
    fn test_merkle_uncompressed_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_merkle_tree(true, &mem_table_type.clone(), range, true, false);
                check_merkle_tree(false, &mem_table_type.clone(), range, true, false);
            }
        }
    }

    #[test]
    fn test_merkle_compressed_no_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_merkle_tree(true, &mem_table_type.clone(), range, false, true);
                check_merkle_tree(false, &mem_table_type.clone(), range, false, true);
            }
        }
    }

    #[test]
    fn test_merkle_compressed_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                check_merkle_tree(true, &mem_table_type.clone(), range, true, true);
                check_merkle_tree(false, &mem_table_type.clone(), range, true, true);
            }
        }
    }

    fn check_merkle_tree(in_single_file: bool, mem_table_type: &MemoryTableType, range: i32, use_variable_encoding: bool, use_compression: bool) {
        let (temp_dir, mut mem_table, summary_density, index_density) = setup_test_environment(mem_table_type, use_compression);
        insert_test_data(&mut mem_table, range);

        // Create an SSTable from the MemoryPool's inner_mem
        let mut sstable = SSTable::open((&temp_dir.path()).to_path_buf(), in_single_file).expect("Failed to open SSTable");

        // Create compression dict if required
        let compression_dict_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let mut compression_dictionary = get_compression_dict(&compression_dict_dir, use_compression);

        sstable.flush(mem_table, summary_density, index_density, None, &mut compression_dictionary, use_variable_encoding).expect("Failed to flush sstable");

        // Get the merkle tree from the SSTable
        let merkle_tree = sstable.get_merkle().expect("Failed to get merkle tree");

        // Check merkle tree against itself, expecting no differences
        let different_chunks_indices = sstable.check_merkle(&merkle_tree).expect("Failed to check merkle tree");
        remove_dir_all(compression_dict_dir).expect("Failed to remove compression dict dirs");
        assert!(different_chunks_indices.is_empty());
    }

    #[test]
    fn test_merge_sstables_uncompressed_no_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, true, false, false);
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, false, false, false);

                merge_sstables(vec![true, false], &mem_table_type.clone(), range, true, false, false);
                merge_sstables(vec![true, false], &mem_table_type.clone(), range, false, false, false);

                merge_sstables(vec![false, true], &mem_table_type.clone(), range, true, false, false);
                merge_sstables(vec![false, true], &mem_table_type.clone(), range, false, false, false);

                merge_sstables(vec![false, false], &mem_table_type.clone(), range, true, false, false);
                merge_sstables(vec![false, false], &mem_table_type.clone(), range, false, false, false);
            }
        }
    }

    #[test]
    fn test_merge_sstables_uncompressed_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, true, true, false);
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, false, true, false);

                merge_sstables(vec![true, false], &mem_table_type.clone(), range, true, true, false);
                merge_sstables(vec![true, false], &mem_table_type.clone(), range, false, true, false);

                merge_sstables(vec![false, true], &mem_table_type.clone(), range, true, true, false);
                merge_sstables(vec![false, true], &mem_table_type.clone(), range, false, true, false);

                merge_sstables(vec![false, false], &mem_table_type.clone(), range, true, true, false);
                merge_sstables(vec![false, false], &mem_table_type.clone(), range, false, true, false);
            }
        }
    }

    #[test]
    fn test_merge_sstables_compressed_no_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, true, false, true);
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, false, false, true);

                merge_sstables(vec![true, false], &mem_table_type.clone(), range, true, false, true);
                merge_sstables(vec![true, false], &mem_table_type.clone(), range, false, false, true);

                merge_sstables(vec![false, true], &mem_table_type.clone(), range, true, false, true);
                merge_sstables(vec![false, true], &mem_table_type.clone(), range, false, false, true);

                merge_sstables(vec![false, false], &mem_table_type.clone(), range, true, false, true);
                merge_sstables(vec![false, false], &mem_table_type.clone(), range, false, false, true);
            }
        }
    }

    #[test]
    fn test_merge_sstables_compressed_variable_encoding() {
        for range in (1..=100).step_by(101) {
            for mem_table_type in &[MemoryTableType::SkipList, MemoryTableType::HashMap, MemoryTableType::BTree] {
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, true, true, true);
                merge_sstables(vec![true, true], &mem_table_type.clone(), range, false, true, true);

                merge_sstables(vec![true, false], &mem_table_type.clone(), range, true, true, true);
                merge_sstables(vec![true, false], &mem_table_type.clone(), range, false, true, true);

                merge_sstables(vec![false, true], &mem_table_type.clone(), range, true, true, true);
                merge_sstables(vec![false, true], &mem_table_type.clone(), range, false, true, true);

                merge_sstables(vec![false, false], &mem_table_type.clone(), range, true, true, true);
                merge_sstables(vec![false, false], &mem_table_type.clone(), range, false, true, true);
            }
        }
    }

    fn merge_sstables(in_single_file: Vec<bool>, mem_table_type: &MemoryTableType, range: i32, merged_in_single_file: bool, use_variable_encoding: bool, use_compression: bool) {
        // contains paths to all sstables
        let mut sstable_paths = Vec::new();

        let (temp_dir, _, summary_density, index_density) = setup_test_environment(mem_table_type, use_compression);

        // Create compression dict if required
        let compression_dict_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let mut compression_dictionary = get_compression_dict(&compression_dict_dir, use_compression);

        // generate data for all sstables nad insert paths to sstable_paths
        for i in 0..in_single_file.len() {
            let (_, _, mut mem_table) = get_density_and_mem_table(mem_table_type, use_compression);
            insert_test_data(&mut mem_table, range);

            let sstable_path = temp_dir.path().join("sstable".to_string() + (i + 1).to_string().as_str());
            let mut sstable = SSTable::open(sstable_path.to_owned(), in_single_file[i]).expect("Failed to open SSTable");

            sstable.flush(mem_table, summary_density, index_density, None, &mut compression_dictionary, use_variable_encoding).expect("Failed to flush sstable");
            sstable_paths.push(sstable_path.to_owned());
        }

        //convert pathbuf to path
        let sstable_paths: Vec<_> = sstable_paths.iter().map(|path_buf| path_buf.to_owned()).collect();

        // Define the path for the merged SSTable
        let merged_sstable_path = temp_dir.path().join("merged_sstable");

        // Merge the two SSTables
        SSTable::merge(sstable_paths, in_single_file, &merged_sstable_path.to_owned(), merged_in_single_file, summary_density, index_density, &mut None, use_variable_encoding)
            .expect("Failed to merge SSTables");

        verify_merged_sstable(&merged_sstable_path, index_density, range, merged_in_single_file, use_variable_encoding);

        remove_dir_all(&compression_dict_dir).expect("Failed to remove compression dict dirs");
    }

    // Helper function to verify that the merged SSTable contains the correct data
    fn verify_merged_sstable(merged_sstable_path: &PathBuf, index_density: usize, range: i32, merged_in_single_file: bool, use_variable_encoding: bool) {
        // Open an SSTable from the merged SSTable path
        let mut merged_sstable = SSTable::open(merged_sstable_path.to_path_buf(), merged_in_single_file)
            .expect("Failed to create merged SSTable");

        // Retrieve and validate data from the merged SSTable
        for i in 0..range {
            let key = format!("test_key_{}", i);
            let expected_value = format!("test_value_{}", i);

            // Retrieve value from the merged SSTable
            if let Some(entry) = merged_sstable.get(key.as_bytes(), index_density, &mut None, use_variable_encoding) {
                // Get the value using the get_value method
                let actual_value_bytes: Box<[u8]> = entry.get_value();

                // Assert that the values match
                assert_eq!(actual_value_bytes, expected_value.as_bytes().into());
            } else {
                // If the key is not found, fail the test
                remove_dir_all(merged_sstable_path.clone()).expect("Failed to remove all dirs");

                panic!("Key {:#?} not found in merged SSTable", key);
            }
        }
        remove_dir_all(merged_sstable_path).expect("Failed to remove all dirs");
    }
}
