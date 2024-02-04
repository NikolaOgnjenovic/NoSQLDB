use std::fs::{read_dir, remove_dir_all, remove_file};
use db_config::DBConfig;
use db_config::MemoryTableType::BTree;
use NoSQLDB::DB;

fn prepare_dirs(dbconfig: &DBConfig) {
    match read_dir(&dbconfig.write_ahead_log_dir) {
        Ok(dir) => {
            dir.map(|dir_entry| dir_entry.unwrap().path())
                .for_each(|file| remove_file(file).unwrap())
        }
        Err(_) => ()
    }

    match read_dir(&dbconfig.sstable_dir) {
        Ok(dir) => {
            dir.map(|dir_entry| dir_entry.unwrap().path())
                .for_each(|dir| remove_dir_all(dir).unwrap_or(()))
        }
        Err(_) => ()
    }

    remove_file(&dbconfig.compression_dictionary_path).ok();
}

#[test]
fn test_compressed_size_vs_uncompressed_100_diff_keys() {
    let mut db_config = DBConfig::default();
    db_config.memory_table_type = BTree;
    db_config.summary_density = 10;
    db_config.index_density = 50;
    db_config.lsm_max_level = 10;
    db_config.use_compression = false;
    db_config.use_variable_encoding = false;
    db_config.token_bucket_capacity = 9999999999;
    db_config.token_bucket_refill_rate = 9999999999;
    db_config.memory_table_capacity = 100;
    db_config.memory_table_pool_num = 10;
    db_config.compression_dictionary_path = "sstables/test_compressed_size_vs_uncompressed_100_diff_keys/dictionary_c.bin".to_string();
    db_config.sstable_dir += "test_compressed_size_vs_uncompressed_100_diff_keys/uncompressed/";
    db_config.write_ahead_log_dir += "test_compressed_size_vs_uncompressed_100_diff_keys/uncompressed/";

    prepare_dirs(&db_config);

    let mut db_uncompressed = DB::build(db_config.clone()).unwrap();

    for i in 0..100_000 {
        let key_num = i % 100;
        // works in other tests, no need to test again here
        let key = format!("test_key_{:0>10}{}", 0, key_num);
        let value = format!("test_value_{}", key_num);
        db_uncompressed.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    db_config.compression_dictionary_path = "sstables/test_compressed_size_vs_uncompressed_100_diff_keys/dictionary_u.bin".to_string();
    db_config.sstable_dir = "sstables/test_compressed_size_vs_uncompressed_100_diff_keys/compressed/".to_string();
    db_config.write_ahead_log_dir = "wal/test_compressed_size_vs_uncompressed_100_diff_keys/uncompressed/".to_string();
    db_config.use_variable_encoding = true;
    db_config.use_compression = true;

    prepare_dirs(&db_config);

    let mut db_compressed = DB::build(db_config.clone()).unwrap();

    for i in 0..100_000 {
        let key_num = i % 100;
        let key = format!("test_key_{:0>10}{}", 0, key_num);
        let value = format!("test_value_{}", key_num);
        db_compressed.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    for i in 0..100 {
        let key_num = i % 100;
        let key = format!("test_key_{:0>10}{}", 0, key_num);
        let value = format!("test_value_{}", key_num);

        let get_op = match db_compressed.get(key.as_bytes()).unwrap() {
            Some(val) => val,
            None => panic!("Get doesn't work")
        };

        assert_eq!(
            value.as_bytes(),
            get_op.as_ref()
        );
    }
}

#[test]
fn test_compressed_size_vs_uncompressed_50000_diff_keys() {
    let mut db_config = DBConfig::default();
    db_config.memory_table_type = BTree;
    db_config.summary_density = 10;
    db_config.index_density = 50;
    db_config.lsm_max_level = 5;
    db_config.use_compression = false;
    db_config.use_variable_encoding = false;
    db_config.token_bucket_capacity = 9999999999;
    db_config.token_bucket_refill_rate = 9999999999;
    db_config.memory_table_capacity = 100;
    db_config.memory_table_pool_num = 100;
    db_config.compression_dictionary_path = "sstables/test_compressed_size_vs_uncompressed_50000_diff_keys/dictionary_u.bin".to_string();
    db_config.sstable_dir += "test_compressed_size_vs_uncompressed_50000_diff_keys/uncompressed/";
    db_config.write_ahead_log_dir += "wal_u/";

    prepare_dirs(&db_config);

    let mut db_uncompressed = DB::build(db_config.clone()).unwrap();

    for i in 0..100_000 {
        let key_num = i % 50_000;
        // works in other tests, no need to test again here
        let key = format!("test_key_{:0>10}{}", 0, key_num);
        let value = format!("test_value_{}", key_num);
        db_uncompressed.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    db_config.compression_dictionary_path = "sstables/test_compressed_size_vs_uncompressed_50000_diff_keys/dictionary_c.bin".to_string();
    db_config.sstable_dir = "sstables/test_compressed_size_vs_uncompressed_50000_diff_keys/compressed/".to_string();
    db_config.write_ahead_log_dir = "wal/wal_c/".to_string();
    db_config.use_variable_encoding = true;
    db_config.use_compression = true;

    prepare_dirs(&db_config);

    let mut db_compressed = DB::build(db_config.clone()).unwrap();

    for i in 0..100_000 {
        let key_num = i % 50_000;
        let key = format!("test_key_{:0>10}{}", 0, key_num);
        let value = format!("test_value_{}", key_num);
        db_compressed.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    for i in 0..100_000 {
        let key_num = i % 50_000;
        let key = format!("test_key_{:0>10}{}", 0, key_num);
        let value = format!("test_value_{}", key_num);

        let get_op = match db_compressed.get(key.as_bytes()).unwrap() {
            Some(val) => val,
            None => panic!("Get doesn't work")
        };

        println!("{i}");

        assert_eq!(
            value.as_bytes(),
            get_op.as_ref()
        );
    }
}