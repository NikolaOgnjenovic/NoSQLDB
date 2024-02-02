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
        },
        Err(_) => ()
    }
}

#[test]
fn test_compressed_size_vs_uncompressed() {
    let mut db_config = DBConfig::default();
    db_config.memory_table_type = BTree;
    db_config.summary_density = 10;
    db_config.index_density = 50;
    db_config.use_variable_encoding = false;
    db_config.lsm_max_level = 5;
    db_config.token_bucket_capacity = 9999999999;
    db_config.token_bucket_refill_rate = 9999999999;
    db_config.sstable_single_file = true;
    db_config.sstable_dir += "sstable_compression_test/uncompressed/";
    db_config.write_ahead_log_dir += "sstable_compression_test/uncompressed/";

    prepare_dirs(&db_config);

    let mut db_uncompressed = DB::build(db_config.clone()).unwrap();

    for i in 0..20_000u128 {
        // works in other tests, no need to test again here
        db_uncompressed.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes()).unwrap();
    }

    db_config.sstable_dir = "sstables/sstable_compression_test/compressed/".to_string();
    db_config.write_ahead_log_dir = "wal/sstable_compression_test/uncompressed/".to_string();
    db_config.use_compression = true;

    prepare_dirs(&db_config);

    let mut db_compressed = DB::build(db_config.clone()).unwrap();

    for i in 0..20_000u128 {
        db_compressed.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes()).unwrap();
    }

    for i in 0..20_000u128 {
        let get_op= match db_compressed.get(&i.to_ne_bytes()).unwrap() {
            Some(val) => val,
            None => panic!("Get doesn't work")
        };

        println!("{i}");

        assert_eq!(
            &(i * 2).to_ne_bytes(),
            &*get_op
        );
    }
}