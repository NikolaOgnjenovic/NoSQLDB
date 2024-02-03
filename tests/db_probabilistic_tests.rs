use std::fs::{read_dir, remove_dir_all, remove_file};
use db_config::DBConfig;
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
}

#[test]
fn test_hyperloglog_insert_and_get_count() {
    let mut db_config = db_config::DBConfig::default();
    db_config.sstable_dir += "hyperloglog_integration/";
    db_config.write_ahead_log_dir += "hyperloglog_integration/";
    db_config.token_bucket_capacity = 999999;
    db_config.token_bucket_refill_rate = 999999;

    prepare_dirs(&db_config);

    let mut db = DB::build(db_config).unwrap();

    let key = "test_hyperloglog".as_bytes();
    let value = "test_value".as_bytes();

    assert!(db.hyperloglog_create(key, None).is_ok());

    for _ in 0..10_000 {
        assert!(db.hyperloglog_increase_count(key, value).is_ok());
    }

    assert!(db.hyperloglog_get_count(key).unwrap() > 0);
}

#[test]
fn test_bloom_filter_operations() {
    let mut db_config = db_config::DBConfig::default();
    db_config.sstable_dir += "bloom_filter_integration/";
    db_config.write_ahead_log_dir += "bloom_filter_integration/";
    db_config.token_bucket_capacity = 999999;
    db_config.token_bucket_refill_rate = 999999;

    prepare_dirs(&db_config);

    let mut db = DB::build(db_config).unwrap();

    let key = "test_bloom_filter".as_bytes();
    let value1 = "value1".as_bytes();
    let value2 = "value2".as_bytes();

    assert!(db.bloom_filter_create(key, Some(0.01), Some(10_000)).is_ok());
    assert!(db.bloom_filter_insert(key, value1).is_ok());

    for i in 0..1_000 {
        assert!(db.bloom_filter_insert(key, &i.to_string().as_bytes()).is_ok());
    }

    let bf_bytes = db.bloom_filter_get(key).expect("");
    assert!(bf_bytes.is_some());

    assert!(db.bloom_filter_contains(key, &100.to_string().as_bytes()).expect(""));

    assert!(!db.bloom_filter_contains(key, value2).expect(""));
}

#[test]
fn test_count_min_sketch_operations() {
    let mut db_config = db_config::DBConfig::default();
    db_config.sstable_dir += "count_min_sketch_integration/";
    db_config.write_ahead_log_dir += "count_min_sketch_integration/";
    db_config.token_bucket_capacity = 999999;
    db_config.token_bucket_refill_rate = 999999;

    prepare_dirs(&db_config);

    let mut db = DB::build(db_config).unwrap();

    let key = "test_count_min_sketch".as_bytes();
    let value1 = "value1".as_bytes();
    let value2 = "value2".as_bytes();

    assert!(db.count_min_sketch_create(key, None, None).is_ok());

    for _ in 0..1_000 {
        assert!(db.count_min_sketch_increase_count(key, &value1).is_ok());
    }

    let cms_bytes = db.count_min_sketch_get(key).expect("");
    assert!(cms_bytes.is_some());

    assert!(db.count_min_sketch_get_count(key, value1).expect("") > 500);

    assert_eq!(db.count_min_sketch_get_count(key, value2).expect(""), 0);
}