use NoSQLDB::DB;

#[test]
fn test_hyperloglog_insert_and_get_count() {
    let mut db = DB::default();

    let key = "test_hyperloglog".as_bytes();
    let value = "test_value".as_bytes();

    assert!(db.hyperloglog_increase_count(key, value).is_ok());

    assert!(db.hyperloglog_get_count(key).unwrap() > 0);
}

#[test]
fn test_hyperloglog_increase_count() {
    let mut db = DB::default();

    let key = "test_hyperloglog".as_bytes();
    let value = "test_value".as_bytes();

    assert!(db.hyperloglog_increase_count(key, value).is_ok());

    assert!(db.hyperloglog_increase_count(key, value).is_ok());

    assert!(db.hyperloglog_get_count(key).unwrap() > 1);
}

#[test]
fn test_bloom_filter_operations() {
    let mut db = DB::default();

    let key = "test_bloom_filter".as_bytes();
    let value1 = "value1".as_bytes();
    let value2 = "value2".as_bytes();

    assert!(db.bloom_filter_insert(key, value1).is_ok());

    let bf_bytes = db.bloom_filter_get(key).expect("");
    assert!(bf_bytes.is_some());

    assert!(db.bloom_filter_contains(key, value1).expect(""));

    assert!(!db.bloom_filter_contains(key, value2).expect(""));
}

#[test]
fn test_count_min_sketch_operations() {
    let mut db = DB::default();

    let key = "test_count_min_sketch".as_bytes();
    let value1 = "value1".as_bytes();
    let value2 = "value2".as_bytes();

    assert!(db.count_min_sketch_increase_count(key, value1).is_ok());

    let cms_bytes = db.count_min_sketch_get(key).expect("");
    assert!(cms_bytes.is_some());

    assert_eq!(db.count_min_sketch_get_count(key, value1).expect(""), 1);

    assert_eq!(db.count_min_sketch_get_count(key, value2).expect(""), 0);
}