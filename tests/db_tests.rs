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
fn test_read_write_path_one() {
    let mut db_config = DBConfig::default();
    db_config.sstable_dir += "general_one/";
    db_config.write_ahead_log_dir += "general_one/";

    prepare_dirs(&db_config);

    let mut db = DB::build(db_config).unwrap();

    db.insert("test_key".as_bytes(), "test_value".as_bytes()).unwrap();

    let get_op= match db.get("test_key".as_bytes()).unwrap() {
        Some(val) => val,
        None => panic!("Get doesn't work")
    };

    assert_eq!(
        "test_value".as_bytes(),
        &*get_op
    );
}

#[test]
fn test_read_write_path_multiple() {
    let mut db_config = DBConfig::default();
    db_config.memory_table_type = BTree;
    db_config.summary_density = 10;
    db_config.index_density = 50;
    db_config.use_variable_encoding = false;
    db_config.lsm_max_level = 5;
    db_config.token_bucket_capacity = 9999999999;
    db_config.token_bucket_refill_rate = 9999999999;
    db_config.sstable_dir += "general_multiple/";
    db_config.write_ahead_log_dir += "general_multiple/";

    prepare_dirs(&db_config);

    let mut db = DB::build(db_config).unwrap();

    let base_key = "test_key";
    let base_value = "test_value";

    for i in 0..20_000u128 {
        let i_bytes = i.to_ne_bytes();
        let i_double_bytes = (i * 2).to_ne_bytes();
        let str_key_bytes = format!("{}{}", base_key, i.to_string());
        let str_val_bytes = format!("{}{}", base_value, i.to_string());
        let (key, value) = if i % 2 == 0 {
            (
                Box::from(str_key_bytes.as_bytes()),
                Box::from(str_val_bytes.as_bytes())
            )
        } else {
            (
                Box::from(i_bytes.as_ref()),
                Box::from(i_double_bytes.as_ref())
            )
        };

        db.insert(&key, &value).unwrap();
    }

    for i in 0..20_000u128 {
        let i_bytes = i.to_ne_bytes();
        let i_double_bytes = (i * 2).to_ne_bytes();
        let str_key_bytes = format!("{}{}", base_key, i.to_string());
        let str_val_bytes = format!("{}{}", base_value, i.to_string());
        let (key, value) = if i % 2 == 0 {
            (
                Box::from(str_key_bytes.as_bytes()),
                Box::from(str_val_bytes.as_bytes())
            )
        } else {
            (
                Box::from(i_bytes.as_ref()),
                Box::from(i_double_bytes.as_ref())
            )
        };

        let get_op= match db.get(&key).unwrap() {
            Some(val) => val,
            None => panic!("Get doesn't work")
        };

        println!("{i}");

        assert_eq!(
            value,
            Box::new(&*get_op)
        );
    }
}

#[test]
fn test_read_write_path_multiple_single_file() {
    let mut db_config = DBConfig::default();
    db_config.memory_table_type = BTree;
    db_config.summary_density = 10;
    db_config.index_density = 50;
    db_config.use_variable_encoding = false;
    db_config.lsm_max_level = 5;
    db_config.token_bucket_capacity = 9999999999;
    db_config.token_bucket_refill_rate = 9999999999;
    db_config.sstable_single_file = true;
    db_config.sstable_dir += "general_multiple_single_file/";
    db_config.write_ahead_log_dir += "general_multiple_single_file/";

    prepare_dirs(&db_config);

    let mut db = DB::build(db_config).unwrap();

    let base_key = "test_key";
    let base_value = "test_value";

    for i in 0..20_000u128 {
        let i_bytes = i.to_ne_bytes();
        let i_double_bytes = (i * 2).to_ne_bytes();
        let str_key_bytes = format!("{}{}", base_key, i.to_string());
        let str_val_bytes = format!("{}{}", base_value, i.to_string());
        let (key, value) = if i % 2 == 0 {
            (
                Box::from(str_key_bytes.as_bytes()),
                Box::from(str_val_bytes.as_bytes())
            )
        } else {
            (
                Box::from(i_bytes.as_ref()),
                Box::from(i_double_bytes.as_ref())
            )
        };

        db.insert(&key, &value).unwrap();
    }

    for i in 0..20_000u128 {
        let i_bytes = i.to_ne_bytes();
        let i_double_bytes = (i * 2).to_ne_bytes();
        let str_key_bytes = format!("{}{}", base_key, i.to_string());
        let str_val_bytes = format!("{}{}", base_value, i.to_string());
        let (key, value) = if i % 2 == 0 {
            (
                Box::from(str_key_bytes.as_bytes()),
                Box::from(str_val_bytes.as_bytes())
            )
        } else {
            (
                Box::from(i_bytes.as_ref()),
                Box::from(i_double_bytes.as_ref())
            )
        };

        let get_op= match db.get(&key).unwrap() {
            Some(val) => val,
            None => panic!("Get doesn't work")
        };

        println!("{i}");

        assert_eq!(
            value,
            Box::new(&*get_op)
        );
    }
}