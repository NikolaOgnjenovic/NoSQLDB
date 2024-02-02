use colored::Colorize;
use enum_iterator::Sequence;
use inquire::Text;
use db_config::DBConfig;
use NoSQLDB::DB;
use crate::impl_menu;
use crate::menus::bloom_filter_menu::bloom_filter_menu;
use crate::menus::count_min_sketch_menu::count_min_sketch_menu;
use crate::menus::{get_input_u8, UserMenu};
use crate::menus::hyperloglog_menu::hyperloglog_menu;

#[derive(Sequence)]
enum DBMenu {
    Insert,
    Get,
    Delete,
    BloomFilter,
    CountMinSketch,
    HyperLogLog,
    SimHash,
    Exit,
}

impl_menu!(
    DBMenu, "MAIN MENU",
    DBMenu::Insert, "Insert".blink(),
    DBMenu::Get, "Get".blink(),
    DBMenu::Delete, "Delete".blink(),
    DBMenu::BloomFilter, "BloomFilter".blink(),
    DBMenu::CountMinSketch, "CountMinSketch".blink(),
    DBMenu::HyperLogLog, "HyperLogLog".blink(),
    DBMenu::SimHash, "SimHash".blink(),
    DBMenu::Exit, "Exit".red().italic()
);

pub fn db_menu(dbconfig: &mut DBConfig) {
    let mut db = match DB::build(dbconfig.clone()) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("Error building DB: {}", err);
            return;
        }
    };
    loop {
        match DBMenu::get_menu() {
            DBMenu::Insert => {
                 clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                let value = get_input_u8("Enter value: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                if value.is_none() {
                    println!("Failed to serialize value into bytes.");
                    continue;
                }

                let key = &key.unwrap();
                let value = &value.unwrap();
                match db.insert(key, value, true) {
                    Ok(()) => println!("Insertion successful"),
                    Err(err) => eprintln!("Error during insertion: {}", err),
                }
            }
            DBMenu::Get => {
                 clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                match db.get(key).ok() {
                    Some(value) => println!("Value found: {:?}", value),
                    None => println!("Value not found for the given key."),
                }
            }
            DBMenu::Delete => {
                 clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                db.delete(key, true).expect("Deletion error");
                match db.delete(&key, true) {
                    Ok(()) => println!("Deletion successful."),
                    Err(err) => eprintln!("Error during deletion: {}", err),
                }
            }
            DBMenu::BloomFilter => bloom_filter_menu(&mut db),

            DBMenu::CountMinSketch => count_min_sketch_menu(&mut db),

            DBMenu::HyperLogLog => hyperloglog_menu(&mut db),

            DBMenu::SimHash => {
                 clearscreen::clear().expect("Failed to clear screen.");

                let data1: String = Text::new("Enter first data string:")
                    .prompt()
                    .unwrap();

                let data2: String = Text::new("Enter second data string:")
                    .prompt()
                    .unwrap();

                println!("Hamming distance of 2 inputs is: {}", db.sim_hash_calculate_hamming_distance(&*data1, &*data2));
            }
            DBMenu::Exit => {
                println!("Exiting...");
                db.shut_down();
                std::process::exit(0);
            }
        }
    }
}