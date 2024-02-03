use colored::Colorize;
use enum_iterator::Sequence;
use NoSQLDB::DB;
use crate::impl_menu;
use crate::menus::{get_input_f64, get_input_u8, get_input_usize, UserMenu};

#[derive(Sequence)]
enum BloomFilterMenu {
    CreateRetrieve,
    InsertValue,
    GetValueFromKey,
    ContainsFromKey,
    Back,
}

impl_menu!(
    BloomFilterMenu, "BloomFilter",
    BloomFilterMenu::CreateRetrieve, "Create/retrieve".blink(),
    BloomFilterMenu::InsertValue, "Insert value".blink(),
    BloomFilterMenu::GetValueFromKey, "Get value from key".blink(),
    BloomFilterMenu::ContainsFromKey, "Contains from key".blink(),
    BloomFilterMenu::Back, "Back".yellow().italic()
);

pub fn bloom_filter_menu(db: &mut DB) {
    loop {
        match BloomFilterMenu::get_menu() {
            BloomFilterMenu::CreateRetrieve => {
                clearscreen::clear().expect("Failed to clear screen.");
                let probability = get_input_f64("Enter probability: (0.001-1");
                let cap = get_input_usize("Enter capacity: (10k-100mil)");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                match db.bloom_filter_create(key, probability, cap) {
                    Ok(_) => println!("BloomFilter created."),
                    Err(err) => eprintln!("Error during creation/retrieval: {}", err),
                }
            }
            BloomFilterMenu::InsertValue => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                let value = get_input_u8("Enter value: ");
                if value.is_none() {
                    println!("Failed to serialize value into bytes.");
                    continue;
                }
                let value = &value.unwrap();

                match db.bloom_filter_insert(key, value) {
                    Ok(()) => println!("Insertion successful."),
                    Err(err) => eprintln!("Error during Insertion: {}", err),
                }
            }
            BloomFilterMenu::GetValueFromKey => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                match db.bloom_filter_get(key) {
                    Ok(value) => {
                        match value {
                            Some(value) => println!("Value found: {:?}", value),
                            None => println!("Value not found.")
                        }
                    },
                    Err(err) => eprintln!("Error while getting: {}", err)
                }
            }
            BloomFilterMenu::ContainsFromKey => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                let value = get_input_u8("Enter value: ");
                if value.is_none() {
                    println!("Failed to serialize value into bytes.");
                    continue;
                }
                let value = &value.unwrap();

                match db.bloom_filter_contains(key, value) {
                    Ok(value) => {
                        if value {
                            println!("Bloom filter contains the given key/value pair.");
                        } else {
                            println!("Bloom filter does not contain the given key/value pair.");
                        }
                    }
                    Err(err) => eprintln!("Error while checking: {}", err),
                }
            }
            BloomFilterMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                break;
            }
        }
    }
}