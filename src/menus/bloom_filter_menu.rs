use colored::Colorize;
use enum_iterator::Sequence;
use db_lib::DB;
use crate::impl_menu;
use crate::menus::get_input_u8;

#[derive(Sequence)]
enum BloomFilterMenu {
    InsertValue,
    GetValueFromKey,
    ContainsFromKey,
    Back,
}

impl_menu!(
    BloomFilterMenu, "BloomFilter",
    BloomFilterMenu::InsertValue, "Insert value".blink(),
    BloomFilterMenu::GetValueFromKey, "Get value from key".blink(),
    BloomFilterMenu::ContainsFromKey, "Contains from key".blink(),
    BloomFilterMenu::Back, "Back".yellow().italic()
);

pub fn bloom_filter_menu(db: &mut DB) {
    loop {
        match BloomFilterMenu::get_menu() {
            BloomFilterMenu::InsertValue => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                let value = get_input_u8("Enter value: ");
                match db.bloom_filter_insert(key, value) {
                    Ok(()) => println!("Insertion successful."),
                    Err(err) => eprintln!("Error during Insertion: {}", err),
                }
            }
            BloomFilterMenu::GetValueFromKey => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                match db.bloom_filter_get(key) {
                    Some(value) => println!("Value found: {:?}", value),
                    Err(err) => eprintln!("Error while getting: {}", err)
                }
            }
            BloomFilterMenu::ContainsFromKey => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                let value = get_input_u8("Enter value: ");
                match db.bloom_filter_contains(key, value) {
                    Some(value) => {
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