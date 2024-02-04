use crate::impl_menu;
use crate::menus::{get_input_u32, get_input_u8, UserMenu};
use colored::Colorize;
use enum_iterator::Sequence;
use NoSQLDB::DB;

#[derive(Sequence)]
enum HyperLogLogMenu {
    CreateRetrieve,
    GetValueFromKey,
    IncreaseCount,
    GetCount,
    Back,
}

impl_menu!(
    HyperLogLogMenu,
    "HyperLogLog",
    HyperLogLogMenu::CreateRetrieve,
    "Create or retrieve from a key".blink(),
    HyperLogLogMenu::GetValueFromKey,
    "Get value from key".blink(),
    HyperLogLogMenu::IncreaseCount,
    "Increase count".blink(),
    HyperLogLogMenu::GetCount,
    "Get count".blink(),
    HyperLogLogMenu::Back,
    "Back".yellow().italic()
);

pub fn hyperloglog_menu(db: &mut DB) {
    loop {
        match HyperLogLogMenu::get_menu() {
            HyperLogLogMenu::CreateRetrieve => {
                clearscreen::clear().expect("Failed to clear screen.");
                let precision = get_input_u32("Enter precision: ");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                match db.hyperloglog_create(key, precision) {
                    Ok(_) => println!("HLL created."),
                    Err(err) => eprintln!("Error during creation/retrieval: {}", err),
                }
            }
            HyperLogLogMenu::GetValueFromKey => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                match db.hyperloglog_get(key) {
                    Ok(value) => match value {
                        Some(value) => println!("Value found: {:?}", value),
                        None => println!("Value not found."),
                    },
                    Err(err) => eprintln!("Error while getting: {}", err),
                }
            }
            HyperLogLogMenu::IncreaseCount => {
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

                match db.hyperloglog_increase_count(key, value) {
                    Ok(()) => println!("Count increased."),
                    Err(err) => eprintln!("Error during increasing count: {}", err),
                }
            }
            HyperLogLogMenu::GetCount => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                match db.hyperloglog_get_count(key) {
                    Ok(value) => println!("Count: {:?}", value),
                    Err(err) => eprintln!("Error while getting count: {}", err),
                }
            }
            HyperLogLogMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                break;
            }
        }
    }
}
