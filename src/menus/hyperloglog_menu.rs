use colored::Colorize;
use enum_iterator::Sequence;
use db_lib::DB;
use crate::impl_menu;
use crate::menus::get_input_u8;

#[derive(Sequence)]
enum HyperLogLogMenu {
    GetValueFromKey,
    IncreaseCount,
    GetCount,
    Back,
}

impl_menu!(
    HyperLogLogMenu, "HyperLogLog",
    HyperLogLogMenu::GetValueFromKey, "Get value from key".blink(),
    HyperLogLogMenu::IncreaseCount, "Increase count".blink(),
    HyperLogLogMenu::GetCount, "Get count".blink(),
    HyperLogLogMenu::Back, "Back".yellow().italic()
);

pub fn hyperloglog_menu(db: &mut DB) {
    loop {
        match HyperLogLogMenu::get_menu() {
            HyperLogLogMenu::GetValueFromKey => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                match db.hyperloglog_get(key) {
                    Some(value) => println!("Value found: {:?}", value),
                    Err(err) => eprintln!("Error while getting: {}", err)
                }
            }
            HyperLogLogMenu::IncreaseCount => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                let value = get_input_u8("Enter value: ");
                match db.hyperloglog_increase_count(key, value) {
                    Ok(()) => println!("Count increased."),
                    Err(err) => eprintln!("Error during increasing count: {}", err),
                }
            }
            HyperLogLogMenu::GetCount => {
                clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                match db.hyperloglog_get_count(key) {
                    Some(value) => println!("Count: {:?}", value),
                    Err(err) => eprintln!("Error while getting count: {}", err)
                }
            }
            HyperLogLogMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                break
            }
        }
    }
}