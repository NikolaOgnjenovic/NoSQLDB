use enum_iterator::Sequence;
use db_lib::DB;
use colored::Colorize;
use crate::impl_menu;
use crate::menus::get_input_u8;

#[derive(Sequence)]
enum CountMinSketchMenu {
    Get,
    IncreaseCount,
    GetCount,
    Back,
}

impl_menu!(
    CountMinSketchMenu, "CountMinSketch",
    CountMinSketchMenu::Get, "Get value".blink(),
    CountMinSketchMenu::IncreaseCount, "Increase Count".blink(),
    CountMinSketchMenu::GetCount, "Get Count".blink(),
    CountMinSketchMenu::Back, "Back".yellow().italic()
);

pub fn count_min_sketch_menu(db: &mut DB) {
    loop {
        match CountMinSketchMenu::get_menu() {
            CountMinSketchMenu::Get => {
                 clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                match db.count_min_sketch_get(key) {
                    Some(value) => println!("Value found: {:?}", value),
                    Err(err) => eprintln!("Error while getting: {}", err)
                }
            }
            CountMinSketchMenu::IncreaseCount => {
                 clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                let value = get_input_u8("Enter value: ");
                match db.count_min_sketch_increase_count(key, value) {
                    Ok(()) => println!("Count increased."),
                    Err(err) => eprintln!("Error during increasing count: {}", err),
                }
            }
            CountMinSketchMenu::GetCount => {
                 clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                let value = get_input_u8("Enter value: ");
                match db.count_min_sketch_get_count(key, value) {
                    Some(value) => println!("Count: {:?}", value),
                    Err(err) => eprintln!("Error while getting count: {}", err)
                }
            }
            CountMinSketchMenu::Back => {
                 clearscreen::clear().expect("Failed to clear screen.");
                break
            }
        }
    }
}