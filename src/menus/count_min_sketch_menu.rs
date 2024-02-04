use enum_iterator::Sequence;
use NoSQLDB::DB;
use colored::Colorize;
use crate::impl_menu;
use crate::menus::{get_input_f64, get_input_u8, UserMenu};

#[derive(Sequence)]
enum CountMinSketchMenu {
    Create,
    Get,
    IncreaseCount,
    GetCount,
    Back,
}

impl_menu!(
    CountMinSketchMenu, "CountMinSketch",
    CountMinSketchMenu::Create, "Create/retrieve".blink(),
    CountMinSketchMenu::Get, "Get value".blink(),
    CountMinSketchMenu::IncreaseCount, "Increase Count".blink(),
    CountMinSketchMenu::GetCount, "Get Count".blink(),
    CountMinSketchMenu::Back, "Back".yellow().italic()
);

pub fn count_min_sketch_menu(db: &mut DB) {
    loop {
        match CountMinSketchMenu::get_menu() {
            CountMinSketchMenu::Create => {
                clearscreen::clear().expect("Failed to clear screen.");
                let probability = get_input_f64("Enter probability: (0.001-1)");
                let tolerance = get_input_f64("Enter tolerance: (0.001-1)");
                let key = get_input_u8("Enter count min sketch associated key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }

                match db.count_min_sketch_create(key.as_ref().unwrap(), probability, tolerance) {
                    Ok(_) => println!("CountMinSketch created."),
                    Err(err) => eprintln!("Error during creation/retrieval: {}", err),
                }
            }
            CountMinSketchMenu::Get => {
                 clearscreen::clear().expect("Failed to clear screen.");
                let key = get_input_u8("Enter key: ");
                if key.is_none() {
                    println!("Failed to serialize key into bytes.");
                    continue;
                }
                let key = &key.unwrap();

                match db.count_min_sketch_get(key) {
                    Ok(value) => {
                        match value {
                            Some(value) => println!("Value found: {:?}", value),
                            None => println!("Value not found")
                        }
                    },
                    Err(err) => eprintln!("Error while getting: {}", err)
                }
            }
            CountMinSketchMenu::IncreaseCount => {
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

                match db.count_min_sketch_increase_count(key, value) {
                    Ok(()) => println!("Count increased."),
                    Err(err) => eprintln!("Error during increasing count: {}", err),
                }
            }
            CountMinSketchMenu::GetCount => {
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

                match db.count_min_sketch_get_count(key, value) {
                    Ok(value) => println!("Count: {:?}", value),
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