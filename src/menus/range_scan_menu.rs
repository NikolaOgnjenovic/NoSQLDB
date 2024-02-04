use crate::impl_menu;
use crate::menus::{get_input_u8, get_input_usize, UserMenu};
use colored::Colorize;
use enum_iterator::Sequence;
use NoSQLDB::DB;

#[derive(Sequence)]
enum RangeScanMenu {
    ChangeParameters,
    Scan,
    Back,
}

impl_menu!(
    RangeScanMenu,
    "Range scan",
    RangeScanMenu::ChangeParameters,
    "Change parameters".blink(),
    RangeScanMenu::Scan,
    "Get scan".blink(),
    RangeScanMenu::Back,
    "Back".yellow()
);

pub fn range_scan_menu(db: &mut DB) {
    let (mut page_count, mut page_len) = (Some(5), Some(20));
    loop {
        println!(
            "Current page count: {}, current page length: {}",
            page_count.unwrap(),
            page_len.unwrap()
        );
        match RangeScanMenu::get_menu() {
            RangeScanMenu::ChangeParameters => {
                page_count = get_input_usize("Enter page count: ");
                page_len = get_input_usize("Enter page size: ");

                if page_count.is_none() {
                    println!("Failed to interpret page count");
                    continue;
                }

                if page_len.is_none() {
                    println!("Failed to page length");
                    continue;
                }
            }
            RangeScanMenu::Scan => {
                let min_key = get_input_u8("Enter minimum key: ");
                let max_key = get_input_u8("Enter maximum key: ");

                if min_key.is_none() {
                    println!("Failed to serialize min_key into bytes");
                    continue;
                }

                if max_key.is_none() {
                    println!("Failed to serialize max_key into bytes");
                    continue;
                }

                let mut paginator = db.get_paginator();

                match paginator.range_scan(
                    &min_key.unwrap(),
                    &max_key.unwrap(),
                    page_count.unwrap(),
                    page_len.unwrap(),
                ) {
                    Ok(entries) => {
                        for (key, entry) in entries {
                            let entry_val = entry.get_value();
                            let key_string = String::from_utf8_lossy(&key);
                            let value_string = String::from_utf8_lossy(&entry_val);
                            println!("Found key: {}, with value: {}", key_string, value_string);
                        }
                    }
                    Err(e) => eprintln!("An error occurred during paginator creation: {}", e),
                }
            }
            RangeScanMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                break;
            }
        }
    }
}
