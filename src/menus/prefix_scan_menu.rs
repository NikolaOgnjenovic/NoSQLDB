use crate::impl_menu;
use crate::menus::{get_input_u8, get_input_usize, UserMenu};
use colored::Colorize;
use enum_iterator::Sequence;
use NoSQLDB::DB;

#[derive(Sequence)]
enum PrefixScanMenu {
    ChangeParameters,
    Scan,
    Back,
}

impl_menu!(
    PrefixScanMenu,
    "Prefix scan",
    PrefixScanMenu::ChangeParameters,
    "Change parameters".blink(),
    PrefixScanMenu::Scan,
    "Get scan".blink(),
    PrefixScanMenu::Back,
    "Back".yellow()
);

pub fn prefix_scan_menu(db: &mut DB) {
    let (mut page_count, mut page_len) = (Some(5), Some(20));
    loop {
        println!(
            "Current page count: {}, current page length: {}",
            page_count.unwrap(),
            page_len.unwrap()
        );
        match PrefixScanMenu::get_menu() {
            PrefixScanMenu::ChangeParameters => {
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
            PrefixScanMenu::Scan => {
                let prefix = get_input_u8("Enter prefix: ");

                if prefix.is_none() {
                    println!("Failed to serialize prefix into bytes");
                    continue;
                }

                let mut paginator = db.get_paginator();
                match paginator.prefix_scan(
                    &prefix.unwrap(),
                    page_len.unwrap(),
                    page_count.unwrap(),
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
            PrefixScanMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                break;
            }
        }
    }
}
