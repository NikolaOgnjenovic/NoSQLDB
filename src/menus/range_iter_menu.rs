use crate::impl_menu;
use crate::menus::{get_input_u8, get_input_usize, UserMenu};
use colored::Colorize;
use enum_iterator::Sequence;
use NoSQLDB::DB;

#[derive(Sequence)]
enum RangeIterMenu {
    ChangeParameters,
    StartIteration,
    Back,
}

impl_menu!(
    RangeIterMenu,
    "Prefix iterator",
    RangeIterMenu::ChangeParameters,
    "Change parameters".blink(),
    RangeIterMenu::StartIteration,
    "Start iteration".blink(),
    RangeIterMenu::Back,
    "Back".yellow()
);

#[derive(Sequence)]
enum IteratorMenu {
    IterateNext,
    IteratePrev,
    Back,
}

impl_menu!(
    IteratorMenu,
    "Iterator menu",
    IteratorMenu::IterateNext,
    "Iterate next".blink(),
    IteratorMenu::IteratePrev,
    "Iterate previous".blink(),
    IteratorMenu::Back,
    "Back".yellow()
);

pub fn range_iter_menu(db: &mut DB) {
    let (mut page_count, mut page_len) = (Some(5), Some(20));
    loop {
        println!(
            "Current page number: {}, current page length: {}",
            page_count.unwrap(),
            page_len.unwrap()
        );
        match RangeIterMenu::get_menu() {
            RangeIterMenu::ChangeParameters => {
                page_count = get_input_usize("Enter page number: ");
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
            RangeIterMenu::StartIteration => {
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

                let mut on_first = true;
                let mut on_last = false;

                loop {
                    match IteratorMenu::get_menu() {
                        IteratorMenu::IterateNext => {
                            if on_last {
                                println!("On last element, nothing found");
                                continue;
                            }

                            on_first = false;

                            let iter_result = paginator.range_iterate_next(
                                min_key.as_ref().unwrap(),
                                max_key.as_ref().unwrap(),
                            );
                            match iter_result {
                                Ok(iter_element_result) => match iter_element_result {
                                    None => {
                                        on_last = true;
                                        continue;
                                    }
                                    Some((key, entry)) => {
                                        let entry_val = entry.get_value();
                                        let key_string = String::from_utf8_lossy(&key);
                                        let value_string = String::from_utf8_lossy(&entry_val);
                                        println!(
                                            "Found key: {}, with value: {}",
                                            key_string, value_string
                                        );
                                    }
                                },
                                Err(e) => eprintln!(
                                    "An error occurred during paginator next iteration: {}",
                                    e
                                ),
                            }
                        }
                        IteratorMenu::IteratePrev => {
                            if on_first {
                                println!("On first element, nothing found");
                                continue;
                            }

                            on_last = false;

                            let iter_result = paginator.iterate_prev();
                            match iter_result {
                                Ok(iter_element_result) => match iter_element_result {
                                    None => {
                                        on_first = true;
                                        continue;
                                    }
                                    Some((key, entry)) => {
                                        let entry_val = entry.get_value();
                                        let key_string = String::from_utf8_lossy(&key);
                                        let value_string = String::from_utf8_lossy(&entry_val);
                                        println!(
                                            "Found key: {}, with value: {}",
                                            key_string, value_string
                                        );
                                    }
                                },
                                Err(e) => eprintln!(
                                    "An error occurred during paginator prev iteration: {}",
                                    e
                                ),
                            }
                        }
                        IteratorMenu::Back => {
                            paginator.iterate_stop();
                            break;
                        }
                    }
                }
            }
            RangeIterMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                break;
            }
        }
    }
}
