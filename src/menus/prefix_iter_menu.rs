use crate::impl_menu;
use crate::menus::{get_input_u8, UserMenu};
use colored::Colorize;
use enum_iterator::Sequence;
use NoSQLDB::DB;

#[derive(Sequence)]
enum PrefixIterMenu {
    StartIteration,
    Back,
}

impl_menu!(
    PrefixIterMenu,
    "Prefix iterator",
    PrefixIterMenu::StartIteration,
    "Start iteration".blink(),
    PrefixIterMenu::Back,
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

pub fn prefix_iter_menu(db: &mut DB) {
    loop {
        match PrefixIterMenu::get_menu() {
            PrefixIterMenu::StartIteration => {
                let prefix = get_input_u8("Enter prefix: ");

                if prefix.is_none() {
                    println!("Failed to serialize prefix into bytes");
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

                            let iter_result =
                                paginator.prefix_iterate_next(prefix.as_ref().unwrap());
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
            PrefixIterMenu::Back => {
                clearscreen::clear().expect("Failed to clear screen.");
                break;
            }
        }
    }
}
