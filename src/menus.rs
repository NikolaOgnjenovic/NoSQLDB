mod db_menu;
pub(crate) mod initializer_menu;
mod count_min_sketch_menu;
mod customize_menu;
mod hyperloglog_menu;
mod bloom_filter_menu;


use std::io;
use inquire::{Select, Text};

pub trait UserMenu: std::fmt::Display + enum_iterator::Sequence {
    fn get_message() -> &'static str;
    fn get_menu() -> Self {
        Select::new(Self::get_message(), enum_iterator::all::<Self>().collect::<Vec<_>>()).prompt().unwrap()
    }
}

#[macro_export]
macro_rules! impl_menu {
    ($menu: ty, $msg: expr, $($op: pat, $op_msg: expr),*) => {
        impl crate::UserMenu for $menu {
            fn get_message() -> &'static str {
                $msg
            }
        }

        impl std::fmt::Display for $menu {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        $op => write!(f, "{}", $op_msg),
                    )*
                    _ => panic!("Unreachable")
                }
            }
        }
    }
}

/// Helper function to get u8 input
fn get_input_u8(prompt_message: &str) -> &[u8] {
    loop {
        let input_str = Text::new(prompt_message)
            .prompt()
            .unwrap();
        let input_bytes = input_str.as_bytes();
        if !input_bytes.is_empty() {
            return input_bytes;
        }
        println!("Invalid key. Please enter a valid byte sequence.");
    }
}

/// Helper function to print out the requirements and get usize input
fn get_input_with_range(prompt_message: &str, min: usize, max: usize) -> usize {
    let prompt_with_range = format!("{} ({:?} - {:?})", prompt_message, min, max);
    loop {
        println!("{}", prompt_with_range);
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        match input.trim().parse::<usize>() {
            Ok(value) if value >= min && value <= max => {
                return value;
            }
            _ => println!("Invalid input. Please enter a valid number between {} and {}.", min, max),
        }
    }
}