mod db_menu;
pub mod initializer_menu;
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
        impl crate::menus::UserMenu for $menu {
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
                    _ => panic!("Not all entries implemented on menu \"{}\"", <$menu>::get_message())
                }
            }
        }
    }
}

/// Helper function to get u8 input
fn get_input_u8(prompt_message: &str) -> Option<Box<[u8]>> {
    let input_str = Text::new(prompt_message)
        .prompt()
        .unwrap();
    let input_bytes = input_str.as_bytes();
    if !input_bytes.is_empty() {
        return Some(Box::from(input_bytes))
    }

    None
}

fn get_input_u32(prompt_message: &str) -> Option<u32> {
    let input_str = Text::new(prompt_message)
        .prompt()
        .unwrap();

    match input_str.trim().parse::<u32>() {
        Ok(value) => Some(value),
        Err(_) => None,
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

fn get_input_usize(prompt_message: &str) -> Option<usize> {
    println!("{}", prompt_message);
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).expect("Failed to read line");
    input.trim().parse::<usize>().ok()
}

fn get_input_f64(prompt_message: &str) -> Option<f64> {
    println!("{}", prompt_message);
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).expect("Failed to read line");
    input.trim().parse::<f64>().ok()
}
