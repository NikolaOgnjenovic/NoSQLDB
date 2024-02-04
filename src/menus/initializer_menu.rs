use crate::impl_menu;
use crate::menus::customize_menu::customize_menu;
use crate::menus::db_menu::db_menu;
use crate::menus::UserMenu;
use colored::Colorize;
use db_config::DBConfig;
use enum_iterator::Sequence;
use std::path::Path;

const CONFIG_FILE_PATH: &str = "config.json";

#[derive(Sequence)]
enum DBConfigMenu {
    Continue,
    CustomizeConfig,
    RevertToDefault,
    Exit,
}

impl_menu!(
    DBConfigMenu,
    "CONFIGURATION",
    DBConfigMenu::Continue,
    "Continue".blink(),
    DBConfigMenu::CustomizeConfig,
    "Customize config".blink(),
    DBConfigMenu::RevertToDefault,
    "Revert config to default".blink(),
    DBConfigMenu::Exit,
    "Exit".red().italic()
);

pub fn initializer_menu() {
    let mut dbconfig = DBConfig::default();
    if !Path::new(CONFIG_FILE_PATH).exists() {
        dbconfig
            .save(CONFIG_FILE_PATH)
            .expect("Failed to save default configuration.");
    }

    clearscreen::clear().expect("Failed to clear screen.");
    loop {
        match DBConfigMenu::get_menu() {
            DBConfigMenu::Continue => {
                clearscreen::clear().expect("Failed to clear screen.");
                println!("Using loaded configuration.");
                db_menu(&mut dbconfig);
            }
            DBConfigMenu::CustomizeConfig => {
                customize_menu(&mut dbconfig); // Customize config
                dbconfig
                    .save(CONFIG_FILE_PATH)
                    .expect("Failed to save config."); // Save changes made in customize_menu
            }
            DBConfigMenu::RevertToDefault => {
                // Revert config to default
                clearscreen::clear().expect("Failed to clear screen.");
                let default_dbconfig = DBConfig::default();
                default_dbconfig
                    .save(CONFIG_FILE_PATH)
                    .expect("Failed to revert back to default.");
                dbconfig = DBConfig::load(CONFIG_FILE_PATH).expect("Failed to load from default.");
                println!("Configuration is set back to default.");
            }
            DBConfigMenu::Exit => {
                println!("Goodbye!");
                return;
            }
        }
    }
}
