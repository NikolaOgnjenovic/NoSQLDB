mod db_config;

pub use db_config::CompactionAlgorithmType;
pub use db_config::DBConfig;
pub use db_config::MemoryTableType;

#[cfg(test)]
mod tests {
    use super::DBConfig;
    use crate::db_config::MemoryTableType;

    #[test]
    fn save_load() {
        let file_path = "config1.json";

        let config1 = DBConfig::new();
        config1.save(file_path).expect("Saving config file failed!");

        let config2 = DBConfig::load(file_path).unwrap();

        assert_eq!(config1, config2);
    }

    #[test]
    fn update_property() {
        let file_path = "config2.json";
        let mut config1 = DBConfig::new();
        config1.memory_table_type = MemoryTableType::BTree;
        config1.save(file_path).expect("Saving config file failed!");

        let mut config2 = DBConfig::load(file_path).unwrap();
        config2.memory_table_type = MemoryTableType::SkipList;
        config2.save(file_path).expect("Saving config file failed!");

        let config3 = DBConfig::load(file_path).unwrap();

        assert_ne!(config1, config3);
        assert_eq!(config2, config3);
    }
}
