use std::collections::HashMap;
use std::io::{Error, ErrorKind, Read, Write};
use std::fs::File;


pub struct CompressionDictionary {
    file: File,
    pub(super) list: Vec<Box<[u8]>>,
    pub(super) map: HashMap<Box<[u8]>, u64>,
}

impl CompressionDictionary {
    /// Loads and fills the dictionary from `file_path` file if it exists.
    /// Creates an empty file if it doesn't exist.
    pub fn load(file_path: &str) -> std::io::Result<CompressionDictionary> {
        let mut map = HashMap::new();
        let mut list = vec![];

        let mut buffer = Vec::new();
        let mut file_cursor: usize = 0;

        let mut file = match File::open(&file_path) {
            Ok(file) => file,
            Err(_) => {
                let file = File::create(&file_path)?;
                return Ok(CompressionDictionary { file, list, map });
            }
        };

        file.read_to_end(&mut buffer)?;

        while file_cursor < buffer.len() {
            let length = buffer[file_cursor] as usize;
            file_cursor += 1;

            let key_decoded: Box<[u8]> = Box::from(&buffer[file_cursor..file_cursor + length]);
            file_cursor += length;
            if !map.contains_key(&key_decoded) {
                map.insert(key_decoded.clone(), list.len() as u64);
                list.push(key_decoded);
            }
        }

        return Ok(CompressionDictionary { file, list, map });
    }

    /// Adds `keys` to the dictionary.
    /// If the key is already in the dictionary it will not be added.
    /// More efficient method for adding a lot of new keys than adding them one by one with encode method.
    pub fn add(&mut self, keys: &Vec<Box<[u8]>>) -> std::io::Result<()> {
        let mut buffer = vec![];

        for key_decoded in keys {
            if !self.map.contains_key(key_decoded) {
                self.map.insert(key_decoded.clone(), self.list.len() as u64);
                self.list.push(key_decoded.clone());
                buffer.push(key_decoded.len() as u8);
                buffer.extend_from_slice(key_decoded);
            }
        }

        self.file.write_all(&buffer)?;
        Ok(())
    }

    /// For a given `key` returns the encoded key from the dictionary.
    /// If it's not already in the dictionary, it will be automatically added.
    pub fn encode(&mut self, key: &Box<[u8]>) -> std::io::Result<Box<[u8]>> {
        match self.map.get(key) {
            Some(value) => Ok(Box::from(value.to_ne_bytes())),
            None => {
                self.map.insert(key.clone(), self.list.len() as u64);
                self.list.push(key.clone());
                let mut buffer = Vec::new();
                buffer.push(key.len() as u8);
                buffer.extend_from_slice(key);
                self.file.write_all(&buffer)?;
                Ok(Box::from((self.list.len() as u64).to_ne_bytes()))
            }
        }
    }

    /// For a given `key` returns the decoded key from the dictionary.
    /// If the encoded key is not in the dictionary return an error.
    pub fn decode(&self, key: &Box<[u8]>) -> std::io::Result<Box<[u8]>> {
        let key_encoded = {
            let mut key_encoded_bytes = [0u8; 8];
            key_encoded_bytes.copy_from_slice(key);
            usize::from_ne_bytes(key_encoded_bytes)
        };

        match self.list.get(key_encoded) {
            Some(key_decoded) => Ok(key_decoded.clone()),
            None => Err(Error::new(ErrorKind::InvalidData, "Encoded key is not in the dictionary!"))
        }
    }
}