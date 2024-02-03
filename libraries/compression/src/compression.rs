use bitvec::macros::internal::funty::Fundamental;
use bitvec::prelude::{BitArray, Msb0};
use bitvec::vec::BitVec;
use bitvec::view::BitView;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Write};
use std::path::Path;

pub struct CompressionDictionary {
    file: File,
    pub(super) list: Vec<Box<[u8]>>,
    pub(super) map: HashMap<Box<[u8]>, u128>,
}

impl CompressionDictionary {
    /// Loads and fills the dictionary from `file_path` file if it exists.
    /// Creates an empty file if it doesn't exist.
    pub fn load(file_path: &str) -> std::io::Result<CompressionDictionary> {
        let mut map = HashMap::new();
        let mut list = vec![];

        let mut buffer = Vec::new();
        let mut file_cursor: usize = 0;

        let mut file = match File::open(file_path) {
            Ok(file) => file,
            Err(_) => {
                if let Some(parent_dir) = Path::new(&file_path).parent() {
                    if !parent_dir.exists() {
                        std::fs::create_dir_all(parent_dir)?;
                    }
                }

                let file = File::create(file_path)?;
                return Ok(CompressionDictionary { file, list, map });
            }
        };

        file.read_to_end(&mut buffer)?;

        while file_cursor < buffer.len() {
            let (key_length, offset) = variable_decode(&buffer[file_cursor..]);
            file_cursor += offset;

            let key_decoded: Box<[u8]> =
                Box::from(&buffer[file_cursor..file_cursor + key_length.unwrap() as usize]);
            file_cursor += key_length.unwrap() as usize;
            if !map.contains_key(&key_decoded) {
                map.insert(key_decoded.clone(), list.len() as u128);
                list.push(key_decoded);
            }
        }

        Ok(CompressionDictionary { file, list, map })
    }

    /// Adds `keys` to the dictionary.
    /// If the key is already in the dictionary it will not be added.
    /// More efficient method for adding a lot of new keys than adding them one by one with encode method.
    pub fn add(&mut self, keys: &Vec<Box<[u8]>>) -> std::io::Result<()> {
        let mut buffer = vec![];

        for key_decoded in keys {
            if !self.map.contains_key(key_decoded) {
                self.map.insert(key_decoded.clone(), self.list.len() as u128);
                self.list.push(key_decoded.clone());
                buffer.extend_from_slice(variable_encode(key_decoded.len() as u128).as_ref());
                buffer.extend_from_slice(key_decoded);
            }
        }

        self.file.write_all(&buffer)?;
        Ok(())
    }

    /// For a given `key` returns the encoded key from the dictionary.
    /// If it's not already in the dictionary, it will be automatically added.
    pub fn encode(&mut self, key: &[u8]) -> std::io::Result<Box<[u8]>> {
        match self.map.get(key) {
            Some(value) => Ok(variable_encode(value.to_owned())),
            None => {
                self.map.insert(Box::from(key), self.list.len() as u128);
                self.list.push(Box::from(key));
                let mut buffer = Vec::new();
                buffer.extend_from_slice(variable_encode(key.len() as u128).as_ref());
                buffer.extend_from_slice(key);
                self.file.write_all(&buffer)?;
                Ok(variable_encode(self.list.len() as u128 - 1))
            }
        }
    }

    /// For a given `key` returns the decoded key from the dictionary.
    /// If the encoded key is not in the dictionary return an error.
    pub fn decode(&self, key: &[u8]) -> std::io::Result<Box<[u8]>> {
        let key_encoded = variable_decode(key).0.unwrap() as usize;

        match self.list.get(key_encoded) {
            Some(key_decoded) => Ok(key_decoded.to_owned()),
            None => Err(Error::new(
                ErrorKind::InvalidData,
                "Encoded key is not in the dictionary!",
            )),
        }
    }
}

/// For given `number_value` returns the boxed slice of bytes with encoded value.
pub fn variable_encode(number_value: u128) -> Box<[u8]> {
    let mut bit_array = BitArray::<[u8; 16], Msb0>::default();
    let mut buffer: Vec<u8> = vec![];

    for i in 0i16..128 {
        bit_array.set(127 - i as usize, (number_value >> i) & 1 == 1);
    }

    let leading_zeros = bit_array.leading_zeros();
    let significant_bytes = if leading_zeros == 128 {
        7
    } else {
        128 - leading_zeros
    };
    let remainder_bytes = if significant_bytes % 7 != 0 {
        7 - significant_bytes % 7
    } else {
        0
    };
    let start_index = 128 - significant_bytes - remainder_bytes;
    let chunks = &mut bit_array[start_index..128].chunks(7).peekable();

    while let Some(chunk) = chunks.next() {
        let mut byte = 0u8;
        for (i, bit) in chunk.iter().enumerate() {
            if !bit.as_bool() {
                byte |= 1 << (6 - i);
            }
        }
        if chunks.peek().is_some() {
            byte |= 1 << 7;
        }
        buffer.push(byte);
    }

    buffer.into_boxed_slice()
}

/// For given `buffer` returns the first encoded value, and it's encoded length in bytes representing new offset.
pub fn variable_decode(buffer: &[u8]) -> (Option<u128>, usize) {
    let mut offset = 0;
    let mut bits = BitVec::<u8, Msb0>::new();

    for byte in buffer {
        let slice = byte.view_bits::<Msb0>();
        for bit in &slice[1..] {
            bits.push(!bit.as_bool());
        }
        offset += 1;
        if !slice[0] {
            break;
        }
    }

    if bits.len() <= 128 {
        let mut value = 0u128;
        for (i, bit) in bits.iter().enumerate() {
            if !bit.as_bool() {
                value |= 1 << (bits.len() - 1 - i);
            }
        }
        (Some(value), offset)
    } else {
        (None, offset)
    }
}
