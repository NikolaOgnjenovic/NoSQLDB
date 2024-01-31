use std::error::Error;
use crate::TimeStamp;
use crc::{Crc, CRC_32_ISCSI};
use compression::{variable_encode, variable_decode};
use crate::crc_error::CRCError;

/// Public struct that SegmentTrait implementations return on get.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct MemoryEntry {
    value: Box<[u8]>,
    tombstone: bool,
    timestamp: u128,
}

impl MemoryEntry {
    pub fn from(value: &[u8], tombstone: bool, timestamp: u128) -> Self {
        MemoryEntry {
            value: Box::from(value),
            timestamp,
            tombstone,
        }
    }

    pub fn serialize(&self, key: &[u8], use_variable_encoding: bool) -> Box<[u8]> {
        let mut entry_bytes = Vec::new();
        let mut with_hasher = Vec::new();
        let crc_hasher = Crc::<u32>::new(&CRC_32_ISCSI);

        let timestamp_bytes = if use_variable_encoding { variable_encode(self.timestamp) } else { Box::new(self.timestamp.to_ne_bytes()) };
        entry_bytes.extend(timestamp_bytes.as_ref());

        entry_bytes.extend((self.tombstone as u8).to_ne_bytes());

        let key_len_bytes = if use_variable_encoding { variable_encode(key.len() as u128) } else { Box::new(key.len().to_ne_bytes()) };
        entry_bytes.extend(key_len_bytes.as_ref());

        let mut value_len_bytes = vec![0u8];
        if !self.tombstone {
            value_len_bytes = Vec::from(if use_variable_encoding { variable_encode(self.value.len() as u128) } else { Box::new(self.value.len().to_ne_bytes()) });
            entry_bytes.extend(&value_len_bytes);
        }

        entry_bytes.extend(key.iter());

        if !self.tombstone {
            entry_bytes.extend(self.value.iter());
        }

        let crc = crc_hasher.checksum(&entry_bytes);
        let crc_bytes = if use_variable_encoding { variable_encode(crc as u128) } else { Box::new(crc.to_ne_bytes()) };
        println!("Serializing key {:#?}. Header len: {:#?}", key, crc_bytes.len() + timestamp_bytes.len() + 1 + key_len_bytes.len() + value_len_bytes.len());
        println!("Key len: {:#?}, value len: {:#?}", key.len(), self.value.len());
        with_hasher.extend(crc_bytes.as_ref());
        with_hasher.extend(entry_bytes);
        println!("hasher: {:#?}", with_hasher);
        println!("\n\n");
        with_hasher.into_boxed_slice()
    }

    pub fn deserialize(bytes: &[u8], use_variable_encoding: bool) -> Result<(Box<[u8]>, Self), Box<dyn Error>> {
        let crc_hasher = Crc::<u32>::new(&CRC_32_ISCSI);

        let (crc, timestamp, tombstone, key_len, value_len, mut offset, _) = deserialize_header(bytes, use_variable_encoding);

        let mut key = vec![0u8; key_len].into_boxed_slice();
        key.copy_from_slice(&bytes[offset..offset + key_len]);
        offset += key_len;

        let value = if tombstone {
            vec![].into_boxed_slice()
        } else {
            let mut value = vec![0u8; value_len].into_boxed_slice();
            value.copy_from_slice(&bytes[offset..offset + value_len]);
            value
        };

        let mut bytes: Vec<u8> = Vec::new();

        let timestamp_bytes = if use_variable_encoding { variable_encode(timestamp) } else { Box::new(timestamp.to_ne_bytes()) };
        bytes.extend(timestamp_bytes.as_ref());
        bytes.extend((tombstone as u8).to_ne_bytes());
        let key_len_bytes = if use_variable_encoding { variable_encode(key.len() as u128) } else { Box::new(key.len().to_ne_bytes()) };
        bytes.extend(key_len_bytes.as_ref());
        if !tombstone {
            let value_len_bytes = if use_variable_encoding { variable_encode(value_len as u128) } else { Box::new(value_len.to_ne_bytes()) };
            bytes.extend(value_len_bytes.as_ref());
        }
        bytes.extend(key.as_ref());
        if !tombstone {
            bytes.extend(value.as_ref());
        }

        if crc_hasher.checksum(&bytes) != crc {
            println!("Checksum failed");
            Err(Box::try_from(CRCError(crc)).unwrap())
        } else {
            let entry = MemoryEntry {
                value,
                tombstone,
                timestamp,
            };
            Ok((key, entry))
        }
    }

    pub fn get_value(&self) -> Box<[u8]> {
        self.value.clone()
    }

    pub fn get_tombstone(&self) -> bool {
        self.tombstone
    }

    pub fn get_timestamp(&self) -> u128 {
        self.timestamp
    }

    pub fn set_value(&mut self, value: &[u8]) { self.value = Box::from(value); }

    pub fn set_tombstone(&mut self, tombstone: bool) { self.tombstone = tombstone; }

    pub fn set_timestamp(&mut self, time_stamp: TimeStamp) { self.timestamp = time_stamp.get_time(); }
}

pub fn deserialize_header(bytes: &[u8], use_variable_encoding: bool) -> (u32, u128, bool, usize, usize, usize, usize) {
    let mut offset = 0;

    let (crc_opt, length) = match use_variable_encoding {
        true => variable_decode(bytes),
        false => (Some(u32::from_ne_bytes(bytes[..4].try_into().unwrap()) as u128), 4)
    };
    let crc = crc_opt.unwrap() as u32;
    offset += length;

    let (timestamp_opt, length) = match use_variable_encoding {
        true => variable_decode(&bytes[offset..]),
        false => (Some(u128::from_ne_bytes(bytes[offset..offset + 16].try_into().unwrap())), 16)
    };
    let timestamp = timestamp_opt.unwrap();
    offset += length;

    let tombstone = bytes[offset] != 0;
    offset += 1;

    let offset_to_key_len = offset;

    let (key_len, length) = deserialize_usize_value(&bytes[offset..], use_variable_encoding);
    offset += length;

    let value_len = if tombstone {
        0
    } else {
        let (value_len, length) = deserialize_usize_value(&bytes[offset..], use_variable_encoding);
        offset += length;
        value_len
    };

    return (crc, timestamp, tombstone, key_len, value_len, offset, offset_to_key_len);
}

pub fn deserialize_usize_value(bytes: &[u8], use_variable_encoding: bool) -> (usize, usize) {
    let (value_len_opt, length) = match use_variable_encoding {
        true => variable_decode(bytes),
        false => (Some(usize::from_ne_bytes(bytes[..std::mem::size_of::<usize>()].try_into().unwrap()) as u128), std::mem::size_of::<usize>())
    };
    (value_len_opt.unwrap() as usize, length)
}
