use std::error::Error;
use std::io;
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

    pub fn serialize(&self, key: &[u8]) -> Box<[u8]> {
        let mut entry_bytes = Vec::new();
        let mut with_hasher = Vec::new();
        let crc_hasher = Crc::<u32>::new(&CRC_32_ISCSI);

        entry_bytes.extend(variable_encode(self.timestamp).as_ref());
        entry_bytes.extend((self.tombstone as u8).to_ne_bytes());
        entry_bytes.extend(variable_encode(key.len() as u128).as_ref());
        if !self.tombstone {
            entry_bytes.extend(variable_encode(self.value.len() as u128).as_ref());
        }
        entry_bytes.extend(key.iter());
        if !self.tombstone {
            entry_bytes.extend(self.value.iter());
        }

        with_hasher.extend(variable_encode(crc_hasher.checksum(&entry_bytes) as u128).as_ref());
        with_hasher.extend(entry_bytes);

        with_hasher.into_boxed_slice()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<(Box<[u8]>, Self), Box<dyn Error>> {
        let crc_hasher = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut offset = 0;

        let (crc_opt, length) = variable_decode(bytes);
        let crc = crc_opt.unwrap() as u32;
        offset += length;

        let (timestamp_opt, length) = variable_decode(&bytes[offset..]);
        let timestamp = timestamp_opt.unwrap();
        offset += length;

        let tombstone = bytes[offset] != 0;
        offset += 1;

        let (key_len_opt, length) = variable_decode(&bytes[offset..]);
        let key_len = key_len_opt.unwrap() as usize;
        offset += length;

        let value_len = if tombstone {
            0
        } else {
            let (value_len_opt, length) = variable_decode(&bytes[offset..]);
            offset += length;
            value_len_opt.unwrap() as usize
        };

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

        bytes.extend(variable_encode(timestamp).as_ref());
        bytes.extend((tombstone as u8).to_ne_bytes());
        bytes.extend(variable_encode(key_len as u128).as_ref());
        if !tombstone {
            bytes.extend(variable_encode(value_len as u128).as_ref());
        }
        bytes.extend(key.as_ref());
        if !tombstone {
            bytes.extend(value.as_ref());
        }

        if crc_hasher.checksum(&bytes) != crc {
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

