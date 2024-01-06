use crate::TimeStamp;
use crc::{Crc, CRC_32_ISCSI};

/// Public struct that SegmentTrait implementations return on get.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct MemoryEntry {
    value: Box<[u8]>,
    tombstone: bool,
    timestamp: u128
}

impl MemoryEntry {
    pub fn from(value: &[u8], tombstone: bool, timestamp: u128) -> Self {
        MemoryEntry {
            value: Box::from(value),
            timestamp,
            tombstone
        }
    }

    pub fn serialize(&self, key: &[u8]) -> Box<[u8]> {
        let mut entry_bytes = Vec::new();
        let mut with_hasher = Vec::new();
        let crc_hasher = Crc::<u32>::new(&CRC_32_ISCSI);

        entry_bytes.extend(self.timestamp.to_ne_bytes());
        entry_bytes.extend((self.tombstone as u8).to_ne_bytes());
        entry_bytes.extend(key.len().to_ne_bytes());
        entry_bytes.extend(self.value.len().to_ne_bytes());
        entry_bytes.extend(key.iter());
        entry_bytes.extend(self.value.iter());

        with_hasher.extend(crc_hasher.checksum(&entry_bytes).to_ne_bytes().as_ref());
        with_hasher.extend(entry_bytes);

        with_hasher.into_boxed_slice()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<(Box<[u8]>, Self), &str> {
        let crc_hasher = Crc::<u32>::new(&CRC_32_ISCSI);

        let crc = {
            let mut crc_bytes = [0u8; 4];
            crc_bytes.copy_from_slice(&bytes[0..4]);
            u32::from_ne_bytes(crc_bytes)
        };

        let timestamp = {
            let mut timestamp_bytes = [0u8; 16];
            timestamp_bytes.copy_from_slice(&bytes[4..20]);
            u128::from_ne_bytes(timestamp_bytes)
        };

        let tombstone = {
            let mut tombstone_bytes = [0u8; 1];
            tombstone_bytes.copy_from_slice(&bytes[20..21]);
            u8::from_ne_bytes(tombstone_bytes) != 0
        };

        let key_len = {
            let mut key_len_bytes = [0u8; 8];
            key_len_bytes.copy_from_slice(&bytes[21..29]);
            usize::from_ne_bytes(key_len_bytes)
        };

        let value_len = {
            let mut value_len_bytes = [0u8; 8];
            value_len_bytes.copy_from_slice(&bytes[29..37]);
            usize::from_ne_bytes(value_len_bytes)
        };

        let mut key = vec![0u8; key_len].into_boxed_slice();
        key.copy_from_slice(&bytes[37..37+key_len]);

        let value = {
            let mut value = vec![0u8; value_len].into_boxed_slice();
            value.copy_from_slice(&bytes[37+key_len..37+key_len+value_len]);
            value
        };

        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend(timestamp.to_ne_bytes().as_ref());
        bytes.extend((false as u8).to_ne_bytes());
        bytes.extend(key_len.to_ne_bytes());
        bytes.extend(value_len.to_ne_bytes());
        bytes.extend(key.as_ref());
        bytes.extend(value.as_ref());


        if crc_hasher.checksum(&bytes) != crc {
            Err("Invalid data, crc doesn't match")
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

