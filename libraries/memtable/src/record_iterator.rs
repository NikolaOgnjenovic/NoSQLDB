use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read};
use std::path::Path;
use crc::{Crc, CRC_32_ISCSI};
use crate::crc_error::CRCError;

pub(crate) struct Record {
    pub(crate) timestamp: u128,
    pub(crate) tombstone: bool,
    pub(crate) key: Box<[u8]>,
    pub(crate) value: Option<Box<[u8]>>
}

pub(crate) struct RecordIterator {
    reader: BufReader<File>,
    crc_hasher: Crc<u32>
}

impl RecordIterator {
    pub fn new(path: &Path) -> io::Result<RecordIterator> {
        let file = OpenOptions::new().read(true).open(path)?;

        Ok(RecordIterator {
            reader: BufReader::new(file),
            crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI)
        })
    }
}

impl Iterator for RecordIterator {
    type Item = Result<Record, CRCError>;

    fn next(&mut self) -> Option<Self::Item> {
        let crc = {
            let mut crc_buffer = [0u8; 4];
            self.reader.read_exact(&mut crc_buffer).ok()?;
            u32::from_ne_bytes(crc_buffer)
        };

        let timestamp = {
            let mut timestamp_buffer = [0u8; 16];
            self.reader.read_exact(&mut timestamp_buffer).ok()?;
            u128::from_ne_bytes(timestamp_buffer)
        };

        let tombstone = {
            let mut tombstone_buffer = [0u8; 1];
            self.reader.read_exact(&mut tombstone_buffer).ok()?;
            u8::from_ne_bytes(tombstone_buffer) != 0
        };

        let key_size = {
            let mut key_size_buffer = [0u8; 8];
            self.reader.read_exact(&mut key_size_buffer).ok()?;
            usize::from_ne_bytes(key_size_buffer)
        };

        let value_size = {
            let mut value_size_buffer = [0u8; 8];
            self.reader.read_exact(&mut value_size_buffer).ok()?;
            usize::from_ne_bytes(value_size_buffer)
        };

        let mut key = vec![0u8; key_size].into_boxed_slice();
        self.reader.read_exact(&mut key).ok()?;

        let value = if !tombstone {
            let mut value_buf = vec![0u8; value_size].into_boxed_slice();
            self.reader.read_exact(&mut value_buf).ok()?;
            Some(value_buf)
        } else {
            None
        };

        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend(timestamp.to_ne_bytes().as_ref());
        bytes.extend((false as u8).to_ne_bytes());
        bytes.extend(key_size.to_ne_bytes());
        bytes.extend(value_size.to_ne_bytes());
        bytes.extend(key.as_ref());

        if value.is_some() {
            bytes.extend(value.as_deref().unwrap());
        }

        if self.crc_hasher.checksum(&bytes) != crc {
            Some(Err(CRCError(crc)))
        } else {
            Some(Ok(Record {
                timestamp, tombstone, key, value
            }))
        }
    }
}