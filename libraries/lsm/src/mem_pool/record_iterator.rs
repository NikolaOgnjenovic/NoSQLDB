use std::fs::{OpenOptions, read_dir};
use std::io;
use std::io::Read;
use std::path::{Path, PathBuf};
use crc::{Crc, CRC_32_ISCSI};
use crate::mem_pool::crc_error::CRCError;

pub(crate) struct Record {
    pub(crate) timestamp: u128,
    pub(crate) tombstone: bool,
    pub(crate) key: Box<[u8]>,
    pub(crate) value: Option<Box<[u8]>>
}

pub(crate) struct RecordIterator {
    all_read_bytes: Vec<u8>,
    crc_hasher: Crc<u32>,
    data_pointer: usize
}

impl RecordIterator {
    pub fn new(dir: &Path) -> io::Result<RecordIterator> {
        let mut files = read_dir(dir)?
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|file| {
                match file.extension() {
                    Some(ext) => ext == "log",
                    None => false
                }
            })
            .collect::<Vec<PathBuf>>();

        files.sort();

        let mut all_read_bytes = Vec::new();

        for file in files {
            let mut file = OpenOptions::new().read(true).open(file)?;
            file.read_to_end(&mut all_read_bytes)?;
        }

        Ok(RecordIterator {
            all_read_bytes,
            data_pointer: 0,
            crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI)
        })
    }
}

impl Iterator for RecordIterator {
    type Item = Result<Record, CRCError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_pointer >= self.all_read_bytes.len() {
            return None;
        }

         let crc = {
            let crc_buffer = &self.all_read_bytes[self.data_pointer..self.data_pointer + 4];
            u32::from_ne_bytes(crc_buffer.try_into().ok()?)
        };

        self.data_pointer += 4;

        let timestamp = {
            let timestamp_buffer = &self.all_read_bytes[self.data_pointer..self.data_pointer + 16];
            u128::from_ne_bytes(timestamp_buffer.try_into().ok()?)
        };

        self.data_pointer += 16;

        let tombstone = {
            let tombstone_buffer = &self.all_read_bytes[self.data_pointer..self.data_pointer + 1];
            u8::from_ne_bytes(tombstone_buffer.try_into().ok()?) != 0
        };

        self.data_pointer += 1;

        let key_size = {
            let key_size_buffer = &self.all_read_bytes[self.data_pointer..self.data_pointer + 8];
            usize::from_ne_bytes(key_size_buffer.try_into().ok()?)
        };

        self.data_pointer += 8;

        let value_size = {
            let value_size_buffer = &self.all_read_bytes[self.data_pointer..self.data_pointer + 8];
            usize::from_ne_bytes(value_size_buffer.try_into().ok()?)
        };

        self.data_pointer += 8;

        let key = self.all_read_bytes[self.data_pointer..self.data_pointer + key_size].to_vec().into_boxed_slice();

        self.data_pointer += key_size;

        let value = if !tombstone {
            let value_buf = self.all_read_bytes[self.data_pointer..self.data_pointer + value_size].to_vec().into_boxed_slice();

            self.data_pointer += value_size;

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