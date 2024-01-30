use std::fs::{OpenOptions, read_dir};
use std::io;
use std::io::Read;
use std::path::{Path, PathBuf};
use crc::{Crc, CRC_32_ISCSI};
use segment_elements::crc_error::CRCError;

pub(crate) struct Record {
    pub(crate) timestamp: u128,
    pub(crate) tombstone: bool,
    pub(crate) key: Box<[u8]>,
    pub(crate) value: Option<Box<[u8]>>
}

pub(crate) struct RecordIterator {
    files: Vec<PathBuf>,
    read_bytes: Vec<u8>,
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

        files.sort_by(|a, b| b.cmp(a));

        let mut all_read_bytes = Vec::new();

        let starting_byte = {
            let mut byte_file = OpenOptions::new().read(true).open(dir.join("byte_index.num"))?;
            let mut byte_buffer = [0u8; 8];
            byte_file.read_exact(&mut byte_buffer).ok();
            usize::from_ne_bytes(byte_buffer)
        };

        let mut iterator = RecordIterator {
            files,
            read_bytes: all_read_bytes,
            data_pointer: starting_byte,
            crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI)
        };

        iterator.read_at_least(starting_byte)?;

        Ok(iterator)
    }

    fn read_next_file(&mut self) -> io::Result<Option<usize>> {
        match self.files.pop() {
            Some(file) => Ok(Some(
                OpenOptions::new()
                    .read(true)
                    .open(file)?
                    .read_to_end(&mut self.read_bytes)?
            )),
            None => Ok(None)
        }
    }

    fn read_at_least(&mut self, num_bytes: usize) -> io::Result<()> {
        // In case we already have the bytes in the buffer
        if self.data_pointer + num_bytes <= self.read_bytes.len() {
            return Ok(())
        }

        let mut already_read_bytes = 0;

        while let Some(num_read_bytes) = self.read_next_file()? {
            already_read_bytes += num_read_bytes;

            if already_read_bytes >= num_bytes {
                break;
            }
        }

        Ok(())
    }
}

impl Iterator for RecordIterator {
    type Item = Result<Record, CRCError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_at_least(37).ok()?;

        if self.data_pointer >= self.read_bytes.len() {
            return None;
        }

         let crc = {
            let crc_buffer = &self.read_bytes[self.data_pointer..self.data_pointer + 4];
            u32::from_ne_bytes(crc_buffer.try_into().ok()?)
        };

        self.data_pointer += 4;

        let timestamp = {
            let timestamp_buffer = &self.read_bytes[self.data_pointer..self.data_pointer + 16];
            u128::from_ne_bytes(timestamp_buffer.try_into().ok()?)
        };

        self.data_pointer += 16;

        let tombstone = {
            let tombstone_buffer = &self.read_bytes[self.data_pointer..self.data_pointer + 1];
            u8::from_ne_bytes(tombstone_buffer.try_into().ok()?) != 0
        };

        self.data_pointer += 1;

        let key_size = {
            let key_size_buffer = &self.read_bytes[self.data_pointer..self.data_pointer + 8];
            usize::from_ne_bytes(key_size_buffer.try_into().ok()?)
        };

        self.data_pointer += 8;

        let value_size = {
            let value_size_buffer = &self.read_bytes[self.data_pointer..self.data_pointer + 8];
            usize::from_ne_bytes(value_size_buffer.try_into().ok()?)
        };

        self.data_pointer += 8;

        self.read_at_least(key_size).ok()?;

        let key = self.read_bytes[self.data_pointer..self.data_pointer + key_size].to_vec().into_boxed_slice();

        self.data_pointer += key_size;

        let value = if !tombstone {
            self.read_at_least(value_size).ok()?;

            let value_buf = self.read_bytes[self.data_pointer..self.data_pointer + value_size].to_vec().into_boxed_slice();

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