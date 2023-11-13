use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read};
use std::path::PathBuf;

#[derive(Debug)]
pub struct CRCError(u32);

impl std::fmt::Display for CRCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CRC of record {} does not match", self.0)
    }
}

impl std::error::Error for CRCError {}

pub struct WALRecord {
    pub(crate) crc: u32,
    pub(crate) timestamp: u128,
    pub(crate) tombstone: bool,
    pub(crate) key_size: usize,
    pub(crate) value_size: usize,
    pub(crate) key: Box<[u8]>,
    pub(crate) value: Option<Box<[u8]>>
}

pub struct WALIterator {
    reader: BufReader<File>
}

impl WALIterator {
    pub fn new(path: PathBuf) -> io::Result<WALIterator> {
        let file = OpenOptions::new().read(true).open(path)?;

        Ok(WALIterator {
            reader: BufReader::new(file)
        })
    }
}

impl Iterator for WALIterator {
    type Item = Result<WALRecord, CRCError>;

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

        bytes
            .extend(timestamp.get_time().to_ne_bytes().as_ref())
            .extend((false as u8).to_ne_bytes())
            .extend(key.len().to_ne_bytes())
            .extend(value.len().to_ne_bytes())
            .extend(key.as_ref())
            .extend(value.as_ref());


        // todo make crc_hasher in the memtable when this iterator is fully implemented
        // todo and compare to local crc and throw a CRCError
        // crc_hasher.checksum(&bytes).to_ne_bytes().as_ref() != crc => Err(CRCError(crc))

        Some(Ok(WALRecord {
            crc, timestamp, tombstone, key_size, value_size, key, value
        }))
    }
}