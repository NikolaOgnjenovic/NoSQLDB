use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use crc::{Crc, CRC_32_ISCSI};
use segment_elements::TimeStamp;

pub struct WriteAheadLog {
    pub(crate) crc_hasher: Crc<u32>,
    pub(crate) file: BufWriter<File>
}

impl WriteAheadLog {
    /// Creates a new file with the current time and a WAL that points to it.
    /// Can be used when continuing a journal from a new file or when starting a new journal.
    pub fn new(dir: &Path) -> io::Result<WriteAheadLog> {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let file_path = Path::new(dir).join(time.to_string() + ".log");

        let file = BufWriter::new(OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?
        );

        Ok(Self { crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI), file })
    }

    /// Opens a path as an append file and continues the WAL log there.
    pub fn from_path(path: &Path) -> io::Result<WriteAheadLog> {
        let file = BufWriter::new(OpenOptions::new()
            .append(true)
            .open(path)?
        );

        Ok(Self { crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI), file })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: TimeStamp) -> io::Result<()> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend(timestamp.get_time().to_ne_bytes().as_ref());
        bytes.extend((false as u8).to_ne_bytes());
        bytes.extend(key.len().to_ne_bytes());
        bytes.extend(value.len().to_ne_bytes());
        bytes.extend(key);
        bytes.extend(value);

        self.file.write_all(self.crc_hasher.checksum(&bytes).to_ne_bytes().as_ref())?;
        self.file.write_all(&bytes)?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: TimeStamp) -> io::Result<()> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend(timestamp.get_time().to_ne_bytes().as_ref());
        bytes.extend((true as u8).to_ne_bytes());
        bytes.extend(key.len().to_ne_bytes());
        bytes.extend(0u64.to_ne_bytes().as_ref()); // value len
        bytes.extend(key);

        self.file.write_all(self.crc_hasher.checksum(&bytes).to_ne_bytes().as_ref())?;
        self.file.write_all(&bytes)?;

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}