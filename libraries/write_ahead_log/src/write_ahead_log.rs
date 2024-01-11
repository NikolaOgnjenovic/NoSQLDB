use std::io;
use std::path::PathBuf;
use crc::{Crc, CRC_32_ISCSI};
use db_config::DBConfig;
use segment_elements::TimeStamp;
use crate::wal_file::WALFile;

struct WALConfig {
    wal_dir: PathBuf,
    wal_max_entries: usize,
    wal_max_size: usize
}

impl WALConfig {
    pub fn from(dbconfig: &DBConfig) -> Self {
        Self {
            wal_dir: PathBuf::from(&dbconfig.write_ahead_log_dir),
            wal_max_size: dbconfig.write_ahead_log_size,
            wal_max_entries: dbconfig.write_ahead_log_num_of_logs
        }
    }
}

pub struct WriteAheadLog {
    crc_hasher: Crc<u32>,
    config: WALConfig,
    files: Vec<WALFile>
}

impl WriteAheadLog {
    /// Creates a new file with the current time and a WAL that points to it.
    /// Can be used when continuing a journal from a new file or when starting a new journal.
    pub fn new(dbconfig: &DBConfig) -> io::Result<WriteAheadLog> {
        let wal_config = WALConfig::from(dbconfig);

        Ok(Self {
            crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI),
            files: vec![WALFile::build(&wal_config.wal_dir)?],
            config: WALConfig::from(dbconfig)
        })
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], timestamp: TimeStamp) -> io::Result<bool> {
        let mut record_bytes: Vec<u8> = Vec::new();
        record_bytes.extend(timestamp.get_time().to_ne_bytes().as_ref());
        record_bytes.extend((false as u8).to_ne_bytes());
        record_bytes.extend(key.len().to_ne_bytes());
        record_bytes.extend(value.len().to_ne_bytes());
        record_bytes.extend(key);
        record_bytes.extend(value);

        let checksum_bytes = Vec::from(self.crc_hasher.checksum(&record_bytes).to_ne_bytes());

        let complete_bytes = checksum_bytes.into_iter().chain(record_bytes.into_iter()).collect::<Vec<u8>>();

        self.push_all_bytes(complete_bytes)?;

        Ok(true)
    }

    pub fn delete(&mut self, key: &[u8], timestamp: TimeStamp) -> io::Result<bool> {
        let mut record_bytes: Vec<u8> = Vec::new();

        record_bytes.extend(timestamp.get_time().to_ne_bytes().as_ref());
        record_bytes.extend((true as u8).to_ne_bytes());
        record_bytes.extend(key.len().to_ne_bytes());
        record_bytes.extend(0usize.to_ne_bytes().as_ref()); // value len
        record_bytes.extend(key);

        let checksum_bytes = Vec::from(self.crc_hasher.checksum(&record_bytes).to_ne_bytes());

        let complete_bytes = checksum_bytes.into_iter().chain(record_bytes.into_iter()).collect::<Vec<u8>>();

        self.push_all_bytes(complete_bytes)?;

        Ok(true)
    }

    fn push_all_bytes(&mut self, mut bytes: Vec<u8>) -> io::Result<()> {
        let mut last_file = self.files.last_mut().unwrap();

        if last_file.num_entries == self.config.wal_max_entries {
            last_file.close_file();
            self.files.push(WALFile::build(&self.config.wal_dir)?);
            last_file = self.files.last_mut().unwrap();
        }

        while bytes.len() >= self.config.wal_max_size - last_file.current_size {
            let cur_size = last_file.current_size;
            last_file.write_bytes(&bytes[0..(self.config.wal_max_size - cur_size)])?;
            bytes.drain(0..(self.config.wal_max_size - cur_size));
            self.files.push(WALFile::build(&self.config.wal_dir)?);
            last_file = self.files.last_mut().unwrap();
        }

        last_file.write_bytes(&bytes)?;

        Ok(())
    }

    pub fn finalize(self) -> io::Result<()> {
        for file in self.files {
            file.remove_file()?
        }

        Ok(())
    }
}

