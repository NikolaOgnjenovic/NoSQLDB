use crate::wal_byte_index::WALByteIndex;
use crate::wal_file::WALFile;
use crc::{Crc, CRC_32_ISCSI};
use db_config::DBConfig;
use segment_elements::TimeStamp;
use std::collections::VecDeque;
use std::fs::read_dir;
use std::path::PathBuf;
use std::{fs, io};

struct WALConfig {
    wal_dir: PathBuf,
    wal_max_entries: usize,
    wal_max_size: usize,
}

impl WALConfig {
    pub fn from(dbconfig: &DBConfig) -> Self {
        Self {
            wal_dir: PathBuf::from(&dbconfig.write_ahead_log_dir),
            wal_max_size: dbconfig.write_ahead_log_size,
            wal_max_entries: dbconfig.write_ahead_log_num_of_logs,
        }
    }
}

pub struct WriteAheadLog {
    crc_hasher: Crc<u32>,
    config: WALConfig,
    last_byte_file: WALByteIndex,
    files: VecDeque<WALFile>,
}

impl WriteAheadLog {
    /// Creates a new file with the current time and a WAL that points to it.
    /// Can be used when continuing a journal from a new file or when starting a new journal.
    pub fn new(dbconfig: &DBConfig) -> io::Result<WriteAheadLog> {
        let wal_config = WALConfig::from(dbconfig);

        // Create a directory if it doesn't exist
        fs::create_dir_all(&wal_config.wal_dir)?;

        Ok(Self {
            crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI),
            files: VecDeque::from(vec![WALFile::build(&wal_config.wal_dir)?]),
            last_byte_file: WALByteIndex::open(&wal_config.wal_dir)?,
            config: WALConfig::from(dbconfig),
        })
    }

    pub fn from_dir(dbconfig: &DBConfig) -> io::Result<WriteAheadLog> {
        let wal_config = WALConfig::from(dbconfig);

        let mut files = VecDeque::new();

        match read_dir(&dbconfig.write_ahead_log_dir) {
            Ok(dir) => {
                let mut sorted_dirs: Vec<PathBuf> = dir
                    .map(|dir_entry| dir_entry.unwrap().path())
                    .filter(|file| match file.extension() {
                        Some(ext) => ext == "log",
                        None => false,
                    })
                    .collect();
                sorted_dirs.sort();
                for path_buf in sorted_dirs {
                    files.push_back(WALFile::open(path_buf)?);
                }
            }
            Err(_) => (),
        };

        Ok(Self {
            crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI),
            files,
            last_byte_file: WALByteIndex::open(&wal_config.wal_dir)?,
            config: WALConfig::from(dbconfig),
        })
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], timestamp: TimeStamp) -> io::Result<()> {
        let mut record_bytes: Vec<u8> = Vec::new();
        record_bytes.extend(timestamp.get_time().to_ne_bytes().as_ref());
        record_bytes.extend((false as u8).to_ne_bytes());
        record_bytes.extend(key.len().to_ne_bytes());
        record_bytes.extend(value.len().to_ne_bytes());
        record_bytes.extend(key);
        record_bytes.extend(value);

        let checksum_bytes = Vec::from(self.crc_hasher.checksum(&record_bytes).to_ne_bytes());

        let complete_bytes = checksum_bytes
            .into_iter()
            .chain(record_bytes)
            .collect::<Vec<u8>>();

        self.push_all_bytes(complete_bytes)?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: TimeStamp) -> io::Result<()> {
        let mut record_bytes: Vec<u8> = Vec::new();

        record_bytes.extend(timestamp.get_time().to_ne_bytes().as_ref());
        record_bytes.extend((true as u8).to_ne_bytes());
        record_bytes.extend(key.len().to_ne_bytes());
        record_bytes.extend(0usize.to_ne_bytes().as_ref()); // value len
        record_bytes.extend(key);

        let checksum_bytes = Vec::from(self.crc_hasher.checksum(&record_bytes).to_ne_bytes());

        let complete_bytes = checksum_bytes
            .into_iter()
            .chain(record_bytes)
            .collect::<Vec<u8>>();

        self.push_all_bytes(complete_bytes)?;

        Ok(())
    }

    fn push_all_bytes(&mut self, mut bytes: Vec<u8>) -> io::Result<()> {
        let mut last_file = self.files.back_mut().unwrap();

        if last_file.num_entries == self.config.wal_max_entries {
            last_file.close_file();
            self.files.push_back(WALFile::build(&self.config.wal_dir)?);
            last_file = self.files.back_mut().unwrap();
        }

        while bytes.len() >= self.config.wal_max_size - last_file.current_size {
            let cur_size = last_file.current_size;
            last_file.write_bytes(&bytes[0..(self.config.wal_max_size - cur_size)])?;
            bytes.drain(0..(self.config.wal_max_size - cur_size));
            self.files.push_back(WALFile::build(&self.config.wal_dir)?);
            last_file = self.files.back_mut().unwrap();
        }

        last_file.write_bytes(&bytes)?;

        Ok(())
    }

    pub fn remove_logs_until(&mut self, byte: usize) -> io::Result<()> {
        self.last_byte_file.add(byte)?;

        let mut file_num = 1;
        let mut subtract_bytes = 0;
        let byte_index = self.last_byte_file.get();

        while file_num * self.config.wal_max_size < byte_index {
            let mut file = self.files.pop_front().unwrap();
            subtract_bytes += file.get_len()?;
            file.remove_file()?;
            file_num += 1;
        }

        self.last_byte_file
            .set(byte_index - subtract_bytes as usize)?;
        Ok(())
    }

    pub fn close(mut self) {
        for file in &mut self.files {
            file.close_file();
        }

        self.last_byte_file.close();
    }
}
