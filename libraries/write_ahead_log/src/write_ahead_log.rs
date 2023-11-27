use std::fs::{File, OpenOptions, read_dir};
use std::io;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use crc::{Crc, CRC_32_ISCSI};
use crate::wal_builder::{WALIterator, WALRecord};

pub enum TimeStamp {
    Now,
    Custom(u128)
}

impl TimeStamp {
    pub fn get_time(self) -> u128 {
        match self {
            TimeStamp::Now => SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros(),
            TimeStamp::Custom(custom_time) => custom_time
        }
    }
}

pub struct WriteAheadLog {
    pub(crate) crc_hasher: Crc<u32>,
    pub(crate) file_path: PathBuf,
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

        Ok(Self { crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI), file_path, file })
    }

    /// Opens a path as an append file and continues the WAL log there.
    pub fn from_path(path: &Path) -> io::Result<WriteAheadLog> {
        let file = BufWriter::new(OpenOptions::new()
            .append(true)
            .open(path)?
        );

        Ok(Self { crc_hasher: Crc::<u32>::new(&CRC_32_ISCSI), file_path: path.to_owned(), file })
    }

    // todo method actually needs to be in the memtable structure because it reconstructs it, not the wal
    /// Loads and merges every WAL from one directory.
    pub fn load_from_dir(dir: &Path) -> io::Result<WriteAheadLog> {
        let mut files = read_dir(dir)?
            .map(|file| file.unwrap().path())
            .filter(|file| file.extension().unwrap() == ".log")
            .collect::<Vec<PathBuf>>();
        files.sort();

        let mut newest_wal = WriteAheadLog::new(dir)?;

        for file in files.iter() {
            if let Ok(loaded_wal) = WriteAheadLog::from_path(file) {
                for entry in loaded_wal.into_iter() {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(e) => {
                            eprintln!("Error: Record was modified");
                            continue
                        }
                    };

                    if entry.tombstone {
                        newest_wal.delete(&entry.key, TimeStamp::Custom(entry.timestamp))?;
                    } else {
                        newest_wal.set(&entry.key, &entry.value.unwrap(), TimeStamp::Custom(entry.timestamp))?;
                    }
                }
            }
        }

        newest_wal.flush().unwrap();

        Ok(newest_wal)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: TimeStamp) -> io::Result<()> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes
            .extend(timestamp.get_time().to_ne_bytes().as_ref())
            .extend((false as u8).to_ne_bytes())
            .extend(key.len().to_ne_bytes())
            .extend(value.len().to_ne_bytes())
            .extend(key)
            .extend(value);


        self.file.write_all(self.crc_hasher.checksum(&bytes).to_ne_bytes().as_ref())?;
        self.file.write_all(&bytes)?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: TimeStamp) -> io::Result<()> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes
            .extend(timestamp.get_time().to_ne_bytes().as_ref())
            .extend((true as u8).to_ne_bytes())
            .extend(key.len().to_ne_bytes())
            .extend(0u64.to_ne_bytes().as_ref())// value len
            .extend(key);

        self.file.write_all(self.crc_hasher.checksum(&bytes).to_ne_bytes().as_ref())?;
        self.file.write_all(&bytes)?;

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

// todo iterator should be implemented on the memtable
impl IntoIterator for WriteAheadLog {
    type Item = WALRecord;
    type IntoIter = WALIterator;

    fn into_iter(self) -> Self::IntoIter {
        WALIterator::new(self.file_path).unwrap()
    }
}