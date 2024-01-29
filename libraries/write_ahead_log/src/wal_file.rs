use std::fs::{File, OpenOptions, remove_file};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) struct WALFile {
    file: Option<File>,
    file_path: PathBuf,
    pub(crate) current_size: usize,
    pub(crate) num_entries: usize
}

impl WALFile {
    pub(crate) fn build(dir: &Path) -> io::Result<Self> {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let file_path = Path::new(dir).join(time.to_string() + ".log");

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?;

        Ok(Self { file: Some(file), file_path, current_size: 0, num_entries: 0 })
    }

    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<bool> {
        if self.file.is_none() {
            return Ok(false);
        }

        self.current_size += bytes.len();
        self.num_entries += 1;

        self.file.as_mut().unwrap().write_all(bytes)?;
        self.file.as_mut().unwrap().flush()?;

        Ok(true)
    }

    pub(crate) fn close_file(&mut self) {
        if self.file.is_some() {
            self.file = None;
        }
    }

    pub(crate) fn remove_file(mut self) -> io::Result<()> {
        self.close_file();
        remove_file(self.file_path)
    }

    pub(crate) fn get_len(&mut self) -> io::Result<u64> {
        match &self.file {
            Some(file) => Ok(file.metadata()?.len()),
            None => {
                let temp_file = OpenOptions::new().read(true).open(&self.file_path).unwrap();
                Ok(temp_file.metadata()?.len())
            }
        }
    }
}