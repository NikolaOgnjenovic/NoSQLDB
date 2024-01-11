use std::fs::{File, OpenOptions, remove_file};
use std::io;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) struct WALFile {
    writer: Option<BufWriter<File>>,
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

        let writer = BufWriter::new(OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?
        );

        Ok(Self { writer: Some(writer), file_path, current_size: 0, num_entries: 0 })
    }

    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<bool> {
        if self.writer.is_none() {
            return Ok(false);
        }

        self.current_size += bytes.len();
        self.num_entries += 1;

        self.writer.as_mut().unwrap().write_all(bytes)?;
        self.writer.as_mut().unwrap().flush()?;

        Ok(true)
    }

    pub(crate) fn close_file(&mut self) {
        if self.writer.is_some() {
            self.writer = None;
        }
    }

    pub(crate) fn remove_file(mut self) -> io::Result<()> {
        self.close_file();
        remove_file(self.file_path)
    }
}