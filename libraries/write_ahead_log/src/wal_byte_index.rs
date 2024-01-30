use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct WALByteIndex {
    file: Option<File>,
    current_value: usize
}

impl WALByteIndex {
    pub(crate) fn open(dir: &Path) -> io::Result<Self> {
        let file_path = dir.join("byte_index.num");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        let mut val = Self {
            file: Some(file),
            current_value: 0
        };

        let mut byte_buffer = [0u8; 8];
        val.file.as_mut().unwrap().read_exact(&mut byte_buffer).ok();
        val.current_value = usize::from_ne_bytes(byte_buffer);

        Ok(val)
    }

    pub(crate) fn add(&mut self, value: usize) -> io::Result<()> {
        self.current_value += value;

        self.set(self.current_value)?;

        Ok(())
    }

    pub(crate) fn set(&mut self, byte: usize) -> io::Result<()> {
        self.current_value = byte;

        if self.file.is_none() {
            return Ok(());
        }

        self.file.as_mut().unwrap().seek(SeekFrom::Start(0)).map(|_| ())?;
        self.file.as_mut().unwrap().write_all(&byte.to_ne_bytes())?;
        self.file.as_mut().unwrap().flush()
    }

    pub(crate) fn get(&mut self) -> usize {
        if self.file.is_none() {
            0
        } else {
            self.current_value
        }
    }

    pub(crate) fn close(&mut self) {
        if self.file.is_some() {
            self.file = None;
        }
    }
}
