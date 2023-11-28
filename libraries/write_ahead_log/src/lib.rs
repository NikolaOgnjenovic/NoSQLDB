mod write_ahead_log;

pub use write_ahead_log::WriteAheadLog;

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};
    use std::{fs, io};
    use std::io::{BufReader, Read};
    use std::path::Path;
    use segment_elements::TimeStamp;
    use crate::write_ahead_log::WriteAheadLog;

    fn validate_entry(path: &Path, passed_key: &[u8], passed_value: Option<&[u8]>) -> bool {
        let file = OpenOptions::new().read(true).open(path).unwrap();
        let mut reader: BufReader<File> = BufReader::new(file);

        let crc = {
            let mut crc_buffer = [0u8; 4];
            if reader.read_exact(&mut crc_buffer).is_err() {
                return false;
            }
            u32::from_ne_bytes(crc_buffer)
        };

        let timestamp = {
            let mut timestamp_buffer = [0u8; 16];
            if reader.read_exact(&mut timestamp_buffer).is_err() {
                return false;
            }
            u128::from_ne_bytes(timestamp_buffer)
        };

        let tombstone = {
            let mut tombstone_buffer = [0u8; 1];
            if reader.read_exact(&mut tombstone_buffer).is_err() {
                return false;
            }
            u8::from_ne_bytes(tombstone_buffer) != 0
        };

        let key_size = {
            let mut key_size_buffer = [0u8; 8];
            if reader.read_exact(&mut key_size_buffer).is_err() {
                return false;
            }
            usize::from_ne_bytes(key_size_buffer)
        };

        let value_size = {
            let mut value_size_buffer = [0u8; 8];
            if reader.read_exact(&mut value_size_buffer).is_err() {
                return false;
            }
            usize::from_ne_bytes(value_size_buffer)
        };

        let mut key = vec![0u8; key_size].into_boxed_slice();
        if reader.read_exact(&mut key).is_err() {
            return false;
        }

        let value = if !tombstone {
            let mut value_buf = vec![0u8; value_size].into_boxed_slice();
            if reader.read_exact(&mut value_buf).is_err() {
                return false;
            }
            Some(value_buf)
        } else {
            None
        };

        assert_eq!(*passed_key, *key);
        assert_eq!(passed_value, value.as_deref());
        assert_eq!(passed_key.len(), key.len());

        if None != passed_value {
            assert_eq!(passed_value.unwrap().len(), value.unwrap().len());
        }

        true
    }

    #[test]
    fn test_log_1() -> io::Result<()> {
        let mut wal1 = WriteAheadLog::new(Path::new("./wal_tests/"))?;
        let k1 = b"key1";
        let v1 = b"value1";

        wal1.set(k1, v1, TimeStamp::Now)?;

        wal1.flush().unwrap();

        let paths = fs::read_dir("./wal_tests/").unwrap();

        for path in paths {
            let path = path.unwrap().path();
            assert!(validate_entry(&path, k1, Some(v1)));
        }

        Ok(())
    }
}
