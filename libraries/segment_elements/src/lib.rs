mod segment_trait;
mod memory_entry;
mod timestamp;

pub use segment_trait::SegmentTrait;
pub use memory_entry::MemoryEntry;
pub use timestamp::TimeStamp;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_serialization() {
        let entry = MemoryEntry::from(&[1], false, TimeStamp::Now.get_time());
        let (key, new_entry) = match MemoryEntry::deserialize(&entry.serialize(&[1])) {
            Ok(value) => value,
            Err(_e) => return
        };
        assert_eq!(&[1], key.as_ref());
        assert_eq!(entry.get_value(), new_entry.get_value());
        assert_eq!(entry.get_timestamp(), new_entry.get_timestamp());
        assert_eq!(entry.get_tombstone(), new_entry.get_tombstone());
    }

    #[test]
    fn test_serialization_deleted() {
        let entry = MemoryEntry::from(&[1], true, TimeStamp::Now.get_time());
        let (key, new_entry) = match MemoryEntry::deserialize(&entry.serialize(&[1])) {
            Ok(value) => value,
            Err(_e) => return
        };
        assert_eq!(&[1], key.as_ref());
        assert_eq!(&[] as &[u8], new_entry.get_value().as_ref());
        assert_eq!(entry.get_timestamp(), new_entry.get_timestamp());
        assert_eq!(entry.get_tombstone(), new_entry.get_tombstone());
    }

    #[test]
    fn test_serialization_deleted_empty() {
        let entry = MemoryEntry::from(&[], true, TimeStamp::Now.get_time());
        let (key, new_entry) = match MemoryEntry::deserialize(&entry.serialize(&[1])) {
            Ok(value) => value,
            Err(_e) => return
        };
        assert_eq!(&[1], key.as_ref());
        assert_eq!(entry.get_value(), new_entry.get_value());
        assert_eq!(entry.get_timestamp(), new_entry.get_timestamp());
        assert_eq!(entry.get_tombstone(), new_entry.get_tombstone());
    }
}

