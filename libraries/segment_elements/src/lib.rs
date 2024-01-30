mod segment_trait;
mod memory_entry;
mod timestamp;
mod hashmap_impl;
mod hashmap_iterator;
pub mod crc_error;

pub use segment_trait::SegmentTrait;
pub use memory_entry::MemoryEntry;
pub use timestamp::TimeStamp;
pub use hashmap_impl::MemEntryHashMap;

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

#[cfg(test)]
mod tests_hashmap_impl {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut map = MemEntryHashMap::new();
        let key = [1, 2, 3];
        let value = [4, 5, 6];
        let timestamp = TimeStamp::Now;

        assert!(map.insert(&key, &value, timestamp));

        assert_eq!(map.get(&key), Some(value.to_vec().into_boxed_slice()));
    }

    #[test]
    fn test_delete() {
        let mut map = MemEntryHashMap::new();
        let key = [1, 2, 3];
        let timestamp = TimeStamp::Now;

        assert!(map.insert(&key, &[4, 5, 6], timestamp));

        assert!(!map.delete(&key, timestamp));

        assert_eq!(map.get(&key), Some(Box::from(&[] as &[u8])));
    }

    #[test]
    fn test_iterator() {
        let mut map = MemEntryHashMap::new();
        let key1 = &[1, 2, 3];
        let key2 = &[4, 5, 6];
        let timestamp = TimeStamp::Now;

        assert!(map.insert(key2, &[10, 11, 12], timestamp));
        assert!(map.insert(key1, &[7, 8, 9], timestamp));

        let mut iter = map.iterator();

        let iter_key1 = iter.next().unwrap().0;
        let iter_key2 = iter.next().unwrap().0;

        assert_eq!(&*iter_key1, key1);
        assert_eq!(&*iter_key2, key2);
        assert_eq!(iter.next(), None);
    }
}

