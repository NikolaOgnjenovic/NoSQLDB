use segment_elements::{MemoryEntry, TimeStamp};

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub(crate) key: Box<[u8]>,
    pub(crate) mem_entry: MemoryEntry,
}

impl Entry {
    pub(crate) fn from(key: &[u8], value: &[u8], tombstone: bool, time_stamp: TimeStamp) -> Self {
        Entry {
            key: Box::from(key),
            mem_entry: MemoryEntry::from(value, tombstone, time_stamp.get_time()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Node {
    pub(crate) el: Entry,
    pub(crate) next: crate::doubly_linked_list::Link,
    pub(crate) prev: crate::doubly_linked_list::Link,
}

impl Node {
    pub(crate) fn new(el: Entry) -> Self {
        Self {
            el,
            next: None,
            prev: None,
        }
    }
}
