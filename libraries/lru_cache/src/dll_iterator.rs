use crate::doubly_linked_list::Link;
use segment_elements::MemoryEntry;

pub struct DLLIterator {
    current: Link,
}

impl DLLIterator {
    pub fn new(current: Link) -> Self {
        DLLIterator {
            current,
        }
    }
}

impl Iterator for DLLIterator {
    type Item = (Box<[u8]>, MemoryEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_some() {
            let entry = self.current.as_ref().unwrap().borrow().el.to_owned();
            let key = entry.key.clone();
            let value = entry.mem_entry.clone();
            let next = self.current.as_ref().unwrap().borrow().next.clone();
            self.current = next;
            return Option::from((key, value));
        }
        None
    }
}