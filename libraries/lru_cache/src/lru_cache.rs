use segment_elements::{MemoryEntry, TimeStamp};
use crate::dll_node::{ Entry, Node };
use crate::doubly_linked_list::DoublyLinkedList;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

pub struct LRUCache {
    pub(crate) list: DoublyLinkedList,
    pub(crate) map: HashMap<Box<[u8]>, Rc<RefCell<Node>>>,
    pub(crate) size: usize,
    pub(crate) capacity: usize,
}

impl LRUCache {
    pub fn new(capacity: usize) -> Self {
        LRUCache {
            list: DoublyLinkedList::new(),
            map: HashMap::new(),
            size: 0,
            capacity,
        }
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn get_capacity(&self) -> usize {
        self.capacity
    }

    pub fn get(&mut self, key: &[u8]) -> Option<MemoryEntry> {
        if self.map.contains_key(key) {
            let node = self.map.get(key);
            let value = node.unwrap().borrow().el.mem_entry.clone();
            let prev_node = node.unwrap().as_ref().borrow_mut().prev.take();
            let next_node = node.unwrap().as_ref().borrow_mut().next.take();

            if prev_node.is_some() {
                prev_node.as_ref().unwrap().borrow_mut().next = next_node.clone();
            } else {
                self.list.tail = next_node.clone();
            }

            if next_node.is_some() {
                next_node.as_ref().unwrap().borrow_mut().prev = prev_node;
            } else {
                self.list.head = prev_node;
            }

            self.list.push_head(node.unwrap().borrow().el.clone());
            return Some(value);

        }

        None
    }

    pub fn insert(&mut self, key: &[u8], memory_entry: Option<MemoryEntry>) {
        let entry = if memory_entry.is_some() {
            let memory_entry = memory_entry.unwrap();
            Entry::from(key, memory_entry.get_value().as_ref(), memory_entry.get_tombstone(), TimeStamp::Custom(memory_entry.get_timestamp()))
        } else {
            Entry::from(key, &[], true, TimeStamp::Now)
        };
        if self.map.contains_key(key) {
            let node = self.map.get(key);
            node.unwrap().borrow_mut().el = entry;
            return;
        }
        self.list.push_head(entry);
        let node = self.list.peak_head();
        self.map.insert(Box::from(key), node.unwrap());
        self.size += 1;

        if self.size > self.capacity {
            let popped = self.list.pop_tail();
            self.map.remove(popped.unwrap().borrow().el.key.as_ref());
            self.size -= 1;
        }
    }
}