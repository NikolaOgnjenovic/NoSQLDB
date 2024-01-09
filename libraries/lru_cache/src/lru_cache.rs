use segment_elements::{MemoryEntry, TimeStamp};
use crate::dll_node::{ Entry, Node };
use crate::doubly_linked_list::DoublyLinkedList;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

//todo capacity needs to be in config file


pub(crate) struct LRUCache {
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
    pub fn read(&mut self, key: &[u8]) -> Option<Box<[u8]>> {
        if self.map.contains_key(key) {
            let node = self.map.get(key);
            let value = node.unwrap().borrow().get_el().mem_entry.get_value();
            let prev_node = &node.unwrap().as_ref().borrow().prev;
            let next_node = &node.unwrap().as_ref().borrow().next;

            if prev_node.is_some() {
                prev_node.as_ref().unwrap().borrow_mut().next = node.unwrap().as_ref().borrow().next.clone();
            } else {
                self.list.head = node.unwrap().as_ref().borrow().next.clone()
            }

            if next_node.is_some() {
                next_node.as_ref().unwrap().borrow_mut().prev = node.unwrap().as_ref().borrow().prev.clone();
            } else {
                self.list.tail = node.unwrap().as_ref().borrow().prev.clone();
            }

            self.list.push_head(node.unwrap().borrow().get_el().clone());
            self.list.size -= 1;
            return Some(value);

        }
        None
    }

    pub fn add(&mut self, key: &[u8], value: &[u8], tombstone: bool, time_stamp: TimeStamp) {
        let entry = Entry::from(key, value, tombstone, time_stamp);
        self.list.push_head(entry);
        let node = self.list.peak_head();
        self.map.insert(Box::from(key), node.unwrap());
        self.size += 1;

        if self.size >= self.capacity {
            let popped = self.list.pop_tail();
            self.map.remove(popped.unwrap().borrow().get_el().key.as_ref());
            self.size -= 1;
        }
    }

    pub fn update(&mut self, key: &[u8], value: &[u8], tombstone: bool, time_stamp: TimeStamp) {
        if self.map.contains_key(key) {
            let node = self.map.get(key);
            node.unwrap().borrow_mut().el = Entry::from(key, value, tombstone, time_stamp);
        }
    }
}