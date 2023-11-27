use std::cell::RefCell;
use std::rc::Rc;
use segment_elements::MemoryEntry;

pub(crate) type Link = Option<Rc<RefCell<Node>>>;

#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) key: Option<Box<[u8]>>,
    pub(crate) value: Option<MemoryEntry>,
    pub(crate) next: Vec<Link>,
    pub(crate) level: usize,
}

impl Node {
    pub fn new(key: Option<Box<[u8]>>, value: Option<MemoryEntry>, level: usize, max_level: usize) -> Self {
        Node {
            key,
            value,
            next: vec![None; max_level],
            level
        }
    }

    pub fn get_key(&self) -> &[u8] {
        self.key.as_ref().unwrap()
    }

    pub fn get_val(&self) -> &MemoryEntry {
        self.value.as_ref().unwrap()
    }
}