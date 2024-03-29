use crate::skip_list_node::Node;
use segment_elements::MemoryEntry;
use std::sync::{Arc, Mutex};

pub struct SkipListIterator {
    pub(crate) current: Option<Arc<Mutex<Node>>>,
}

impl Iterator for SkipListIterator {
    type Item = (Box<[u8]>, MemoryEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next_node) = self.current.take().unwrap().lock().unwrap().next[0].clone() {
            let key = next_node.lock().as_ref().unwrap().key.clone().unwrap();
            let memory_entry = next_node.lock().as_ref().unwrap().value.clone().unwrap();
            self.current = Some(next_node);

            Option::from((key, memory_entry))
        } else {
            None
        }
    }
}
