use crate::skip_list_node::Node;
use std::sync::{ Arc, Mutex };

pub struct SkipListIterator {
    pub(crate) current: Option<Arc<Mutex<Node>>>,
}

impl Iterator for SkipListIterator {
    type Item = Arc<Mutex<Node>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next_node) = self.current.take().unwrap().lock().unwrap().next[0].take() {
            self.current = Some(next_node.clone());
            return Option::from(next_node);
        } else {
            return None;
        }
    }
}