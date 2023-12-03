use std::cell::RefCell;
use std::rc::Rc;
use crate::skip_list_node::Node;

pub struct SkipListIterator {
    pub(crate) current: Option<Rc<RefCell<Node>>>,
}

impl Iterator for SkipListIterator {
    type Item = Rc<RefCell<Node>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next_node) = self.current.take().unwrap().borrow_mut().next[0].take() {
            self.current = Some(next_node.clone());
            return Option::from(next_node);
        } else {
            return None;
        }
    }
}