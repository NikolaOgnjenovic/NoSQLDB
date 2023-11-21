use crate::b_tree_node::{Node, Entry};
use crate::order_error::OrderError;

pub struct BTree {
    root: Option<Node>,
    order: usize
}

impl BTree {
    pub fn new(order: usize) -> Result<Self, OrderError> {
        if order <= 1 {
            Err(OrderError)
        } else {
            Ok(BTree {
                root: None,
                order
            })
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.root.as_ref()?.get(key)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        match self.root.take() {
            None => {
                let mut new_root = Node::new(self.order, true);
                new_root.entries[0] = Some(Entry::from(key, value));
                new_root.n = 1;

                self.root = Some(new_root);
            },
            Some(root) => {
                if root.n == (2 * self.order - 1) {
                    // making a new node
                    let mut s = Node::new(self.order, false);
                    s.children[0] = Some(root);
                    s.split_children(0);
                    let mut index = 0;
                    if key > &s.entries[0].as_ref().unwrap().key {
                        index += 1;
                    }
                    s.children[index].as_mut().unwrap().insert_non_full(key, value);
                    self.root = Some(s);
                } else {
                    // filling up the root node
                    self.root = Some(root);
                    self.root.as_mut().unwrap().insert_non_full(key, value);
                }
            }
        }
    }

    pub fn print_tree(&self) {
        self.root.as_ref().unwrap().print_node(0);
    }
}