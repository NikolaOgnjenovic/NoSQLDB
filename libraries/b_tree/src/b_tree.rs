use crate::b_tree_node::{Node, Entry};
use crate::order_error::OrderError;

/// BTree for keeping arbitrary key and value bytes.
pub struct BTree {
    root: Option<Node>,
    order: usize
}

impl BTree {
    /// Creates a new BTree. Returns an error if order is below 2.
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

    /// Returns the value of some key if it exists.
    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.root.as_ref()?.get(key)
    }

    /// Inserts a key with the corresponding value into the BTree.
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
                    let mut new_root = Node::new(self.order, false);
                    new_root.children[0] = Some(root);
                    new_root.split_children(0);

                    // choose whether the second child receives the new key, if false the key is given to the first
                    let second = key > &new_root.entries[0].as_ref().unwrap().key;
                    new_root.children[second as usize].as_mut().unwrap().insert_non_full(key, value);

                    self.root = Some(new_root);
                } else {
                    // filling up the root node
                    self.root = Some(root);
                    self.root.as_mut().unwrap().insert_non_full(key, value);
                }
            }
        }
    }

    /// Removes a key from BTree if it exists
    pub fn remove(&mut self, key: &[u8]) {

        if self.root.is_none() {
            return;
        }

        self.root.as_mut().unwrap().remove(key);

        //if root has 0 keys make its first child new root
        //if it doesn't have a child set it to None
        if self.root.as_ref().unwrap().n == 0
        {
            if self.root.as_ref().unwrap().is_leaf {
                self.root = None;
            } else {
                self.root = self.root.as_mut().unwrap().children[0].take();
            }
        }

    }

    // temp function, will be removed
    pub fn print_tree(&self) {
        self.root.as_ref().unwrap().print_node(0);
    }
}