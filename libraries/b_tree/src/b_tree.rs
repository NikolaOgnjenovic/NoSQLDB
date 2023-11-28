use segment_elements::TimeStamp;
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

    /// Permanently removes a key from BTree if it exists.
    pub fn delete_permanent(&mut self, key: &[u8]) {
        if self.root.is_none() {
            return;
        }

        self.root.as_mut().unwrap().remove(key);

        // if root has 0 keys make it's first child new root
        // if it doesn't have a child set it to None
        if self.root.as_ref().unwrap().n == 0 {
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

impl segment_elements::SegmentTrait for BTree {
    /// Inserts or updates a key with the corresponding value into the BTree.
    // todo add update functionality
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) {
        match self.root.take() {
            None => {
                let mut new_root = Node::new(self.order, true);
                new_root.entries[0] = Some(Entry::from(key, value, false, time_stamp));
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
                    new_root.children[second as usize].as_mut().unwrap().insert_non_full(key, value, false, time_stamp);

                    self.root = Some(new_root);
                } else {
                    // filling up the root node
                    self.root = Some(root);
                    self.root.as_mut().unwrap().insert_non_full(key, value, false, time_stamp);
                }
            }
        }
    }

    // todo impl logical delete, with tombstone = true and time_stamp
    // todo returns true if successfully deleted
    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {

        self.root.as_mut().unwrap().logical_deletion(key, time_stamp)
    }

    /// Returns the value of some key if it exists.
    fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.root.as_ref()?.get(key)
    }

    fn empty(&mut self) {
        self.root = None;
    }
}
