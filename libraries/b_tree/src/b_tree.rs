use segment_elements::{MemoryEntry, SegmentTrait, TimeStamp};
use crate::b_tree_node::{Node, Entry};
use crate::b_tree_iterator::BTreeIterator;
use crate::order_error::OrderError;
use bloom_filter::BloomFilter;

/// BTree for keeping arbitrary key and value bytes.
pub struct BTree {
    root: Option<Node>,
    order: usize,
    length: usize,
}

impl BTree {
    /// Creates a new BTree. Returns an error if order is below 2.
    pub fn new(order: usize) -> Result<Self, OrderError> {
        if order <= 1 {
            Err(OrderError)
        } else {
            Ok(BTree {
                root: None,
                order,
                length:0,
            })
        }
    }

    pub fn size(&self) -> usize {
        self.length
    }

    /// Permanently removes a key from BTree if it exists.
    pub fn delete_permanent(&mut self, key: &[u8]) {
        if self.root.is_none() {
            return;
        }
        if self.get(key).is_some() {
            self.root.as_mut().unwrap().remove(key);
            self.length -= 1;
        }
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

    ///Returns Option<Iterator> for BTree that yields sorted (Key, MemEntry) pairs
    /// The value is Some if length > 0 otherwise None
    pub fn iter(&self) -> Option<BTreeIterator> {
        if self.length > 0 {
            let mut stack = Vec::new();
            let mut entry_stack = Vec::new();
            if let Some(root) = self.root.as_ref() {
                stack.push(root);
                entry_stack.push(0);
            }
            let mut iterator = BTreeIterator {
                stack,
                entry_stack,
            };

            iterator.find_leftmost_child();
            return Some(iterator);
        }
        None
    }

}

impl SegmentTrait for BTree {
    /// Inserts or updates a key with the corresponding value into the BTree.
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool {
        if self.get(key).is_some() {
            self.root.as_mut().unwrap().update(key, value, time_stamp);
            return false;
        }

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
        self.length += 1;
        true
    }

    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        if self.get(key).is_some(){
            self.root.as_mut().unwrap().logical_deletion(key, time_stamp)
        } else {
            self.insert(key, &[], time_stamp);
            true
        }

    }

    fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.root.as_ref()?.get(key)
    }

    fn serialize(&self) -> Box<[u8]> {
        let mut ss_table_bytes = vec![];
        let mut data_bytes:Vec<u8> = vec![];
        let mut index_bytes = vec![];
        let mut offset = 0;
        let mut filter = BloomFilter::new(0.01, self.length);

        let iterable = self.iter();
        if let Some(iterator) = iterable {
            for entry in iterator {
                let key = entry.0;
                let memory_entry = entry.1;
                let entry_bytes = memory_entry.serialize(&key);
                data_bytes.extend(entry_bytes.iter());

                //index structure contains key_len(8 bytes), key and offset in data block(8 bytes)
                index_bytes.extend(usize::to_ne_bytes(key.len()));
                index_bytes.extend(key.iter());
                index_bytes.extend(usize::to_ne_bytes(offset));

                filter.add(&key);

                offset += entry_bytes.len();
            }

            let filter_bytes = filter.serialize();

            ss_table_bytes.extend(usize::to_ne_bytes(data_bytes.len()));
            ss_table_bytes.extend(data_bytes);
            ss_table_bytes.extend(usize::to_ne_bytes(index_bytes.len()));
            ss_table_bytes.extend(index_bytes);
            ss_table_bytes.extend(usize::to_ne_bytes(filter_bytes.len()));
            ss_table_bytes.extend(filter_bytes.iter());
        }
        ss_table_bytes.into_boxed_slice()
    }

    fn empty(&mut self) {
        self.root = None;
    }

    fn iterator(&self) -> Box<dyn Iterator<Item = (Box<[u8]>, MemoryEntry)> + '_> {
        if let Some(root) = self.root.as_ref() {
            let mut stack = Vec::new();
            let mut entry_stack = Vec::new();
            stack.push(root);
            entry_stack.push(0);

            let iterator = BTreeIterator {
                stack,
                entry_stack,
            };

            Box::new(iterator)
        } else {
            Box::new(std::iter::empty())
        }
        //Box::new(self.iter().map(|iterator| iterator).into_iter().flatten())
    }
}
