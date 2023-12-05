use segment_elements::{MemoryEntry, TimeStamp};
use bloom_filter::BloomFilter;

#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub(crate) key: Box<[u8]>,
    pub(crate) mem_entry: MemoryEntry
}

impl Entry {
    pub(crate) fn from(key: &[u8], value: &[u8], tombstone: bool, time_stamp: TimeStamp) -> Self {
        Entry { key: Box::from(key), mem_entry: MemoryEntry::from(value, tombstone, time_stamp.get_time()) }
    }

    pub(crate) fn get_key(&self) -> &[u8] {
        &self.key
    }

    pub(crate) fn get_mem_entry(&self) -> &MemoryEntry {
        &self.mem_entry
    }
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?}, {:?})", self.key, self.mem_entry.get_value())
    }
}

#[derive(Clone)]
pub(crate) struct Node {
    pub(crate) is_leaf: bool,
    pub(crate) children: Box<[Option<Node>]>,
    pub(crate) entries: Box<[Option<Entry>]>,
    pub(crate) degree: usize,
    pub(crate) n: usize
}

impl Node {
    pub(crate) fn new(degree: usize, is_leaf: bool) -> Self {
        Node {
            is_leaf,
            children: vec![None; 2 * degree].into_boxed_slice(),
            entries: vec![None; 2 * degree - 1].into_boxed_slice(),
            degree,
            n: 0
        }
    }

    pub(crate) fn in_order(&self, data_bytes: &mut Vec<u8>, index_bytes: &mut Vec<u8>, offset: &mut usize, filter: &mut BloomFilter) {
        for i in 0..=self.n {
            if !self.is_leaf {
                self.children[i].as_ref().unwrap().in_order(data_bytes, index_bytes, offset, filter);
            }
            if i < self.n {
                let entry = self.entries[i].as_ref().unwrap();
                let key = entry.get_key();
                let memory_entry = entry.get_mem_entry();
                let memory_entry_bytes = memory_entry.serialize(key);
                data_bytes.extend(memory_entry_bytes.iter());

                index_bytes.extend(usize::to_ne_bytes(key.len()));
                index_bytes.extend(key);
                index_bytes.extend(usize::to_ne_bytes(*offset));

                filter.add(key);

                *offset += data_bytes.len();
            }
        }
    }
    pub(crate) fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        let mut node_index = 0;

        while node_index < self.n && key > &self.entries[node_index].as_ref().unwrap().key {
            node_index += 1;
        }

        if node_index < self.n && key == &*self.entries[node_index].as_ref().unwrap().key {
            if !self.entries[node_index].as_ref().unwrap().mem_entry.get_tombstone() {
                Some(self.entries[node_index].as_ref().unwrap().mem_entry.get_value())
            } else {
                None
            }
        } else if self.is_leaf {
            None
        } else {
            self.children[node_index].as_ref().unwrap().get(key)
        }
    }

    pub(crate) fn insert_non_full(&mut self, key: &[u8], value: &[u8], tombstone: bool, time_stamp: TimeStamp) {
        let mut curr_node_index = self.n as i64 - 1;

        if self.is_leaf {

            while curr_node_index >= 0 && key < &self.entries[curr_node_index as usize].as_ref().unwrap().key {
                self.entries[(curr_node_index + 1) as usize] = self.entries[curr_node_index as usize].take();
                curr_node_index -= 1;
            }

            self.entries[(curr_node_index + 1) as usize] = Some(Entry::from(key, value, tombstone, time_stamp));
            self.n += 1;
        } else {
            while curr_node_index >= 0 && key < &self.entries[curr_node_index as usize].as_ref().unwrap().key {
                curr_node_index -= 1;
            }

            if self.children[(curr_node_index + 1) as usize].as_ref().unwrap().n == (2 * self.degree - 1) {
                self.split_children((curr_node_index + 1) as usize);

                if key > &self.entries[(curr_node_index + 1) as usize].as_ref().unwrap().key {
                    curr_node_index += 1;
                }
            }

            self.children[(curr_node_index + 1) as usize].as_mut().unwrap().insert_non_full(key, value, tombstone, time_stamp);
        }
    }

    pub(crate) fn split_children(&mut self, split_child_index: usize) {
        let child_to_be_split = self.children[split_child_index].as_mut().unwrap();

        let mut new_child = Node::new(child_to_be_split.degree, child_to_be_split.is_leaf);
        new_child.n = self.degree - 1;

        for i in 0..self.degree - 1 {
            new_child.entries[i] = child_to_be_split.entries[i + self.degree].take();
        }

        if !child_to_be_split.is_leaf {
            for i in 0..self.degree {
                new_child.children[i] = child_to_be_split.children[i + self.degree].take();
            }
        }

        child_to_be_split.n = self.degree - 1;

        // the entries were done firstly because then the y ref can be used for the last remaining purpose
        // otherwise it would need to be borrowed again
        // checked subtraction is used because the indices can be 0
        for i in (split_child_index.checked_sub(1).unwrap_or(0)..=self.n.checked_sub(1).unwrap_or(0)).rev() {
            self.entries[i + 1] = self.entries[i].clone();
        }
        self.entries[split_child_index] = child_to_be_split.entries[self.degree - 1].take();

        for i in (split_child_index..=self.n).rev() {
            self.children[i + 1] = self.children[i].clone();
        }

        self.children[split_child_index + 1] = Some(new_child);

        self.n += 1;
    }

    pub(crate) fn logical_deletion(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
        let mut index = 0;

        while index < self.n && key > &self.entries[index].as_ref().unwrap().key {
            index += 1;
        }

        if index < self.n && key == &*self.entries[index].as_ref().unwrap().key {
            self.entries[index] = Some(Entry::from(key, &[], true, time_stamp));
            false
        } else {
            if self.is_leaf {
                true
            } else {
                self.children[index].as_mut().unwrap().logical_deletion(key, time_stamp)
            }
        }
    }

    pub(crate) fn update(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) {
        let mut index = 0;
        while index < self.n && key > &self.entries[index].as_ref().unwrap().key {
            index += 1;
        }
        if index < self.n && key ==  &*self.entries[index].as_ref().unwrap().key {
            self.entries[index] = Some(Entry::from(key, value, false, time_stamp));
        } else {
            if self.is_leaf {
                return;
            } else {
                self.children[index].as_mut().unwrap().update(key, value,time_stamp)
            }
        }
    }
}

// True deletion node impl. Separated because it provides additional and potentially unnecessary
// functionality.
impl Node {
    /// Utility function, finds first index of a key greater or equal to given key.
    pub(crate) fn find_key(&self, key: &[u8]) -> usize {
        let mut index = 0;
        while index < self.n && key > &self.entries[index].as_ref().unwrap().key {
            index += 1;
        }

        index
    }

    /// Function that permanently removes a key from subtree rooted with this node.
    pub(crate) fn remove(&mut self, key: &[u8]) {
        let index = self.find_key(key);

        // if this node contains the key to be deleted
        if index < self.n && key == self.entries[index].as_ref().unwrap().key.as_ref() {
            if self.is_leaf {
                self.remove_from_leaf(index);
            } else {
                self.remove_from_non_leaf(index);
            }
        } else {
            if self.is_leaf { return; }

            // if true, key is located in the subtree rooted with the last child of curr node
            let flag = if index == self.n { true } else { false };

            //if child has less keys than order fill that child
            if self.children[index].as_ref().unwrap().n < self.degree {
                self.fill(index);
            }

            //if the last child has been merged, we need to look for a key in index-1 position
            if flag && index > self.n {
                self.children[index-1].as_mut().unwrap().remove(key);
            } else {
                self.children[index].as_mut().unwrap().remove(key);
            }
        }
    }

    pub(crate) fn remove_from_leaf(&mut self, index_to_del: usize) {
        // move all the keys 1 position backwards to fill the gap of deleted key
        for i in (index_to_del+1)..self.n {
            self.entries[i - 1] = self.entries[i].take();
        }
        self.n -= 1;
    }

    pub(crate) fn remove_from_non_leaf(&mut self, index_to_del: usize) {
        let key = &self.entries[index_to_del].as_ref().unwrap().key.clone();

        if self.children[index_to_del].as_ref().unwrap().n >= self.degree {
            // if a child that precedes key has more than minimum number of keys,
            // swap key with its predecessor, and delete predecessor from children[index] node
            let pred_key = self.get_pred(index_to_del);
            self.entries[index_to_del] = pred_key.clone();
            self.children[index_to_del].as_mut().unwrap().remove(&pred_key.unwrap().key);
        } else if self.children[index_to_del + 1].as_ref().unwrap().n >= self.degree {
            // same process with successor node of our key in children array
            let succ_key = self.get_succ(index_to_del);
            self.entries[index_to_del] = succ_key.clone();
            self.children[index_to_del + 1].as_mut().unwrap().remove(&succ_key.unwrap().key);
        } else {
            // if both successor and predecessor have less than degree keys, merge them and delete key
            self.merge(index_to_del);
            self.children[index_to_del].as_mut().unwrap().remove(key);
        }
    }

    /// Function that returns predecessor of the index of a given key of a node.
    pub(crate) fn get_pred(&self, curr_key_index: usize) -> Option<Entry> {
        let mut current_node = self.children[curr_key_index].as_ref().unwrap();

        // find the the right-most child of left subtree rooted at this key
        while !current_node.is_leaf {
            let last_child = current_node.n;
            current_node = current_node.children[last_child].as_ref().unwrap();
        }

        let key_position = current_node.n - 1;
        current_node.entries[key_position].clone()
    }

    /// Function that returns successor of the index of a given key of a node.
    pub(crate) fn get_succ(&self, curr_key_index: usize) -> Option<Entry> {
        let mut current_node = self.children[curr_key_index + 1].as_ref().unwrap();

        // find the the left-most child of right subtree rooted at this key
        while !current_node.is_leaf {
            current_node = current_node.children[0].as_ref().unwrap();
        }

        current_node.entries[0].clone()
    }

    /// Function that fills up a child node of self located in the given position in a child array
    /// only if it has less than order - 1 keys.
    pub(crate) fn fill(&mut self, index_to_fill: usize) {
        if index_to_fill != 0 && self.children[index_to_fill-1].as_ref().unwrap().n >= self.degree {
            // take key from left sibling if exists and has enough keys
            self.borrow_from_prev(index_to_fill);
        } else if index_to_fill != self.n && self.children[index_to_fill + 1].as_ref().unwrap().n >= self.degree {
            // take key from right sibling if exists and has enough keys
            self.borrow_from_next(index_to_fill);
        } else {
            // if neither left nor right sibling have enough keys, merge children[index] with next sibling
            // or merge it with its previous sibling if the next doesnt exist
            if index_to_fill != self.n {
                self.merge(index_to_fill);
            } else {
                self.merge(index_to_fill - 1);
            }
        }
    }

    /// Function that borrows an entry from node located in child array at index-1 and gives it to the
    /// node located at the children[index].
    pub(crate) fn borrow_from_prev(&mut self, index: usize) {
        // this is the entry that is going to go to child
        let childs_entry = self.entries[index-1].take();

        let sibling = self.children[index - 1].as_mut().unwrap();

        // move entry from sibling to parent to fill the hole
        self.entries[index - 1] = sibling.entries[sibling.n - 1].take();

        // siblings last child will end up in child node of self
        let mut siblings_last_child = None;
        if !sibling.is_leaf {
            siblings_last_child = sibling.children[sibling.n].take();
        }
        sibling.n -= 1;

        let child = self.children[index].as_mut().unwrap();

        // last key from sibling goes up to the parent and entry[index-1] of a parent goes to the child
        // the entry gets inserted at the index 0 in child entry-set

        // move all keys of child 1 step ahead
        for i in (0..=(child.n - 1)).rev() {
            child.entries[i + 1] = child.entries[i].take();
        }

        // if child isn't leaf move all its pointers
        if !child.is_leaf {
            for i in (0..=child.n).rev() {
                child.children[i + 1] = child.children[i].take();
            }
            child.children[0] = siblings_last_child;
        }

        // move the key from parent to child
        child.entries[0] = childs_entry;
        child.n += 1;
    }

    pub(crate) fn borrow_from_next(&mut self, index: usize) {
        // this is the entry that is going to go to child
        let childs_entry = self.entries[index].take();

        let sibling = self.children[index + 1].as_mut().unwrap();

        // move entry from sibling to parent to fill the hole
        self.entries[index] = sibling.entries[0].take();

        // siblings first child will end up in child node of self
        let mut siblings_first_child = None;
        if !sibling.is_leaf {
            siblings_first_child = sibling.children[0].take();
        }

        // move all the entries in sibling 1 step behind
        for i in 1..sibling.n {
            sibling.entries[i-1] = sibling.entries[i].take();
        }

        // move all the children of sibling 1 step behind
        for i in 1..=sibling.n {
            sibling.children[i-1] = sibling.children[i].take();
        }

        sibling.n -= 1;

        let child = self.children[index].as_mut().unwrap();

        // insert the entry at first available position
        child.entries[child.n] = childs_entry;
        if !child.is_leaf {
            child.children[child.n+1] = siblings_first_child;
        }

        child.n += 1;
    }

    /// Function that merges children of self located at index and index+1 positions.
    pub(crate) fn merge(&mut self, index: usize) {
        let sibling = self.children[index + 1].as_mut().unwrap();
        let children_len = sibling.children.len();
        let entries_len = sibling.entries.len();
        let mut sibling_children = std::mem::replace(&mut sibling.children, vec![None; children_len].into_boxed_slice());
        let mut sibling_entries = std::mem::replace(&mut sibling.entries, vec![None; entries_len].into_boxed_slice());
        let sibling_n = sibling.n;

        let child = self.children[index].as_mut().unwrap();

        // take the entry from current node and insert it in between current entries and siblings entries
        child.entries[self.degree-1] = self.entries[index].take();

        // copy the entries from sibling
        for i in 0..sibling_n {
            child.entries[i + self.degree] = sibling_entries[i].take();
        }

        // copy the children from sibling
        if !child.is_leaf {
            for i in 0..=sibling_n {
                child.children[i + self.degree] = sibling_children[i].take();
            }
        }

        // update n of child
        child.n += sibling_n + 1;

        // move all the keys 1 step to the left to fill the gap
        for i in (index+1)..self.n {
            self.entries[i-1] = self.entries[i].take();
        }

        // move child pointers to the right because node has 1 less child
        for i in (index+2)..=self.n {
            self.children[i-1] = self.children[i].take();
        }

        //update n of self
        self.n -= 1;
    }
}