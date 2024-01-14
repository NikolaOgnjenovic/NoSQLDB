use segment_elements::MemoryEntry;
use crate::b_tree_node::Node;

pub struct BTreeIterator<'a> {
    pub(crate) stack: Vec<&'a Node>,
    pub(crate) entry_stack: Vec<usize>,
}

impl<'a> BTreeIterator<'a> {
    pub fn find_leftmost_child(&mut self) {
        let mut current_node = *self.stack.last().unwrap();
        let mut index = *self.entry_stack.last().unwrap();
        while let Some(left_child) = current_node.children[index].as_ref() {
            current_node = left_child;
            index = 0;
            self.stack.push(current_node);
            self.entry_stack.push(0);
        }
    }
}

impl<'a> Iterator for BTreeIterator<'a> {
    type Item = (Box<[u8]>, MemoryEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            return None;
        }
        while let Some(current_node) = self.stack.last() {
            let entry_stack_len = self.entry_stack.len();
            let curr_entry_index = self.entry_stack[entry_stack_len-1];
            if curr_entry_index < current_node.n {
                let yielded_entry = current_node.entries[curr_entry_index].clone().unwrap();
                let key = yielded_entry.key;
                let mem_entry = yielded_entry.mem_entry;
                self.entry_stack[entry_stack_len-1] += 1;
                self.find_leftmost_child();
                return Option::from((key, mem_entry));
            }
            else {
                self.entry_stack.pop();
                self.stack.pop();

            }
        }
        None
    }
}
