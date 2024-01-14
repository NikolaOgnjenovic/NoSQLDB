use segment_elements::MemoryEntry;
use crate::b_tree_node::Node;

pub struct BTreeIterator<'a> {
    pub(crate) stack: Vec<&'a Node>,
    pub(crate) entry_stack: Vec<usize>,
}

// impl<'a> BTreeIterator<'a> {
//     pub fn find_leftmost_child(&mut self) {
//         let mut current_node = *self.stack.last().unwrap();
//         let mut index = *self.entry_stack.last().unwrap();
//         while let Some(left_child) = current_node.children[index].as_ref() {
//             current_node = left_child;
//             index = 0;
//             self.stack.push(current_node);
//             self.entry_stack.push(0);
//         }
//     }
// }
//
// impl<'a> Iterator for BTreeIterator<'a> {
//     type Item = (Box<[u8]>, MemoryEntry);
//
//     fn next(&mut self) -> Option<Self::Item> {
//         let mut index = self.stack.len() - 1;
//         let mut current_node = self.stack[index];
//
//         while index > 0 {
//             if self.entry_stack[index] < current_node.n {
//                 let memory_entry = current_node.entries[self.entry_stack[index]].as_ref().unwrap().mem_entry.clone();
//                 let key = current_node.entries[self.entry_stack[index]].as_ref().unwrap().key.clone();
//                 self.entry_stack[index] += 1;
//                 return Option::from((key, memory_entry));
//             } else {
//                 self.entry_stack.pop();
//                 self.stack.pop();
//
//                 index = self.stack.len() - 1;
//                 current_node = self.stack[index];
//                 if let Some(entry) = current_node.entries[self.entry_stack[index]].as_ref() {
//                     let memory_entry = entry.mem_entry.clone();
//                     let key = entry.key.clone();
//                     self.entry_stack[index] += 1;
//                     self.find_leftmost_child();
//                     return Option::from((key, memory_entry));
//                 }
//             }
//         }
//         None
//     }
// }
impl<'a> BTreeIterator<'a> {
    pub fn find_leftmost_child(&mut self) {
        while let Some(left_child) = self.stack.last().and_then(|node| node.children.get(0)).and_then(Option::as_ref) {
            self.stack.push(left_child);
            self.entry_stack.push(0);
        }
    }
}

impl<'a> Iterator for BTreeIterator<'a> {
    type Item = (Box<[u8]>, MemoryEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            return None; // B-tree is empty
        }

        let entry_stack_len = self.entry_stack.len();

        while let Some(current_node) = self.stack.last() {
            let index = *self.entry_stack.last().unwrap();
            if index < current_node.n {
                let entry = &current_node.entries[index].as_ref()?;
                let memory_entry = entry.mem_entry.clone();
                let key = entry.key.clone();
                self.entry_stack[entry_stack_len - 1] += 1;
                return Some((key, memory_entry));
            } else {
                self.entry_stack.pop();
                self.stack.pop();
                self.find_leftmost_child();
            }
        }

        None
    }
}