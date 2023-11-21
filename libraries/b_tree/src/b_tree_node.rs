#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub(crate) key: Box<[u8]>,
    pub(crate) value: Box<[u8]>
}

impl Entry {
    pub(crate) fn from(key: &[u8], value: &[u8]) -> Self {
        Entry { key: Box::from(key), value: Box::from(value) }
    }
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?}, {:?})", self.key, self.value)
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

    pub(crate) fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        let mut node_index = 0;

        while node_index < self.n && key > &self.entries[node_index].as_ref().unwrap().key {
            node_index += 1;
        }

        if node_index < self.n && key == &*self.entries[node_index].as_ref().unwrap().key {
            Some(Box::clone(&self.entries[node_index].as_ref().unwrap().value))
        } else if self.is_leaf {
            None
        } else {
            self.children[node_index].as_ref().unwrap().get(key)
        }
    }

    pub(crate) fn insert_non_full(&mut self, key: &[u8], value: &[u8]) {
        let mut curr_node_index = self.n as i64 - 1;

        if self.is_leaf {
            while curr_node_index >= 0 && key < &self.entries[curr_node_index as usize].as_ref().unwrap().key {
                self.entries[(curr_node_index + 1) as usize] = self.entries[curr_node_index as usize].take();
                curr_node_index -= 1;
            }

            self.entries[(curr_node_index + 1) as usize] = Some(Entry::from(key, value));
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

            self.children[(curr_node_index + 1) as usize].as_mut().unwrap().insert_non_full(key, value);
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

    pub(crate) fn print_node(&self, mut level: usize) {
        print!("Level\t{}\t{}: ", level, self.n);

        for entry in self.entries.iter().take(self.n).filter(|entry| entry.is_some()).map(|entry| entry.as_ref().unwrap()) {
            print!("{} ", entry);
        }

        println!();

        level += 1;

        if self.children.len() > 0 {
            for child in self.children.iter().filter(|entry| entry.is_some()).map(|child| child.as_ref().unwrap()) {
                child.print_node(level);
            }
        }
    }
}