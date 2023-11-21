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
        let mut i = self.n as i64 - 1;

        if self.is_leaf {
            while i >= 0 && key < &self.entries[i as usize].as_ref().unwrap().key {
                self.entries[(i + 1) as usize] = self.entries[i as usize].clone();
                i -= 1;
            }

            self.entries[(i + 1) as usize] = Some(Entry::from(key, value));
            self.n += 1;
        } else {
            while i >= 0 && key < &self.entries[i as usize].as_ref().unwrap().key {
                i -= 1;
            }

            if self.children[(i + 1) as usize].as_ref().unwrap().n == (2 * self.degree - 1) {
                self.split_children((i + 1) as usize);

                if key > &self.entries[(i + 1) as usize].as_ref().unwrap().key {
                    i += 1;
                }
            }

            self.children[(i + 1) as usize].as_mut().unwrap().insert_non_full(key, value);
        }
    }

    pub(crate) fn split_children(&mut self, i: usize) {
        //let mut y = Option::take(&mut self.children[i]).unwrap();
        let y = self.children[i].clone().unwrap();

        let mut z = Node::new(y.degree, y.is_leaf);
        z.n = self.degree - 1;

        for j in 0..self.degree - 1 {
            z.entries[j] = y.entries[j + self.degree].clone();
        }

        if !y.is_leaf {
            for j in 0..self.degree {
                z.children[j] = y.children[j + self.degree].clone();
            }
        }

        // y.n = self.degree - 1; - wrong because y is copied, this needs to be done to the original y
        self.children[i].as_mut().unwrap().n = self.degree - 1;

        for j in (i..=self.n).rev() {
            self.children[j + 1] = self.children[j].clone();
        }

        self.children[i + 1] = Some(z);

        for j in (i.checked_sub(1).unwrap_or(0)..=self.n.checked_sub(1).unwrap_or(0)).rev() {
            self.entries[j + 1] = self.entries[j].clone();
        }

        self.entries[i] = y.entries[self.degree - 1].clone();
        //self.entries[i] = Option::take(&mut y.entries[self.degree - 1]);

        self.n += 1;
        //self.children[i] = Some(y);
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