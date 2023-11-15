use std::cmp::Ordering;
use std::error;
use std::fmt;

//TODO!
//treba implementirati brisanje
//dodavanje nije testirano onako skroz ali sam proverio kod sebe u mainu dok sam pravio trebalo bi da radi
//treba naravno refaktorisati kod na nekim  mestima ali to otom potom




#[derive(Clone)]
pub struct Node {
    k: Vec<KeyVal>,
    v: Vec<KeyVal>,
    m: usize,                   //maximum number of pointers
    n: usize,                   //current number of keys
    links: Vec<Link>,
    leaf: bool,
}

impl Node {

    pub fn new(m: usize, leaf: bool) -> Self {
        let links = vec![None; m];
        let k = vec![None; m-1];
        let v = vec![None; m-1];

        Node {
            k,
            v,
            m,
            n: 0,
            links,
            leaf,
        }
    }

    ///inserts new key to a child, throws error if a key is already present, assumes that node is not full
    pub fn insert_non_full(&mut self, key: Box<[u8]>, value: Box<[u8]>) -> Result<(), Box<dyn error::Error>> {

        let mut i = self.n - 1;
        if self.leaf {

            while let Some(node_key) = &self.k[i] {

                match key.cmp(node_key) {

                    Ordering::Less => i -= 1,
                    Ordering::Equal => return Err(Box::new(ExistingKeyError(String::from("Key already present")))),
                    Ordering::Greater => break,

                }
            }

            self.k.insert(i+1, Some(key));
            self.v.insert(i+1, Some(value));
            self.n += 1;

        } else {

            while let Some(node_key) = &self.k[i] {

                match key.cmp(node_key) {

                    Ordering::Less => i -= 1,
                    Ordering::Equal => return Err(Box::new(ExistingKeyError(String::from("Key already present")))),
                    Ordering::Greater => {

                        let child_node_ptr = &self.links[i+1];
                        let mut child_node = unsafe { &mut (*child_node_ptr.unwrap()) };

                        if child_node.n == self.m-1 {
                            self.split_child(i+1, child_node);

                            //if split happens we append new key to our current node so we need to check position
                            if let Some(node_key) = &self.k[i+1] {
                                if key.cmp(node_key) == Ordering::Greater {
                                    i += 1;
                                }
                            }
                        }

                        //also need to update pointer
                        let child_node_ptr = &self.links[i+1];
                        let mut child_node = unsafe { &mut (*child_node_ptr.unwrap()) };

                        child_node.insert_non_full(key.clone(), value.clone())
                            .unwrap_or_else(|e| eprintln!("{:?}", e));

                    }
                }
            }
        }

        Ok(())
    }

    ///splits child(node_y) of our current node, i is the index of node_y in child array
    pub fn split_child(&mut self, i: usize, node_y: &mut Node) {

        let mut new_node = Node::new(self.m, node_y.leaf);
        new_node.n = self.m/2 - 1;

        //switch the links for keys and values
        for j in 0..self.m/2-1 {
            new_node.k[j] = node_y.k[j+self.m/2].clone();
            node_y.k[j+self.m/2] = None;

            new_node.v[j] = node_y.v[j+self.m/2].clone();
            node_y.v[j+self.m/2] = None;
        }

        //fix the links for child pointers
        if !node_y.leaf {
            for j in 0..self.m/2 {
                new_node.links[j] = node_y.links[j + self.m/2].clone();
                node_y.links[j+self.m/2] = None;
            }
        }

        //fix number of keys in this child node
        node_y.n = self.m/2 - 1;

        //link this new node
        self.links[i+1] = Some(&mut new_node);

        //move the middle k,v pair to this node and update the n of this node
        self.k.insert(i, node_y.k[self.m/2-1].clone());
        self.v.insert(i, node_y.k[self.m/2-1].clone());

        node_y.k[self.m/2-1] = None;
        node_y.v[self.m/2-1] = None;

        self.n += 1;
    }
}

//b tree of order m means that each node can have up to m children and store up to m-1 k,v pairs
pub struct BTree {
    root: Option<Node>,
    m:usize,
}

impl BTree {

    pub fn new(m: usize) -> Self {
        let root = None;

        BTree {
            root,
            m,
        }
    }

    ///function that gets called by get function if we want to start from root
    pub fn _get(&self, key: Box<[u8]>, node: &Node) -> KeyVal {

        let mut i = 0;
        while let Some(node_key) = &node.k[i] {

            match key.cmp(node_key) {

                Ordering::Less => break,
                Ordering::Equal => return node.v[i].clone(),
                Ordering::Greater => i += 1,

            }
        }
        if node.leaf { return None; }

        if let Some(next_ptr) = node.links[i] {

            let next_node = unsafe { &(*next_ptr) };
            BTree::_get(&self, key.clone(), next_node);

        }

        None
    }

    ///Function that returns value associated with the given key, none if key doesn't exist
    pub fn get(&self, key: Box<[u8]>) -> KeyVal {

        let current_node = &self.root.as_ref().unwrap();
        BTree::_get(&self, key, current_node)

    }

    ///function that inserts new node in BTree and does all the balancing if needed
    pub fn insert(&mut self, key: Box<[u8]>, value: Box<[u8]>) -> Result<(), Box<dyn error::Error>> {

        if self.root.is_none() {
            let mut root = Node::new(self.m, true);
            root.k[0] = Some(key);
            root.v[0] = Some(value);
            root.n = 1;
        } else {
            if self.root.as_ref().unwrap().n == self.m - 1 {

                //make new root and split the old root
                let mut new_root = Node::new(self.m, false);
                new_root.links[0] = Some(&mut self.root.clone().unwrap());
                new_root.split_child(0, &mut self.root.clone().unwrap());

                //decide which child(which part of the former root will receive new key
                let mut i = 0;
                if key.cmp(unsafe { &(*new_root.links[0].unwrap()).k[0].clone().unwrap() }) == Ordering::Greater {
                    i += 1;
                }

                unsafe {
                    &(*new_root.links[i].unwrap())
                        .insert_non_full(key, value)
                        .unwrap_or_else(|e| eprintln!("{:?}", e));
                }

            } else {
                self.root.as_mut().unwrap().insert_non_full(key.clone(), value.clone())
                    .unwrap_or_else(|e| eprintln!("{:?}", e));
            }
        }


        Ok(())
    }

}


type Link = Option<*mut Node>;
type KeyVal = Option<Box<[u8]>>;
#[derive(Debug)]
struct ExistingKeyError(String);

impl fmt::Display for ExistingKeyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error occurred {}", self.0)
    }
}

impl error::Error for ExistingKeyError {}


