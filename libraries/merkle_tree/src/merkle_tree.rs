use sha256::digest;

/// Represents a node in a Merkle tree.
#[derive(Clone, Debug)]
pub struct Node {
    pub hash: String,
    pub left_child: Option<Box<Node>>,
    pub right_child: Option<Box<Node>>
}

impl Node {
    /// Creates a new leaf node with the hash computed from the given data.
    fn new(data: &[u8]) -> Node {
        Node {
            hash: digest(data),
            left_child: None,
            right_child: None
        }
    }

    /// Creates a new empty node with an empty hash.
    fn new_empty() -> Node {
        Node {
            hash: String::from(""),
            left_child: None,
            right_child: None
        }
    }

    /// Creates a new node with children, computing the hash from their concatenated hashes.
    fn new_with_children(left_child: Option<Box<Node>>, right_child: Option<Box<Node>>) -> Node {
        if left_child.is_none() && right_child.is_none() {
            return Node::new_empty();
        }

        let concatenated_hashes = format!(
            "{}{}",
            left_child
                .as_ref()
                .map_or("", |node| node.hash.as_str()),
            right_child
                .as_ref()
                .map_or("", |node| node.hash.as_str())
        );
        Node {
            hash: digest(concatenated_hashes),
            left_child,
            right_child
        }
    }

    /// Creates a new empty node with a given hash value.
    fn new_with_hash(hash: String) -> Node {
        Node {
            hash,
            left_child: None,
            right_child: None
        }
    }

    /// Checks if the node is a leaf (has no children).
    fn is_leaf(&self) -> bool {
        self.left_child.is_none() && self.right_child.is_none()
    }
}

/// Represents a Merkle tree.
#[derive(Debug)]
pub struct MerkleTree {
    pub root: Option<Box<Node>>
}

impl MerkleTree {
    /// Creates a new Merkle tree from the given data with a chunk size of 1024 bytes.
    pub fn new(data: &[u8]) -> MerkleTree {
        let mut tree = MerkleTree { root: None };

        let nodes: Vec<Node> = data
            .chunks(1024)
            .map(|d| Node::new(d))
            .collect();

        tree.build_tree(nodes);

        tree
    }

    /// Recursively builds the Merkle tree from a list of data nodes.
    fn build_tree(&mut self, nodes: Vec<Node>) {
        if nodes.is_empty() {
            return;
        }

        let mut new_nodes: Vec<Node> = Vec::new();

        for chunk in nodes.chunks(2) {
            match chunk {
                [left] => {
                    new_nodes.push(left.clone());
                }
                [left, right] => {
                    let parent = Node::new_with_children(Some(Box::from(left.clone())), Some(Box::from(right.clone())));
                    new_nodes.push(parent);
                }
                _ => unreachable!(),
            }
        }

        if new_nodes.len() == 1 {
            self.root = Some(Box::from(new_nodes.remove(0)));
        } else {
            self.build_tree(new_nodes);
        }
    }

    // Returns the indices of chunks that have to are different in the checking MerkleTree
    pub fn get_different_chunks_indices(&self, checking: &MerkleTree) -> Vec<usize> {
        let mut different_chunk_indices = Vec::new();

        if let (Some(self_root), Some(checking_root)) = (self.root.as_ref(), checking.root.as_ref()) {
            Self::find_different_chunk_indices(self_root, checking_root, &mut different_chunk_indices, 0);
        }

        different_chunk_indices
    }

    /// Recursively finds the indices of chunks that are different between two Merkle trees.
    fn find_different_chunk_indices(root1: &Node, root2: &Node, indices: &mut Vec<usize>, current_index: usize) {
        if root1.is_leaf() && root2.is_leaf() && root1.hash != root2.hash {
            indices.push(current_index);
            return;
        }

        if let (Some(left1), Some(left2)) = (&root1.left_child, &root2.left_child) {
            Self::find_different_chunk_indices(left1, left2, indices, current_index * 2);
        }

        if let (Some(right1), Some(right2)) = (&root1.right_child, &root2.right_child) {
            Self::find_different_chunk_indices(right1, right2, indices, current_index * 2 + 1);
        }
    }

    /// Serializes the Merkle tree into a boxed slice of bytes.
    pub fn serialize(&self) -> Box<[u8]> {
        let mut data = Vec::new();

        Self::get_data_blocks(self.root.as_ref(), &mut data);

        data.into_boxed_slice()
    }

    /// Recursively collects the data blocks from the Merkle tree nodes.
    fn get_data_blocks(root: Option<&Box<Node>>, data: &mut Vec<u8>) {
        if let Some(node) = root {
            if node.is_leaf() {
                data.extend(node.hash.as_bytes());
            } else {
                Self::get_data_blocks(node.left_child.as_ref(), data);
                Self::get_data_blocks(node.right_child.as_ref(), data);
            }
        }
    }

    /// Deserializes the Merkle tree from a boxed slice of bytes.
    pub fn deserialize(data: &[u8]) -> MerkleTree {
        let mut tree = MerkleTree { root: None };

        let nodes: Vec<Node> = data
            .chunks(1024)
            .map(|d| {
                Node::new_with_hash(String::from_utf8(d.to_vec()).unwrap())
            })
            .collect();

        tree.build_tree(nodes);

        tree
    }
}