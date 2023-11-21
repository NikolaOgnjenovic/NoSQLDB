mod merkle_tree;

pub use merkle_tree::Node;
use merkle_tree::MerkleTree;
use sha256::digest;

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the hashing mechanism of the Merkle Tree.
    #[test]
    fn test_hashes() {
        let data = &[1, 2, 3, 4];

        let merkle_tree = MerkleTree::new(data);

        verify_hashes(&merkle_tree.root);
    }

    /// Recursively verifies the hashes in the Merkle Tree.
    fn verify_hashes(node: &Option<Box<Node>>) {
        if let Some(node) = node {
            // If a node has no children (is the lowest-level node), assume that it's hash is correct
            if node.left_child.is_none() && node.right_child.is_none() {
                return;
            }

            let child_hash = digest(format!(
                "{}{}",
                node.left_child.as_ref().map_or("", |child| &child.hash),
                node.right_child.as_ref().map_or("", |child| &child.hash)
            ));
            assert_eq!(
                node.hash,
                child_hash
            );

            verify_hashes(&node.left_child);
            verify_hashes(&node.right_child);
        }
    }

    /// Tests the equality check between two identical Merkle Trees.
    #[test]
    fn test_equal_trees() {
        let data = &[1, 2, 3, 4];

        let merkle_tree = MerkleTree::new(data);
        let second_tree = MerkleTree::new(data);

        assert_eq!(0, merkle_tree.get_different_chunks_indices(&second_tree).len());
    }

    /// Tests the inequality check between two different Merkle Trees.
    #[test]
    fn test_unequal_trees() {
        let data = &[1, 2, 3, 4];
        let merkle_tree = MerkleTree::new(data);

        let second_data = &[1,2,3,5];
        let second_tree = MerkleTree::new(second_data);

        assert_ne!(0, merkle_tree.get_different_chunks_indices(&second_tree).len());
    }

    /// Tests the creation of a Merkle Tree from empty data.
    #[test]
    fn test_empty_data() {
        let data: &[u8] = &[];

        let merkle_tree = MerkleTree::new(data);

        assert!(merkle_tree.root.is_none());
    }

    /// Tests the serialization and deserialization of a Merkle tree.
    #[test]
    fn test_serialization_deserialization() {
        let original_data = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let merkle_tree = MerkleTree::new(original_data);
        let serialized_data = merkle_tree.serialize();
        let deserialized_tree = MerkleTree::deserialize(serialized_data);

        assert_eq!(
            merkle_tree.get_different_chunks_indices(&deserialized_tree),
            Vec::<usize>::new()
        );
    }
}
