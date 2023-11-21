mod b_tree;
mod b_tree_node;
mod order_error;

pub use b_tree::BTree;

#[cfg(test)]
mod tests {
    use rand::Rng;
    use crate::b_tree::BTree;

    #[test]
    fn insert_test() {
        let mut b = BTree::new(3).unwrap();

        for i in (0..10u8).rev() {
            b.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes());
        }

        b.print_tree();
    }

    #[test]
    fn get_test() {
        for i in 2..15 {
            let mut b = BTree::new(i).unwrap();

            let mut added_elements = Vec::new();
            let mut rng = rand::thread_rng();

            for _ in 0..100000 {
                let random_number: u128 = rng.gen_range(0..=10000000);
                added_elements.push(random_number);
                b.insert(&random_number.to_ne_bytes(), &(random_number * 2).to_ne_bytes());
            }

            for random_number in added_elements {
                assert_eq!(b.get(&random_number.to_ne_bytes()), Some(Box::from((random_number * 2).to_ne_bytes())));
            }
        }
    }
}
