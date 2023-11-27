mod b_tree;
mod b_tree_node;
mod order_error;

pub use b_tree::BTree;

#[cfg(test)]
mod tests {
    use rand::Rng;
    use crate::b_tree::BTree;

    // will be removed, used for visualising the tree while still in production
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
                let random_number: u128 = rng.gen_range(0..=1000000000);
                added_elements.push(random_number);
                b.insert(&random_number.to_ne_bytes(), &(random_number * 2).to_ne_bytes());
            }

            for random_number in added_elements {
                assert_eq!(b.get(&random_number.to_ne_bytes()), Some(Box::from((random_number * 2).to_ne_bytes())));
            }
        }
    }

    #[test]
    fn delete_test1() {
        for i in 2..15 {
            let mut b = BTree::new(2).unwrap();

            let mut added_elements = Vec::new();
            let mut rng = rand::thread_rng();

            for _ in 0..10 {
                let random_number: u128 = rng.gen_range(0..=1000000000);
                added_elements.push(random_number);
                b.insert(&random_number.to_ne_bytes(), &(random_number * 2).to_ne_bytes());
            }

            let mut removed_elements = Vec::new();

            for j in 0..10 {
                if rng.gen_bool(1.0) {
                    let element_to_be_removed = added_elements[j];
                    removed_elements.push(element_to_be_removed);

                    b.remove(&element_to_be_removed.to_ne_bytes());
                }
            }

            for random_number in removed_elements {
                assert_eq!(b.get(&random_number.to_ne_bytes()), None);
            }
        }
    }
}
