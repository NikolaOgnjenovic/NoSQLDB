mod b_tree;
mod b_tree_node;
mod order_error;

pub use b_tree::BTree;
pub use order_error::OrderError;

#[cfg(test)]
mod tests {
    use peak_alloc::PeakAlloc;
    use rand::Rng;
    use segment_elements::{SegmentTrait, TimeStamp};
    use crate::b_tree::BTree;

    #[global_allocator]
    static PEAK_ALLOC: PeakAlloc = PeakAlloc;

    // will be removed, used for visualising the tree while still in production
    #[test]
    fn insert_test() {
        let mut b = BTree::new(3).unwrap();

        for i in (0..10u8).rev() {
            b.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

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
                b.insert(&random_number.to_ne_bytes(), &(random_number * 2).to_ne_bytes(), TimeStamp::Now);
            }

            for random_number in added_elements {
                assert_eq!(b.get(&random_number.to_ne_bytes()).unwrap(), Box::from((random_number * 2).to_ne_bytes()));
            }
        }
    }

    #[test]
    fn insert_multiple_twice() {
        let mut b = BTree::new(4).unwrap();

        for i in 0..100u128 {
            b.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

        b.insert(&50u128.to_ne_bytes(), &0u128.to_ne_bytes(), TimeStamp::Now);
        b.insert(&60u128.to_ne_bytes(), &1u128.to_ne_bytes(), TimeStamp::Now);
        b.insert(&31u128.to_ne_bytes(), &2u128.to_ne_bytes(), TimeStamp::Now);
        b.insert(&34u128.to_ne_bytes(), &3u128.to_ne_bytes(), TimeStamp::Now);
        b.insert(&89u128.to_ne_bytes(), &4u128.to_ne_bytes(), TimeStamp::Now);
        b.insert(&23u128.to_ne_bytes(), &5u128.to_ne_bytes(), TimeStamp::Now);

        assert_eq!(b.get(&50u128.to_ne_bytes()).unwrap(), Box::from(0u128.to_ne_bytes()));
        assert_eq!(b.get(&60u128.to_ne_bytes()).unwrap(), Box::from(1u128.to_ne_bytes()));
        assert_eq!(b.get(&31u128.to_ne_bytes()).unwrap(), Box::from(2u128.to_ne_bytes()));
        assert_eq!(b.get(&34u128.to_ne_bytes()).unwrap(), Box::from(3u128.to_ne_bytes()));
        assert_eq!(b.get(&89u128.to_ne_bytes()).unwrap(), Box::from(4u128.to_ne_bytes()));
        assert_eq!(b.get(&23u128.to_ne_bytes()).unwrap(), Box::from(5u128.to_ne_bytes()));

        assert_eq!(b.get(&70u128.to_ne_bytes()).unwrap(), Box::from(140u128.to_ne_bytes()));
        assert_eq!(b.get(&80u128.to_ne_bytes()).unwrap(), Box::from(160u128.to_ne_bytes()));
        assert_eq!(b.get(&90u128.to_ne_bytes()).unwrap(), Box::from(180u128.to_ne_bytes()));
    }

    #[test]
    fn delete_perm_test() {
        for i in 2..15 {
            let mut b = BTree::new(i).unwrap();

            let mut added_elements = Vec::new();
            let mut rng = rand::thread_rng();

            for _ in 0..10 {
                let random_number: u128 = rng.gen_range(0..=1000000000);
                added_elements.push(random_number);
                b.insert(&random_number.to_ne_bytes(), &(random_number * 2).to_ne_bytes(), TimeStamp::Now);
            }

            let mut removed_elements = Vec::new();

            for j in 0..10 {
                if rng.gen_bool(1.0) {
                    let element_to_be_removed = added_elements[j];
                    removed_elements.push(element_to_be_removed);

                    b.delete_permanent(&element_to_be_removed.to_ne_bytes());
                }
            }

            for random_number in removed_elements {
                assert_eq!(b.get(&random_number.to_ne_bytes()), None);
            }
        }
    }

    #[test]
    fn test_delete1() {
        let mut b = BTree::new(5).unwrap();

        for i in 0..100u128 {
            b.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

        assert!(b.delete(&50u128.to_ne_bytes(), TimeStamp::Now));
        assert_eq!(b.get(&50u128.to_ne_bytes()), None);
    }

    #[test]
    fn test_delete2() {
        let mut b = BTree::new(5).unwrap();

        for i in 0..100u128 {
            b.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

        assert!(!b.delete(&1000u128.to_ne_bytes(), TimeStamp::Now));
    }

    #[test]
    fn test_insert_return_value() {
        let mut b = BTree::new(5).unwrap();

        for i in 0..100u128 {
            assert!(b.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now));
        }

        assert!(!b.insert(&50u128.to_ne_bytes(), &[50], TimeStamp::Now));

    }

    #[test]
    #[ignore]
    fn test_memory() {
        let mut b = BTree::new(3).unwrap();

        for i in 0..1000000u128 {
            b.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

        println!("Current mem usage with full btree: {}MB", PEAK_ALLOC.current_usage_as_mb());

        b.empty();

        println!("Current mem usage with empty btree: {}MB", PEAK_ALLOC.current_usage_as_mb());
    }
}
