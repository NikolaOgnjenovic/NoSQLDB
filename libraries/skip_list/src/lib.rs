mod skip_list;

pub use skip_list::SkipList;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10]);
        skip_list.insert(&[2], &[20]);
        skip_list.insert(&[3], &[30]);

        assert_eq!(skip_list.get(&[1]), Some(Box::from([10].to_vec())));
        assert_eq!(skip_list.get(&[2]), Some(Box::from([20].to_vec())));
        assert_eq!(skip_list.get(&[3]), Some(Box::from([30].to_vec())));
    }

    // todo infinite loop
    #[test]
    fn test_insert_and_delete() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10]);
        skip_list.insert(&[2], &[20]);
        skip_list.insert(&[3], &[30]);

        assert_eq!(skip_list.delete(&[2]), Some(Box::from([20])));
        assert_eq!(skip_list.get(&[2]), None);
    }

    #[test]
    fn test_insert_duplicate_key() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10]);
        skip_list.insert(&[1], &[100]); // Duplicate key

        assert_eq!(skip_list.get(&[1]), Some(Box::from([100].to_vec())));
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10]);

        assert_eq!(skip_list.delete(&[2]), None);
    }

    #[test]
    fn test_empty_skiplist() {
        let mut skip_list = SkipList::new(4);

        assert_eq!(skip_list.get(&[1]), None);
        assert_eq!(skip_list.delete(&[1]), None);
    }

    // todo fails
    #[test]
    fn test_random_insert_and_get() {
        use rand::Rng;
        let mut skip_list = SkipList::new(4);
        let mut reference_map = std::collections::HashMap::new();
        let mut rng = rand::thread_rng();

        for _ in 0..1000 {
            let key = rng.gen_range(0..1000);
            let value = rng.gen_range(0..1000);
            skip_list.insert(&[key as u8], &[value as u8]);
            reference_map.insert(key, value);
        }

        for (key, value) in reference_map.iter() {
            assert_eq!(skip_list.get(&[*key as u8]), Some(Box::from(vec![*value as u8])));
        }
    }

    // todo infinite loop
	#[test]
	fn random_method_calls(){
		let mut skip_list = SkipList::new(10);
		assert_eq!(skip_list.insert(&[0,1,2,3,4,5,6,7], &[0,0,11,11,1,1,1,1]), None);
		assert_eq!(skip_list.insert(&[0,2,2,3,4,5,6,7], &[0,4,11,11,1,1,1,1]), None);
		assert_eq!(skip_list.insert(&[0,3,2,3,4,5,6,7], &[0,5,11,11,1,1,1,1]), None);
		assert_eq!(skip_list.delete(&[0,3,22,3,4,5,6,7]), None);
		assert_eq!(skip_list.delete(&[0,3,2,3,4,5,6,7]), Some(Box::from([0,5,11,11,1,1,1,1].to_vec())));
		assert_eq!(skip_list.insert(&[0,2,2,3,4,52,6,7], &[0,4,11,111,1,12,1,1]), None);
		assert_eq!(skip_list.insert(&[0,3,2,3,4,5,36,7], &[0,5,11,11,1,31,1,1]), None);
		assert_eq!(skip_list.delete(&[0,2,2,3,4,52,6,7]), Some(Box::from([0,4,11,111,1,12,1,1].to_vec())));
		assert_eq!(skip_list.get(&[0,3,2,3,4,5,36,7]), Some(Box::from([0,5,11,11,1,31,1,1].to_vec())));

		assert_eq!(skip_list.get(&[0,3,2,3,4,5,6,7]), None);
	}
}