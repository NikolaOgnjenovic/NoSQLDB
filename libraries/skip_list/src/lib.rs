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
        assert_eq!(skip_list.get(&[0,1,2,3,4,5,6,7]), Some(Box::from([0,0,11,11,1,1,1,1].to_vec())));
        assert_eq!(skip_list.get(&[0,2,2,3,4,5,6,7]), Some(Box::from([0,4,11,11,1,1,1,1].to_vec())));
	}

    #[test]
    fn test_insert_and_get_more() {
        let max_level = 16;
        let mut skip_list = SkipList::new(max_level);

        for i in 0..30 {
            let key = (i as i32).to_ne_bytes();
            let value = ((i * 2) as i32).to_ne_bytes();
            skip_list.insert(&key, &value);
        }

        for i in 0..30 {
            let key = (i as i32).to_ne_bytes();
            let value = ((i * 2) as i32).to_ne_bytes();
            let result = skip_list.get(&key);
            assert_eq!(result, Some(Box::from(value)));
        }
    }
}