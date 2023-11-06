mod skip_list;

pub use skip_list::SkipList

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut skip_list = SkipList::new(4);

        let key1 = [1, 1, 1, 1, 1, 1, 1, 1];
        let value1 = [10, 10, 10, 10, 10, 10, 10, 10];

        let key2 = [2, 2, 2, 2, 2, 2, 2, 2];
        let value2 = [20, 20, 20, 20, 20, 20, 20, 20];

        skip_list.insert(key1, value1);
        skip_list.insert(key2, value2);

        assert_eq!(skip_list.get(&key1), Some(value1));
        assert_eq!(skip_list.get(&key2), Some(value2));
    }

    #[test]
    fn test_insert_and_delete() {
        let mut skip_list = SkipList::new(4);

        let key1 = [1, 1, 1, 1, 1, 1, 1, 1];
        let value1 = [10, 10, 10, 10, 10, 10, 10, 10];

        let key2 = [2, 2, 2, 2, 2, 2, 2, 2];
        let value2 = [20, 20, 20, 20, 20, 20, 20, 20];

        skip_list.insert(key1, value1);
        skip_list.insert(key2, value2);

        assert_eq!(skip_list.get(&key1), Some(value1));

        let deleted_value = skip_list.delete(&key1);
        assert_eq!(deleted_value, Some(value1));
        assert_eq!(skip_list.get(&key1), None);
    }

    #[test]
    fn test_insert_and_delete_nonexistent_key() {
        let mut skip_list = SkipList::new(4);

        let key1 = [1, 1, 1, 1, 1, 1, 1, 1];
        let value1 = [10, 10, 10, 10, 10, 10, 10, 10];

        let key2 = [2, 2, 2, 2, 2, 2, 2, 2];
        let value2 = [20, 20, 20, 20, 20, 20, 20, 20];

        skip_list.insert(key1, value1);

        let deleted_value = skip_list.delete(&key2);
        assert_eq!(deleted_value, None);
    }

	#[test]
	fn random_method_calls(){
		let mut skip_list = SkipList::new(4);
		assert_eq!(skip_list.insert([0,1,2,3,4,5,6,7], [0,0,11,11,1,1,1,1]), None); 
		assert_eq!(skip_list.insert([0,2,2,3,4,5,6,7], [0,4,11,11,1,1,1,1]), None);
		assert_eq!(skip_list.insert([0,3,2,3,4,5,6,7], [0,5,11,11,1,1,1,1]), None);
		assert_eq!(skip_list.delete(&[0,3,22,3,4,5,6,7]), None);
		assert_eq!(skip_list.delete(&[0,3,2,3,4,5,6,7]), Some([0,5,11,11,1,1,1,1]));
		assert_eq!(skip_list.insert([0,2,2,3,4,52,6,7], [0,4,11,111,1,12,1,1]), None);
		assert_eq!(skip_list.insert([0,3,2,3,4,5,36,7], [0,5,11,11,1,31,1,1]), None);
		assert_eq!(skip_list.delete(&[0,2,2,3,4,52,6,7]), Some([0,4,11,111,1,12,1,1]));
		assert_eq!(skip_list.get(&[0,3,2,3,4,5,36,7]), Some([0,5,11,11,1,31,1,1]));

		assert_eq!(skip_list.get(&[0,3,2,3,4,5,6,7]), None);
	}
}