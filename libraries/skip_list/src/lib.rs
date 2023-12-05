mod skip_list;
mod skip_list_node;
mod skip_list_iterator;

pub use skip_list::SkipList;

#[cfg(test)]
mod tests {
    use peak_alloc::PeakAlloc;
    use segment_elements::{SegmentTrait, TimeStamp};
    use super::*;

    #[global_allocator]
    static PEAK_ALLOC: PeakAlloc = PeakAlloc;

    #[test]
    fn test_insert_and_get() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10], TimeStamp::Now);
        skip_list.insert(&[2], &[20], TimeStamp::Now);
        skip_list.insert(&[3], &[30], TimeStamp::Now);

        assert_eq!(skip_list.get(&[1]), Some(Box::from([10].to_vec())));
        assert_eq!(skip_list.get(&[2]), Some(Box::from([20].to_vec())));
        assert_eq!(skip_list.get(&[3]), Some(Box::from([30].to_vec())));
    }

    #[test]
    fn test_delete1() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10], TimeStamp::Now);
        skip_list.insert(&[2], &[20], TimeStamp::Now);
        skip_list.insert(&[3], &[30], TimeStamp::Now);

        assert!(!skip_list.delete(&[2], TimeStamp::Now));
        assert_eq!(skip_list.get(&[2]), None);
    }

    #[test]
    fn test_delete2() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10], TimeStamp::Now);

        assert!(skip_list.delete(&[2], TimeStamp::Now));
    }

    #[test]
    fn test_insert_duplicate_key() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10], TimeStamp::Now);
        skip_list.insert(&[1], &[100], TimeStamp::Now); // Duplicate key

        assert_eq!(skip_list.get(&[1]), Some(Box::from([100].to_vec())));
    }

    #[test]
    fn test_empty_skiplist() {
        let mut skip_list = SkipList::new(4);

        assert_eq!(skip_list.get(&[1]), None);
        assert!(skip_list.delete(&[1], TimeStamp::Now));
    }

    #[test]
    fn test_insert_and_get_more() {
        let max_level = 16;
        let mut skip_list = SkipList::new(max_level);

        for i in 0..30i32 {
            let key = (i).to_ne_bytes();
            let value = (i * 2).to_ne_bytes();
            skip_list.insert(&key, &value, TimeStamp::Now);
        }

        for i in 0..30i32 {
            let key = (i).to_ne_bytes();
            let value = (i * 2).to_ne_bytes();
            let result = skip_list.get(&key);
            assert_eq!(result, Some(Box::from(value)));
        }
    }

    #[test]
    fn test_insert_return_value() {
        let max_level = 16;
        let mut skip_list = SkipList::new(max_level);

        assert!(skip_list.insert(&[1], &[1], TimeStamp::Now));
        assert!(skip_list.insert(&[2], &[2], TimeStamp::Now));
        assert!(!skip_list.insert(&[1], &[3], TimeStamp::Now));

        assert_eq!(Some(Box::from([3].to_vec())), skip_list.get(&[1]));
    }

    #[test]
    fn test_delete_insert_length() {
        let max_level = 16;
        let mut skip_list = SkipList::new(max_level);

        for i in 0..40i32 {
            let key = (i).to_ne_bytes();
            let value = (i * 2).to_ne_bytes();
            skip_list.insert(&key, &value, TimeStamp::Now);
        }
        
        for i in 25..45i32 {
            skip_list.delete(&(i).to_ne_bytes(), TimeStamp::Now);
        }

        assert_eq!(45, skip_list.get_length());
        assert_eq!(skip_list.get(&20i32.to_ne_bytes()), Some(Box::from(40i32.to_ne_bytes())));
    }

    #[test]
    fn iterator_test() {
        let max_level = 8;
        let mut skip_list = SkipList::new(max_level);

        skip_list.insert(&[4], &[1], TimeStamp::Now);
        skip_list.insert(&[22], &[2], TimeStamp::Now);
        skip_list.insert(&[11], &[3], TimeStamp::Now);

        let mut iterator = skip_list.iter();
        assert_eq!(&[4], iterator.next().unwrap().lock().unwrap().get_key());
        assert_eq!(&[11], iterator.next().unwrap().lock().unwrap().get_key());
        assert_eq!(&[22], iterator.next().unwrap().lock().unwrap().get_key());
    }

    #[test]
    #[ignore]
    fn test_memory() {
        let mut s = SkipList::new(10);

        for i in 0..100000u128 {
            s.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

        println!("Current mem usage with full skiplist: {}MB", PEAK_ALLOC.current_usage_as_mb());

        s.empty();

        println!("Current mem usage with empty skiplist: {}MB", PEAK_ALLOC.current_usage_as_mb());
    }
}