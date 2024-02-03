mod skip_list;
mod skip_list_iterator;
mod skip_list_node;

pub use skip_list::SkipList;

#[cfg(test)]
mod tests {
    use super::*;
    use peak_alloc::PeakAlloc;
    use segment_elements::{SegmentTrait, TimeStamp};

    #[global_allocator]
    static PEAK_ALLOC: PeakAlloc = PeakAlloc;

    #[test]
    fn test_insert_and_get() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10], TimeStamp::Now);
        skip_list.insert(&[2], &[20], TimeStamp::Now);
        skip_list.insert(&[3], &[30], TimeStamp::Now);

        assert_eq!(
            skip_list.get(&[1]).unwrap().get_value(),
            Box::from([10].to_vec())
        );
        assert_eq!(
            skip_list.get(&[2]).unwrap().get_value(),
            Box::from([20].to_vec())
        );
        assert_eq!(
            skip_list.get(&[3]).unwrap().get_value(),
            Box::from([30].to_vec())
        );
    }

    #[test]
    fn test_delete1() {
        let mut skip_list = SkipList::new(4);

        skip_list.insert(&[1], &[10], TimeStamp::Now);
        skip_list.insert(&[2], &[20], TimeStamp::Now);
        skip_list.insert(&[3], &[30], TimeStamp::Now);

        assert!(!skip_list.delete(&[2], TimeStamp::Now));
        assert_eq!(skip_list.get(&[2]).unwrap().get_value(), Box::from([]));
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

        assert_eq!(
            skip_list.get(&[1]).unwrap().get_value(),
            Box::from([100].to_vec())
        );
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
            let key = i.to_ne_bytes();
            let value = (i * 2).to_ne_bytes();
            skip_list.insert(&key, &value, TimeStamp::Now);
        }

        for i in 0..30i32 {
            let key = i.to_ne_bytes();
            let value = (i * 2).to_ne_bytes();
            let result = skip_list.get(&key);
            assert_eq!(result.unwrap().get_value(), Box::from(value));
        }
    }

    #[test]
    fn test_insert_return_value() {
        let max_level = 16;
        let mut skip_list = SkipList::new(max_level);

        assert!(skip_list.insert(&[1], &[1], TimeStamp::Now));
        assert!(skip_list.insert(&[2], &[2], TimeStamp::Now));
        assert!(!skip_list.insert(&[1], &[3], TimeStamp::Now));

        assert_eq!(
            Box::from([3].to_vec()),
            skip_list.get(&[1]).unwrap().get_value()
        );
    }

    #[test]
    fn test_delete_insert_length() {
        let max_level = 16;
        let mut skip_list = SkipList::new(max_level);

        for i in 0..40i32 {
            let key = i.to_ne_bytes();
            let value = (i * 2).to_ne_bytes();
            skip_list.insert(&key, &value, TimeStamp::Now);
        }

        for i in 25..45i32 {
            skip_list.delete(&i.to_ne_bytes(), TimeStamp::Now);
        }

        assert_eq!(45, skip_list.get_length());
        assert_eq!(
            skip_list.get(&20i32.to_ne_bytes()).unwrap().get_value(),
            Box::from(40i32.to_ne_bytes())
        );
    }

    #[test]
    fn iterator_test() {
        for max_level in 5..12 {
            let mut skip_list = SkipList::new(max_level);

            for i in 0..255u32 {
                assert!(skip_list.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now));
            }
            let iterator = skip_list.iter();

            let mut i: u32 = 0;
            for entry in iterator {
                let key = entry.0;
                let entry = entry.1;
                assert_eq!(<[u8; 4] as Into<Box<[u8]>>>::into(i.to_ne_bytes()), key);
                i += 1;
                println!("{:?}", key);
            }
        }
    }

    #[test]
    fn test_iterator() {
        let mut s = SkipList::new(3);

        for i in -100..100i32 {
            s.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

        let mut prev_key: Box<[u8]> = Box::from([]);
        for (key_bytes, _) in s.iter() {
            if key_bytes < prev_key {
                panic!("")
            }

            let el_int = i32::from_ne_bytes(<[u8; 4]>::try_from(&*key_bytes).unwrap());
            println!("{}", el_int);

            prev_key = key_bytes;
        }
    }

    #[test]
    fn test_iterator_string() {
        let mut s = SkipList::new(3);

        let base_key = "test_key_";

        for i in -100..100i32 {
            let key = format!("{}{}", base_key, i.to_string());
            s.insert(key.as_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now);
        }

        let mut prev_key: Box<[u8]> = Box::from([]);
        for (key_bytes, _) in s.iter() {
            if key_bytes < prev_key {
                panic!("")
            }

            let el_s = String::from_utf8_lossy(&key_bytes);
            println!("{}", el_s);

            prev_key = key_bytes;
        }
    }
}
