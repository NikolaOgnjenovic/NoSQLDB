mod dll_node;
mod doubly_linked_list;
mod lru_cache;

pub use lru_cache::LRUCache;

#[cfg(test)]
mod tests {
    use super::*;
    use segment_elements::{MemoryEntry, TimeStamp};

    #[test]
    fn test_insert_and_get_elements() {
        let mut lru = LRUCache::new(1000);
        for i in 0..500_u32 {
            lru.insert(
                &i.to_ne_bytes(),
                Some(MemoryEntry::from(
                    &(2 * i).to_ne_bytes(),
                    false,
                    TimeStamp::Now.get_time(),
                )),
            );
            let newest = lru.get(&i.to_ne_bytes());
            if let Some(element) = newest {
                if let Some(node) = lru.list.peak_head() {
                    let actual = node.borrow().el.mem_entry.clone();
                    assert_eq!(actual, element);
                }
            }
        }
        assert_eq!(500, lru.get_size());

        for i in 250..550u32 {
            let newest = lru.get(&i.to_ne_bytes());
            if let Some(element) = newest {
                if let Some(node) = lru.list.peak_head() {
                    let actual = node.borrow().el.mem_entry.clone();
                    assert_eq!(actual, element);
                }
            }
        }
    }

    #[test]
    fn test_insert_and_get_elements_string() {
        let mut lru = LRUCache::new(1000);

        let base_key = "test_key";
        let base_value = "test_value";

        for i in 0..500 {
            let key = format!("{}{}", base_key, i.to_string());
            let value = format!("{}{}", base_value, i.to_string());

            lru.insert(
                key.as_bytes(),
                Some(MemoryEntry::from(
                    value.as_bytes(),
                    false,
                    TimeStamp::Now.get_time(),
                )),
            );

            let newest = lru.get(key.as_bytes());
            if let Some(element) = newest {
                if let Some(node) = lru.list.peak_head() {
                    let actual = node.borrow().el.mem_entry.clone();
                    assert_eq!(actual, element);
                }
            }
        }
        assert_eq!(500, lru.get_size());

        for i in 250..550 {
            let key = format!("{}{}", base_key, i.to_string());

            let newest = lru.get(key.as_bytes());
            if let Some(element) = newest {
                if let Some(node) = lru.list.peak_head() {
                    let actual = node.borrow().el.mem_entry.clone();
                    assert_eq!(actual, element);
                }
            }
        }
    }

    #[test]
    fn test_removing_oldest() {
        for capacity in 450..650 {
            let mut lru = LRUCache::new(capacity);
            let upper_bound = (capacity + 1) as u32;
            for i in 0..upper_bound {
                lru.insert(
                    &i.to_ne_bytes(),
                    Some(MemoryEntry::from(
                        &(2 * i).to_ne_bytes(),
                        false,
                        TimeStamp::Now.get_time(),
                    )),
                );
            }

            let oldest = if !lru.list.tail.is_none() {
                lru.list.tail.clone()
            } else {
                None
            };

            if let Some(node_ptr) = oldest {
                let memory_entry = node_ptr.borrow().el.mem_entry.clone();
                assert_eq!(memory_entry, lru.get(&1_u32.to_ne_bytes()).unwrap())
            }
            assert_eq!(capacity, lru.get_size());
        }
    }

    #[test]
    fn test_update_elements() {
        let mut lru = LRUCache::new(500);

        for i in 0..300u32 {
            lru.insert(
                &i.to_ne_bytes(),
                Some(MemoryEntry::from(
                    &(2 * i).to_ne_bytes(),
                    false,
                    TimeStamp::Now.get_time(),
                )),
            );
        }
        for i in 200..350u32 {
            lru.insert(
                &i.to_ne_bytes(),
                Some(MemoryEntry::from(
                    &(3 * i).to_ne_bytes(),
                    false,
                    TimeStamp::Now.get_time(),
                )),
            );
        }
        assert_eq!(350, lru.get_size());
        for i in 200..300u32 {
            let newest = lru.get(&i.to_ne_bytes());
            assert_eq!(
                <[u8; 4] as Into<Box<[u8]>>>::into((3 * i).to_ne_bytes()),
                newest.unwrap().get_value()
            );
        }
    }
}
