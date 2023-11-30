mod memtable;
mod record_iterator;
mod crc_error;
mod mem_pool;
mod insert_error;

pub use mem_pool::MemoryPool;

#[cfg(test)]
mod tests {
    use b_tree::BTree;
    use segment_elements::TimeStamp;
    use crate::MemoryPool;

    #[test]
    fn test_async_flush() {
        let mut mem_pool = MemoryPool::<BTree>::new(10, 1000, 10).unwrap();

        for i in 0..10000000u128 {
            print!("{} ", i);
            match mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now) {
                Ok(()) => {},
                Err(e) => println!("{}", e)
            }
        }
    }
}