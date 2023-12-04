mod memtable;
mod record_iterator;
mod crc_error;
mod mem_pool;
mod insert_error;

pub use mem_pool::MemoryPool;

#[cfg(test)]
mod tests {
    use db_config::DBConfig;
    use segment_elements::TimeStamp;
    use crate::MemoryPool;

    #[test]
    fn test_async_flush() {
        let mut mem_pool = MemoryPool::new(&DBConfig::default()).unwrap();

        for i in 0..10000000u128 {
            print!("{} ", i);
            match mem_pool.insert(&i.to_ne_bytes(), &(i * 2).to_ne_bytes(), TimeStamp::Now) {
                Ok(()) => {},
                Err(e) => println!("{}", e)
            }
        }
    }
}