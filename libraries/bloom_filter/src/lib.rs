mod bloom_filter;

pub use bloom_filter::BloomFilter;

#[cfg(test)]
mod tests {
    use super::BloomFilter;

    #[test]
    fn test_add_and_contains() {
        let mut bloom_filter = BloomFilter::new(0.01, 100_00_00);

        bloom_filter.add(&[1, 2, 3, 4]);

        assert!(bloom_filter.contains(&[1, 2, 3, 4]));
        assert!(!bloom_filter.contains(&[5, 6, 7, 8]));
    }

    #[test]
    fn test_contains_after_add() {
        let mut bloom_filter = BloomFilter::new(0.01, 100_00_00);

        assert!(!bloom_filter.contains(&[1, 2, 3, 4]));

        bloom_filter.add(&[1, 2, 3, 4]);

        assert!(bloom_filter.contains(&[1, 2, 3, 4]));
        assert!(!bloom_filter.contains(&[5, 6, 7, 8]));
    }

    #[test]
    fn test_contains_empty() {
        let bloom_filter = BloomFilter::new(0.01, 100_00_00);

        assert!(!bloom_filter.contains(&[1, 2, 3, 4]));
        assert!(!bloom_filter.contains(&[5, 6, 7, 8]));
    }

    #[test]
    fn test_serialize_deserialize_contains() {
        let mut bloom_filter = BloomFilter::new(0.01, 100_00_00);

        bloom_filter.add(&[1, 2, 3, 4]);

        let second_bloom_filter =
            BloomFilter::deserialize(&bloom_filter.serialize()).expect("Failed to deserialize");
        assert!(bloom_filter.contains(&[1, 2, 3, 4]));
        assert!(second_bloom_filter.contains(&[1, 2, 3, 4]));
    }

    #[test]
    fn test_serialize_deserialize_does_not_contain() {
        let mut bloom_filter = BloomFilter::new(0.01, 100_00_00);

        bloom_filter.add(&[1, 2, 3, 4]);

        let second_bloom_filter =
            BloomFilter::deserialize(&bloom_filter.serialize()).expect("Failed to deserialize");
        assert!(!second_bloom_filter.contains(&[5, 6, 7, 8]));
    }
}
