pub mod token_bucket;

#[cfg(test)]
mod tests {
    use crate::token_bucket::TokenBucket;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket_basic() {
        let capacity = 100;
        let refill_rate = 10;
        let mut bucket = TokenBucket::new(capacity, refill_rate);

        assert_eq!(bucket.take(50), true);
        assert_eq!(bucket.take(60), false); // Bucket should be empty

        // Sleep for enough time to refill the bucket
        thread::sleep(Duration::from_secs(6));

        assert_eq!(bucket.take(60), true); // Bucket should be refilled
    }

    #[test]
    fn test_token_bucket_serialization_deserialization() {
        let capacity = 100;
        let refill_rate = 10;
        let mut bucket = TokenBucket::new(capacity, refill_rate);

        // Take some tokens to demonstrate state change
        assert_eq!(bucket.take(50), true);

        let serialized = bucket.serialize();
        let deserialized_bucket = TokenBucket::deserialize(&serialized);

        // Check if deserialized bucket matches original
        assert_eq!(bucket.capacity, deserialized_bucket.capacity);
        assert_eq!(bucket.tokens, deserialized_bucket.tokens);
        assert_eq!(bucket.refill_rate, deserialized_bucket.refill_rate);
    }
}
