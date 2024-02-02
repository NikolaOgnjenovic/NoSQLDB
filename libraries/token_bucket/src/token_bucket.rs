use segment_elements::TimeStamp;

pub struct TokenBucket {
    pub(crate) capacity: usize, // Pub for tests
    pub(crate) tokens: usize,
    last_refill_time: TimeStamp,
    pub(crate) refill_rate: usize, // Tokens per second
}

impl Default for TokenBucket {
    fn default() -> Self {
        let now = TimeStamp::Now;
        TokenBucket {
            capacity: 100, // Set default capacity
            tokens: 100,   // Set default tokens
            last_refill_time: TimeStamp::Custom(now.get_time()), // Set default last refill time
            refill_rate: 10, // Set default refill rate
        }
    }
}

impl TokenBucket {
    pub fn new(capacity: usize, refill_rate: usize) -> Self {
        let now = TimeStamp::Now;
        TokenBucket {
            capacity,
            tokens: capacity,
            last_refill_time: TimeStamp::Custom(now.get_time()),
            refill_rate,
        }
    }

    pub fn take(&mut self, tokens: usize) -> bool {
        self.refill_tokens();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill_tokens(&mut self) {
        let now = TimeStamp::Now;
        let last_refill_micros = match self.last_refill_time {
            TimeStamp::Now => panic!("Cannot use TimeStamp::Now as last refill time"),
            TimeStamp::Custom(time) => time,
        };
        let current_micros = now.get_time();
        let elapsed_micros = current_micros - last_refill_micros;
        let seconds_elapsed = elapsed_micros as f64 / 1_000_000.0;

        let tokens_to_add = (self.refill_rate as f64 * seconds_elapsed) as usize;
        self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
        self.last_refill_time = TimeStamp::Custom(now.get_time());
    }

    pub fn serialize(&self) -> Box<[u8]> {
        let mut serialized_data = Vec::with_capacity(8 + 8 + 8 + 8);

        serialized_data.extend(self.capacity.to_ne_bytes());
        serialized_data.extend(self.tokens.to_ne_bytes());
        serialized_data.extend(self.last_refill_time.get_time().to_ne_bytes());
        serialized_data.extend(self.refill_rate.to_ne_bytes());

        serialized_data.into_boxed_slice()
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let capacity = usize::from_ne_bytes(bytes[0..8].try_into().unwrap());
        let tokens = usize::from_ne_bytes(bytes[8..16].try_into().unwrap());
        let last_refill_time = TimeStamp::Custom(u128::from_ne_bytes(bytes[16..32].try_into().unwrap()));
        let refill_rate = usize::from_ne_bytes(bytes[32..40].try_into().unwrap());

        TokenBucket {
            capacity,
            tokens,
            last_refill_time,
            refill_rate,
        }
    }
}