use std::cmp::max;
use std::cmp::min;
use twox_hash::xxh3::hash64;

pub struct HLL {
    buckets: Box<[u8]>,
    precision: u32,
    constant: f64,
}

impl HLL {
    /// `precision` - corresponds to the number of buckets used.
    /// More buckets reduces error at the expense of memory.
    /// Precision should be an integer value between 4 and 16.
    pub fn new(precision: u32) -> Self {
        let buckets_len = 2usize.pow(precision.clamp(4, 16));

        HLL {
            constant: 0.7213 * buckets_len as f64 / (buckets_len as f64 + 1.079),
            buckets: vec![0; buckets_len].into_boxed_slice(),
            precision: precision.clamp(4, 16),
        }
    }

    /// Adds the given key to the count.
    pub fn add_to_count(&mut self, key: &[u8]) {
        let hash = hash64(key);
        let index = (hash >> (64 - self.precision)) as usize;
        let trailing_zeros = min(64 - self.precision, hash.trailing_zeros()) + 1;

        self.buckets[index] = max(self.buckets[index], trailing_zeros as u8);
    }

    /// Returns the estimated count of different elements.
    pub fn get_count(&self) -> u64 {
        let harmonic_mean = self
            .buckets
            .iter()
            .map(|bucket| 1f64 / 2u64.pow(*bucket as u32) as f64)
            .sum::<f64>();

        (self.constant * self.buckets.len() as f64 * (self.buckets.len() as f64 / harmonic_mean))
            .round() as u64
    }

    /// Serializes the structure into a stream of bytes.
    pub fn serialize(&self) -> Box<[u8]> {
        let mut serialized_data = Vec::with_capacity(8 + 4 + 8 + self.buckets.len());

        serialized_data.extend(self.constant.to_ne_bytes());
        serialized_data.extend(self.precision.to_ne_bytes());
        serialized_data.extend(self.buckets.len().to_ne_bytes());
        serialized_data.extend_from_slice(&self.buckets);

        serialized_data.into_boxed_slice()
    }

    /// Deserializes the structure from an array of bytes.
    pub fn deserialize(bytes: &[u8]) -> Self {
        let constant = f64::from_ne_bytes(bytes[0..8].try_into().unwrap());
        let precision = u32::from_ne_bytes(bytes[8..12].try_into().unwrap());
        let buckets_len = usize::from_ne_bytes(bytes[12..20].try_into().unwrap());

        let buckets =
            unsafe { std::slice::from_raw_parts(bytes[20..].as_ptr(), buckets_len) }.to_vec();

        HLL {
            buckets: buckets.into_boxed_slice(),
            precision,
            constant,
        }
    }
}
