use std::cmp::Ordering;
use std::io::Result;
use twox_hash::xxh3::hash64_with_seed;

pub struct BloomFilter {
    data: Vec<u8>,
    hash_fun_count: u8,
}

impl BloomFilter {
    /// Creates a new Bloom filter with a given false positive probability and capacity.
    ///
    /// The `probability` parameter represents the desired false positive probability,
    /// and the `cap` parameter is the expected maximum number of elements in the filter.
    pub fn new(probability: f64, cap: usize) -> Self {
        let row_len = (-(cap as f64 * probability.ln()) / (2_f64.ln() * 2_f64.ln())) as usize;
        let hash_fun_count = ((row_len / cap) as f64 * 2_f64.ln()) as u8;
        BloomFilter {
            data: vec![0; row_len],
            //data: bitvec::bitvec![0; row_len],
            hash_fun_count,
        }
    }

    /// Adds a key to the bloom filter.
    pub fn add(&mut self, key: &[u8]) {
        for i in 0..self.hash_fun_count {
            let hashed_index = hash64_with_seed(key, i as u64) as usize % self.data.len();
            self.data[hashed_index] = 1u8;
            //self.data.set(hashed_index, true);
        }
    }

    /// Checks whether the key is likely to be in the Bloom filter.
    pub fn contains(&self, key: &[u8]) -> bool {
        (0..self.hash_fun_count).all(|i| {
            let hashed_index = hash64_with_seed(key, i as u64) as usize % self.data.len();
            self.data[hashed_index].cmp(&1u8) == Ordering::Equal
        })
    }

    pub fn serialize(&self) -> Box<[u8]> {
        let total_size = 9 + self.data.len();
        let mut serialized_data = vec![0u8; total_size].into_boxed_slice();

        // Push the hash_fun_count as a single byte
        serialized_data[0] = self.hash_fun_count;

        // Push 8 bytes for the data length
        let data_len_bytes = (self.data.len() as u64).to_ne_bytes();
        serialized_data[1..9].copy_from_slice(&data_len_bytes);

        // Copy the data bytes directly
        serialized_data[9..].copy_from_slice(&self.data);

        serialized_data
    }

    pub fn deserialize(input: &[u8]) -> Result<BloomFilter> {
        if input.len() < 9 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Bloom filter data is too short on deserialize",
            ));
        }

        let hash_fun_count = input[0];

        let mut data_len_bytes = [0u8; 8];
        data_len_bytes.copy_from_slice(&input[1..9]);

        let data_len = usize::from_ne_bytes(data_len_bytes);

        if input.len() != 9 + data_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid bloom filter length",
            ));
        }

        let data = input[9..].to_vec();

        Ok(BloomFilter { data, hash_fun_count })
    }
}
