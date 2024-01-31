use bitvec::prelude::BitVec;
use std::io::Result;
use twox_hash::xxh3::hash64_with_seed;

pub struct BloomFilter {
    data: BitVec,
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
            data: bitvec::bitvec![0; row_len],
            hash_fun_count,
        }
    }

    /// Adds a key to the bloom filter.
    pub fn add(&mut self, key: &[u8]) {
        for i in 0..self.hash_fun_count {
            let hashed_index = hash64_with_seed(key, i as u64) as usize % self.data.len();
            self.data.set(hashed_index, true);
        }
    }

    /// Checks whether the key is likely to be in the Bloom filter.
    pub fn contains(&self, key: &[u8]) -> bool {
        (0..self.hash_fun_count).all(|i| {
            let hashed_index = hash64_with_seed(key, i as u64) as usize % self.data.len();
            self.data[hashed_index]
        })
    }

    /// Serializes the bloom filter into a boxed byte array.
    pub fn serialize(&self) -> Box<[u8]> {
        let total_size = 10 + self.data.len();
        let mut serialized_data = vec![0u8; total_size].into_boxed_slice();

        // The byte 0x01 to indicates that the data is a bloom filter (this is subject to change)
        serialized_data[0] = 0x01;

        // Push the hash_fun_count as a single byte
        serialized_data[1] = self.hash_fun_count;

        // Push 8 bytes for the data length
        let data_len_bytes = (self.data.len() as u64).to_ne_bytes();
        serialized_data[2..10].copy_from_slice(&data_len_bytes);

        let mut current_byte_index = 10;
        let mut bit_counter = 0; // Count bits to write them as single bytes to the array
        let mut current_byte = 0u8;
        for bit in self.data.iter() {
            if *bit {
                current_byte |= 1 << (bit_counter % 8);
            }
            bit_counter += 1;

            if bit_counter % 8 == 0 {
                serialized_data[current_byte_index] = current_byte;
                current_byte_index += 1;
                current_byte = 0;
            }
        }

        // If there are remaining bits in the last byte, write it.
        if bit_counter % 8 != 0 {
            serialized_data[current_byte_index] = current_byte;
        }

        serialized_data
    }

    /// Deserializes the bloom filter from a byte array.
    pub fn deserialize(input: &[u8]) -> Result<BloomFilter> {
        if input.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Bloom filter data is empty on deserialize",
            ));
        }

        // Read and validate the byte indicating the data is a bloom filter (0x01)
        if input[0] != 0x01 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid bloom filter header byte",
            ));
        }

        let hash_fun_count = input[1];

        let mut data_len_bytes = [0u8; 8];
        data_len_bytes.copy_from_slice(&input[2..10]);

        let data_len = u64::from_le_bytes(data_len_bytes);

        let mut data_bytes_count = data_len / 8;
        if data_len % 8 != 0 {
            data_bytes_count += 1;
        }

        let mut data_bytes = vec![0u8; data_bytes_count as usize];
        data_bytes.copy_from_slice(&input[10..(data_bytes_count + 10) as usize]);

        let mut data = BitVec::new();

        /* Count each bit and stop reading when data_len bits are read.
        This is done because data is read in bytes and the last byte can be incomplete
        because the data of the bloom filter is a bit vector. */
        let mut bit_counter = 0;
        for byte in data_bytes.iter() {
            let current_byte = *byte;
            for i in 0..8 {
                if bit_counter >= data_len {
                    break;
                }

                let bit = (current_byte >> i) & 1;
                data.push(bit == 1);
                bit_counter += 1;
            }
        }

        Ok(BloomFilter {
            data,
            hash_fun_count,
        })
    }
}
