use std::cmp::max;
use twox_hash::xxh3::hash64_with_seed;

pub struct CMSketch {
    table: Box<[Box<[u64]>]>
}

impl CMSketch {
    /// Lower probability (of collisions) means the count of a key is less likely to be impacted by other keys.
    /// Higher tolerance (to error) means the count of some key is sampled more times.
    pub fn new(probability: f64, tolerance: f64) -> Self {
        let hash_func_len = max(
            1,
            ((1.0 - tolerance).ln() / (0.5_f64).ln()).floor() as usize
        );

        let row_len = max(
            2,
            ((std::f64::consts::E / probability).round() as usize)
                .checked_next_power_of_two()
                .unwrap_or(usize::MAX)
        );

        CMSketch {
            table: vec![vec![0; row_len].into_boxed_slice(); hash_func_len].into_boxed_slice()
        }
    }

    /// Increases the count for some given key.
    pub fn increase_count(&mut self, key: &[u8]) {
        for (seed, row) in self.table.iter_mut().enumerate() {
            let hash = hash64_with_seed(key, seed as u64) as usize % row.len();

            row[hash] += 1;
        }
    }

    pub fn get_count(&self, key: &[u8]) -> u64 {
        self.table.iter()
            .enumerate()
            .map(|(seed, row)| {
                let hash = hash64_with_seed(key, seed as u64) as usize % row.len();

                row[hash]
            })
            .min()
            .unwrap_or(0)
    }

    /// Serializes the structure into a stream of bytes.
    pub fn serialize(&self) -> Box<[u8]> {
        let mut structure_bytes = Vec::with_capacity(
        self.table.len() * self.table[0].len() * 8 + 8 + 8
        );

        structure_bytes.extend(self.table.len().to_ne_bytes());
        structure_bytes.extend(self.table[0].len().to_ne_bytes());
        structure_bytes.extend(self.table.iter()
            .flat_map(|row| row.iter())
            .flat_map(|item| item.to_ne_bytes()));

        structure_bytes.into_boxed_slice()
    }

    /// Deserializes the structure from an array of bytes.
    pub fn deserialize(bytes: &[u8]) -> Self {
        let num_rows = usize::from_ne_bytes(bytes[0..8].try_into().unwrap());
        let row_len = usize::from_ne_bytes(bytes[8..16].try_into().unwrap());

        let mut table = Vec::<Box<[u64]>>::new();

        for i in 0..num_rows {
            let start = 16 + i * row_len * 8;
            let end = start + row_len * 8;
            let row_ptr = bytes[start..end].as_ptr() as *const u64;
            let final_slice = unsafe { std::slice::from_raw_parts(row_ptr, row_len) };

            table.push(final_slice.to_vec().into_boxed_slice())
        }

        CMSketch { table: table.into_boxed_slice() }
    }
}