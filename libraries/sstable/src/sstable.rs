use std::cmp::{max, min};
use std::fs::{create_dir_all, OpenOptions};
use std::io;
use std::io::{BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use bloom_filter::BloomFilter;
use segment_elements::MemoryEntry;
use merkle_tree::merkle_tree::MerkleTree;

/// Struct representing an SSTable (Sorted String Table) for storing key-value pairs on disk.
pub struct SSTable<'a> {
    // Base directory path where the SSTable files will be stored.
    dir: &'a Path,
    // In-memory segment containing key-value pairs.
    inner_mem: &'a Box<dyn segment_elements::SegmentTrait + Send>,
    // Flag indicating whether to store data in a single file or multiple files.
    in_single_file: bool,
    // Offset for data file when storing in single file.
    data_offset: usize,
    // Offset for index file when storing in single file.
    index_offset: usize,
    // Offset for summary file when storing in single file.
    summary_offset: usize,
    // Offset for Bloom filter file when storing in single file.
    bloom_filter_offset: usize,
    // Offset for Merkle tree file when storing in single file.
    merkle_offset: usize
}

impl<'a> SSTable<'a> {
    /// Creates a new SSTable instance.
    ///
    /// # Arguments
    ///
    /// * `dir` - The base directory path where SSTable files will be stored.
    /// * `inner_mem` - In-memory segment containing key-value pairs.
    /// * `in_single_file` - Flag indicating whether to store data in a single file or multiple files.
    ///
    /// # Returns
    ///
    /// A Result containing the initialized SSTable instance or an IO error.
    pub fn new(dir: &'a Path, inner_mem: &'a Box<dyn segment_elements::SegmentTrait + Send>, in_single_file: bool) -> io::Result<SSTable<'a>> {
        // Create directory if it doesn't exist
        create_dir_all(dir)?;

        let sstable = Self {
            dir,
            inner_mem,
            in_single_file,
            data_offset: 0,
            index_offset: 0,
            summary_offset: 0,
            bloom_filter_offset: 0,
            merkle_offset: 0
        };

        Ok(sstable)
    }

    /// Flushes the in-memory segment to the SSTable files on disk.
    ///
    /// # Arguments
    ///
    /// * `summary_density` - The density parameter for creating the summary.
    ///
    /// # Returns
    ///
    /// A Result indicating success or an IO error.
    pub fn flush(&mut self, summary_density: usize) -> io::Result<()> {
        let (data, index_builder, bloom_filter) = self.build_data_and_index_and_filter()?;
        let index_summary = self.build_summary(&index_builder, summary_density);
        let serialized_merkle_tree = MerkleTree::new(&data).serialize();

        if self.in_single_file {
           self.write_to_single_file(&data, &index_builder, &index_summary, &bloom_filter, &serialized_merkle_tree);
        } else {
            self.write_to_index_file(&index_builder)?;
            self.write_to_filter_file(&bloom_filter)?;
            self.write_to_summary_file(&index_summary)?;
            self.write_to_data_file(&data)?;
            self.write_to_merkle_file(&serialized_merkle_tree)?;
        }

        Ok(())
    }

    /// Builds the SSTable data, index builder and Bloom Filter using self.inner_mem.
    ///
    /// # Returns
    ///
    /// A Result that contains:
    /// a tuple consisting of a data Vec<u8>, an index builder key pair Vec<(Vec<u8>, u64)> and a Bloom filter,
    /// or an IO error.
    fn build_data_and_index_and_filter(&self) -> io::Result<(Vec<u8>, Vec<(Vec<u8>, u64)>, BloomFilter)> {
        let mut index_builder = Vec::new();
        let mut bloom_filter = BloomFilter::new(0.01, 100_000);
        let mut data = Vec::new();

        let mut offset: u64 = 0;
        for (key, entry) in self.inner_mem.iterator() {
            let entry_data = entry.serialize(&key);
            let entry_len = entry_data.len().to_ne_bytes();

            data.extend_from_slice(&entry_len);
            data.extend_from_slice(&entry_data);
            index_builder.push((key.to_vec(), offset));
            bloom_filter.add(&key);

            offset += entry_len.len() as u64 + entry_data.len() as u64;
        }

        Ok((data, index_builder, bloom_filter))
    }

    /// Builds the index
    ///
    /// # Arguments
    /// * `index_builder` - An array of key, offset pairs.
    /// * `summary_density` - The density parameter for creating the summary.
    ///
    /// # Returns
    ///
    /// A vector of bytes which represent the index summary.
    fn build_summary(&self, index_builder: &[(Vec<u8>, u64)], summary_density: usize) -> Vec<u8> {
        if index_builder.len() == 0 || summary_density < 1 {
            return Vec::new();
        }

        let mut summary = Vec::new();
        // Find the min and max keys
        let (min_key, _) = index_builder.first().unwrap();
        let (max_key, _) = index_builder.last().unwrap();

        // Add the min and max keys to the summary
        summary.extend_from_slice(&min_key.len().to_ne_bytes());
        summary.extend_from_slice(min_key);
        summary.extend_from_slice(&max_key.len().to_ne_bytes());
        summary.extend_from_slice(max_key);

        // Add every step-th key and its offset to the summary
        for i in (0..index_builder.len()).step_by(summary_density) {
            let (key, _) = &index_builder[i];
            summary.extend_from_slice(&key.len().to_ne_bytes());
            summary.extend_from_slice(key);
            let offset_index = if i == 0 {
                0
            } else {
                (key.len() + key.len().to_ne_bytes().len() + std::mem::size_of::<u64>()) * i
            };
            summary.extend_from_slice(&offset_index.to_ne_bytes());
        }

        summary
    }

    /// Writes the SSTable data, index, summary, Bloom Filter, and Merkle tree to a single file.
    ///
    /// # Arguments
    ///
    /// * `data` - The serialized data to be written.
    /// * `index_builder` - The key, offset pairs for the index.
    /// * `index_summary` - The serialized index summary.
    /// * `bloom_filter` - The Bloom filter.
    /// * `serialized_merkle_tree` - The serialized Merkle tree.
    fn write_to_single_file(&mut self, data: &[u8], index_builder: &[(Vec<u8>, u64)], index_summary: &[u8], bloom_filter: &BloomFilter, serialized_merkle_tree: &Box<[u8]>) -> io::Result<()> {
        let mut file = self.open_buf_writer("-SSTable.db", false)?;
        let total_offset = 5 * std::mem::size_of::<usize>();
        self.data_offset = total_offset;
        Ok(())
    }

    /// Writes the serialized data to the data file.
    ///
    /// # Arguments
    ///
    /// * `data` - The serialized data to be written.
    fn write_to_data_file(&self, data: &[u8]) -> io::Result<()> {
        let mut data_file = self.open_buf_writer("-Data.db", false)?;

        data_file.write_all(&data)?;

        Ok(())
    }

    /// Writes the serialized Merkle tree to the Merkle tree file.
    ///
    /// # Arguments
    ///
    /// * `serialized_merkle_tree` - The serialized Merkle tree.
    fn write_to_merkle_file(&self, serialized_merkle_tree: &[u8]) -> io::Result<()> {
        let mut merkle_file = self.open_buf_writer("-Merkle.db", false)?;

        merkle_file.write_all(&serialized_merkle_tree)?;

        Ok(())
    }

    /// Writes the index entries to the index file.
    ///
    /// # Arguments
    ///
    /// * `index_entries` - The key, offset pairs for the index.
    fn write_to_index_file(&self, index_entries: &[(Vec<u8>, u64)]) -> io::Result<()> {
        let mut index_file = self.open_buf_writer("-Index.db", false)?;

        for (key, offset) in index_entries {
            // Write the key length followed by the key
            index_file.write_all(&key.len().to_ne_bytes())?;
            index_file.write_all(key)?;
            index_file.write_all(&offset.to_ne_bytes())?;
        }

        Ok(())
    }

    /// Writes the index summary to the summary file.
    ///
    /// # Arguments
    ///
    /// * `index_summary` - The serialized index summary.
    fn write_to_summary_file(&self, index_summary: &[u8]) -> io::Result<()> {
        let mut summary_file = self.open_buf_writer("-Summary.db", false)?;

        summary_file.write_all(index_summary)?;

        Ok(())
    }

    /// Writes the serialized Bloom filter to the Bloom filter file.
    ///
    /// # Arguments
    ///
    /// * `bloom_filter` - The Bloom filter.
    fn write_to_filter_file(&self, bloom_filter: &BloomFilter) -> io::Result<()> {
        let mut filter_file = self.open_buf_writer("-Filter.db", false)?;
        filter_file.write_all(&bloom_filter.serialize())?;

        Ok(())
    }

    /// Retrieves a MemoryEntry corresponding to the given key if it exists in the SSTable.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for in the SSTable.
    ///
    /// # Returns
    ///
    /// An Option containing the MemoryEntry if the key is found, otherwise None.
    pub fn get(&self, key: &[u8]) -> Option<MemoryEntry> {
        if self.is_key_in_bloom_filter(key).unwrap_or(false) {
            if let Some(offset) = self.find_offset_in_summary(key) {
                return self.read_entry_from_data_file(offset);
            }
        }

        None
    }

    /// Checks if the given key is likely present in the Bloom filter.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check in the Bloom filter.
    ///
    /// # Returns
    ///
    /// A Result containing a boolean indicating whether the key is likely present or an IO error.
    fn is_key_in_bloom_filter(&self, key: &[u8]) -> io::Result<bool> {
        // Read the entire filter file into memory (assuming it's not too large)
        let mut filter_data = Vec::new();

        let filter_file = self.open_buf_writer("-Filter.db", true)?;
        filter_file.get_ref().read_to_end(&mut filter_data).unwrap();

        // Use the deserialize method of BloomFilter
        let bloom_filter = BloomFilter::deserialize(&filter_data).unwrap();

        Ok(bloom_filter.contains(key))
    }

    /// Finds the offset of the given key in the SSTable based on the index summary.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to find in the SSTable.
    ///
    /// # Returns
    ///
    /// An Option containing the offset if the key is found, otherwise None.
    fn find_offset_in_summary(&self, key: &[u8]) -> Option<u64> {
        // Open the summary file for reading
        let summary_file = self.open_buf_writer("-Summary.db", true).ok()?;
        let mut summary_file_ref = summary_file.get_ref();

        // Read the min key length and min key from the summary file
        let mut min_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        summary_file_ref.read_exact(&mut min_key_len_bytes).unwrap();
        let min_key_len = usize::from_ne_bytes(min_key_len_bytes);
        let mut min_key = vec![0u8; min_key_len];
        summary_file_ref.read_exact(&mut min_key).unwrap();

        // Read the max key length and max key from the summary file
        let mut max_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        summary_file_ref.read_exact(&mut max_key_len_bytes).unwrap();
        let max_key_len = usize::from_ne_bytes(max_key_len_bytes);
        let mut max_key = vec![0u8; max_key_len];
        summary_file_ref.read_exact(&mut max_key).unwrap();

        // Check if the key is within the range of the lowest and highest keys in the summary
        if key < min_key.as_slice() || key > max_key.as_slice() {
            return None;
        }

        let mut current_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        let mut previous_offset_bytes = [0u8; 8];
        while summary_file_ref.read_exact(&mut current_key_len_bytes).is_ok() {
            let current_key_len = usize::from_ne_bytes(current_key_len_bytes);

            let mut current_key_bytes = vec![0u8; current_key_len];
            summary_file_ref.read_exact(&mut current_key_bytes).unwrap();

            let mut offset_bytes = [0u8; 8]; //u64
            summary_file_ref.read_exact(&mut offset_bytes).unwrap();

            // Key < current key, read starting from previous offset
            if key < current_key_bytes.as_slice() {
                let previous_offset = u64::from_ne_bytes(previous_offset_bytes);

                return self.read_data_offset_from_index_file(previous_offset, key);
            }

            previous_offset_bytes = offset_bytes;
        }

        let previous_offset = u64::from_ne_bytes(previous_offset_bytes);
        return self.read_data_offset_from_index_file(previous_offset, key);
    }

    /// Reads the data offset from the index file based on the seek offset and key.
    ///
    /// # Arguments
    ///
    /// * `seek_offset` - The offset to seek in the index file.
    /// * `key` - The key to find in the index file.
    ///
    /// # Returns
    ///
    /// An Option containing the data offset if the key is found, otherwise None.
    fn read_data_offset_from_index_file(&self, seek_offset: u64, key: &[u8]) -> Option<u64> {
        // Seek to the calculated position
        let mut index_file = self.open_buf_writer("-Index.db", true).unwrap();

        // Use the Seek trait to set the position
        index_file.seek(io::SeekFrom::Start(seek_offset)).unwrap();

        let mut current_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        let mut result = index_file.get_ref().read_exact(&mut current_key_len_bytes);
        while result.is_ok() {
            let current_key_len = usize::from_ne_bytes(current_key_len_bytes);
            let mut current_key_bytes = vec![0u8; current_key_len];
            index_file.get_ref().read_exact(&mut current_key_bytes).unwrap();

            let mut offset_bytes = [0u8; 8]; //u64
            index_file.get_ref().read_exact(&mut offset_bytes).unwrap();
            if key == &current_key_bytes {
                return Some(u64::from_ne_bytes(offset_bytes));
            }

            result = index_file.get_ref().read_exact(&mut current_key_len_bytes);
        }

        // Key not found
        None
    }

    /// Reads the MemoryEntry from the data file based on the given offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset in the data file to read the MemoryEntry from.
    ///
    /// # Returns
    ///
    /// An Option containing the MemoryEntry if successful, otherwise None.
    fn read_entry_from_data_file(&self, offset: u64) -> Option<MemoryEntry> {
        let mut data_file = self.open_buf_writer("-Data.db", true).unwrap();
        data_file.seek(io::SeekFrom::Start(offset)).unwrap();

        let mut entry_len_bytes = [0u8; std::mem::size_of::<usize>()];
        data_file.get_ref().read_exact(&mut entry_len_bytes).unwrap();
        let entry_len = usize::from_ne_bytes(entry_len_bytes);

        let mut entry_bytes = vec![0u8; entry_len];
        data_file.get_ref().read_exact(&mut entry_bytes).unwrap();

        // Deserialize the entry bytes
        match MemoryEntry::deserialize(&entry_bytes) {
            Ok(entry) => Some(entry.1),
            Err(_) => None,
        }
    }

    /// Opens a buffered writer for the specified file name, with an option to enable reading.
    ///
    /// # Arguments
    ///
    /// * `file_name` - The name of the file to open.
    /// * `read` - A flag indicating whether reading should be enabled.
    ///
    /// # Returns
    ///
    /// A Result containing the opened BufWriter or an IO error.
    fn open_buf_writer(&self, file_name: &str, read: bool) -> io::Result<BufWriter<std::fs::File>> {
        let file_path = PathBuf::from(self.dir).join(file_name);

        let mut open_options = OpenOptions::new();
        open_options.write(true).create(true);

        if read {
            open_options.read(true);
        }

        Ok(BufWriter::new(open_options.open(&file_path)?))
    }
}