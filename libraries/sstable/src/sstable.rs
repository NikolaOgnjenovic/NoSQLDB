use std::fs::{create_dir_all, OpenOptions, remove_dir_all};
use std::io;
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use b_tree::BTree;
use bloom_filter::BloomFilter;
use db_config::{DBConfig, MemoryTableType};
use segment_elements::{MemoryEntry, SegmentTrait, TimeStamp};
use merkle_tree::merkle_tree::MerkleTree;
use skip_list::SkipList;

/// Struct representing an SSTable (Sorted String Table) for storing key-value pairs on disk.
pub struct SSTable<'a> {
    // Base directory path where the SSTable files will be stored.
    base_path: &'a Path,
    // In-memory segment containing key-value pairs.
    inner_mem: &'a (dyn SegmentTrait + Send),
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
    /// * `base_path` - The base directory path where SSTable files will be stored.
    /// * `inner_mem` - In-memory segment containing key-value pairs.
    /// * `in_single_file` - Flag indicating whether to store data in a single file or multiple files.
    ///
    /// # Returns
    ///
    /// An `io::Result` containing the initialized SSTable instance or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there is an issue when creating directories.
    pub fn new(base_path: &'a Path, inner_mem: &'a (dyn SegmentTrait + Send), in_single_file: bool) -> io::Result<SSTable<'a>> {
        // Create directory if it doesn't exist
        create_dir_all(base_path)?;

        Ok(Self {
            base_path,
            inner_mem,
            in_single_file,
            data_offset: 0,
            index_offset: 0,
            summary_offset: 0,
            bloom_filter_offset: 0,
            merkle_offset: 0
        })
    }

    /// Flushes the in-memory segment to the SSTable files on disk.
    ///
    /// # Arguments
    ///
    /// * `summary_density` - The density parameter for creating the summary.
    ///
    /// # Returns
    ///
    /// An `io::Result` indicating success or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there is an issue flushing the data or serializing components.
    pub fn flush(&mut self, summary_density: usize) -> io::Result<()> {
        // Build serialized data, index_builder, and bloom_filter
        let (serialized_data, index_builder, bloom_filter) = self.build_data_and_index_and_filter();

        // Serialize the index, summary, bloom filter and merkle tree
        let serialized_index = self.get_serialized_index(&index_builder);
        let serialized_index_summary = self.get_serialized_summary(&index_builder, summary_density);
        let serialized_bloom_filter = bloom_filter.serialize();

        let serialized_merkle_tree = MerkleTree::new(&serialized_data).serialize();

        if self.in_single_file {
            self.write_to_single_file(&serialized_data, &serialized_index, &serialized_index_summary, &serialized_bloom_filter, &serialized_merkle_tree)
        } else {
            self.write_to_file_with_postfix(&serialized_data, "-Data.db")?;
            self.write_to_file_with_postfix(&serialized_index, "-Index.db")?;
            self.write_to_file_with_postfix(&serialized_index_summary, "-Summary.db")?;
            self.write_to_file_with_postfix(&serialized_bloom_filter, "-BloomFilter.db")?;
            self.write_to_file_with_postfix(&serialized_merkle_tree, "-MerkleTree.db")?;

            Ok(())
        }
    }

    /// Builds the SSTable data, index builder, and Bloom Filter using self.inner_mem.
    ///
    /// # Returns
    ///
    /// A tuple consisting of a data Vec<u8>, an index builder key pair Vec<(Vec<u8>, u64)>, and a Bloom filter.
    ///
    /// # Errors
    /// None.
    fn build_data_and_index_and_filter(&self) -> (Vec<u8>, Vec<(Vec<u8>, u64)>, BloomFilter) {
        let mut index_builder = Vec::new();
        let mut bloom_filter = BloomFilter::new(0.01, 10_000);
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

        (data, index_builder, bloom_filter)
    }

    /// Serializes the index from the given key, offset pair array.
    ///
    /// # Arguments
    /// * `index_builder` - An array of key, offset pairs.
    ///
    /// # Returns
    ///
    /// Returns the serialized index.
    ///
    /// # Errors
    ///
    /// None.
    fn get_serialized_index(&self, index_builder: &[(Vec<u8>, u64)]) -> Vec<u8> {
        let mut index = Vec::new();
        for (key, offset) in index_builder {
            index.extend(&key.len().to_ne_bytes());
            index.extend(key);
            index.extend(&offset.to_ne_bytes());
        }

        index
    }

    /// Serializes the index summary from the given key, offset pair array.
    ///
    /// # Arguments
    /// * `index_builder` - An array of key, offset pairs.
    /// * `summary_density` - The density parameter for creating the summary.
    ///
    /// # Returns
    ///
    /// Returns the serialized index summary.
    ///
    /// # Errors
    ///
    /// None.
    fn get_serialized_summary(&self, index_builder: &[(Vec<u8>, u64)], summary_density: usize) -> Vec<u8> {
        if index_builder.is_empty() || summary_density < 1 {
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
    /// * `serialized_data` - The serialized data.
    /// * `serialized_index` - The serialized index.
    /// * `serialized_index_summary` - The serialized index summary.
    /// * `serialized_bloom_filter` - The serialized Bloom filter.
    /// * `serialized_merkle_tree` - The serialized Merkle tree.
    ///
    /// # Returns
    ///
    /// Returns the result of the IO operation.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the writing process fails.
    fn write_to_single_file(&mut self, serialized_data: &[u8], serialized_index: &[u8], serialized_index_summary: &[u8], serialized_bloom_filter: &[u8], serialized_merkle_tree: &[u8]) -> io::Result<()> {
        // Calculate the offset for each part of the file and write them into the buffer
        let mut total_offset = 5 * std::mem::size_of::<usize>();
        self.data_offset = total_offset;

        total_offset += serialized_data.len();
        self.index_offset = total_offset;

        total_offset += serialized_index.len();
        self.summary_offset = total_offset;

        total_offset += serialized_index_summary.len();
        self.bloom_filter_offset = total_offset;

        total_offset += serialized_bloom_filter.len();
        self.merkle_offset = total_offset;

        // Create a buffer to hold the offsets and serialized data
        let mut buffer = Vec::new();

        // Write the offsets to the buffer
        buffer.extend_from_slice(&self.data_offset.to_ne_bytes());
        buffer.extend_from_slice(&self.index_offset.to_ne_bytes());
        buffer.extend_from_slice(&self.summary_offset.to_ne_bytes());
        buffer.extend_from_slice(&self.bloom_filter_offset.to_ne_bytes());
        buffer.extend_from_slice(&self.merkle_offset.to_ne_bytes());

        // Write the serialized data to the buffer
        buffer.extend_from_slice(serialized_data);
        buffer.extend_from_slice(serialized_index);
        buffer.extend_from_slice(serialized_index_summary);
        buffer.extend_from_slice(serialized_bloom_filter);
        buffer.extend_from_slice(serialized_merkle_tree);

        // Write the entire buffer to the file
        let mut sstable_file = SSTable::open_buf_writer(self.base_path,".db", false)?;
        sstable_file.write_all(&buffer)
    }

    /// Retrieves a MemoryEntry corresponding to the given key if it exists in the SSTable.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for in the SSTable.
    ///
    /// # Returns
    ///
    /// Returns an Option containing the MemoryEntry if the key is found, otherwise None.
    ///
    /// # Errors
    ///
    /// None.
    pub fn get(&self, key: &[u8]) -> Option<MemoryEntry> {
        if self.is_key_in_bloom_filter(key).unwrap_or(false) {
            if let Some(offset) = self.find_offset_in_summary(key) {
                return self.read_entry_from_data_file(offset);
            }
        }

        None
    }

    /// Compares the written merkle tree with the given tree and returns the indices of the different data chunks.
    ///
    /// # Arguments
    ///
    /// * `other_merkle` - The other merkle tree, assumed to be correct & non-corrupt.
    ///
    /// # Returns
    ///
    /// Returns An `io::Result` containing a vector of indices of the different data chunks or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the comparison process fails.
    pub fn check_merkle(&self, other_merkle: &MerkleTree) -> io::Result<Vec<usize>> {
        let merkle_tree = self.get_merkle();

        Ok(merkle_tree?.get_different_chunks_indices(other_merkle))
    }

    /// Returns the merkle tree of data part of the SSTable from the file.
    ///
    /// # Returns
    ///
    /// Returns An `io::Result` containing the merkle tree or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the retrieval of the merkle tree fails.
    pub fn get_merkle(&self) -> io::Result<MerkleTree> {
        let mut merkle_cursor = SSTable::get_cursor_data(self.base_path, self.in_single_file, "-MerkleTree.db", 4, 5)?;

        let mut merkle_data = Vec::new();
        merkle_cursor.read_to_end(&mut merkle_data)?;

        let merkle_tree = MerkleTree::deserialize(merkle_data.as_slice());

        Ok(merkle_tree)
    }

    /// Merges two SSTables into a new SSTable using merge sort on keys and timestamps.
    /// Deletes the old SSTables and flushes the merged SSTable on completion.
    ///
    /// # Arguments
    ///
    /// * `sstable1_base_path` - The base path of the first SSTable to merge.
    /// * `sstable2_base_path` - The base path of the second SSTable to merge.
    /// * `merged_base_path` - The base path where the merged SSTable files will be stored.
    /// * `sstable1_in_single_file` - A boolean indicating whether the first SSTable is stored in a single file.
    /// * `sstable2_in_single_file` - A boolean indicating whether the second SSTable is stored in a single file.
    /// * `dbconfig` - The configuration for the database.
    ///
    /// # Returns
    ///
    /// Returns An `io::Result` indicating success or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the merging process fails.
    pub fn merge_sstables(sstable1_base_path: &Path, sstable2_base_path: &Path, merged_base_path: &Path, sstable1_in_single_file: bool, sstable2_in_single_file: bool, dbconfig: &DBConfig) -> io::Result<()> {
        // Create directory if it doesn't exist
        create_dir_all(merged_base_path)?;

        // Merge data
        let merged_data = SSTable::merge_sorted_entries(sstable1_base_path, sstable2_base_path, sstable1_in_single_file, sstable2_in_single_file)?;

        let mut inner_mem: Box<dyn SegmentTrait + Send> = match dbconfig.memory_table_type {
            MemoryTableType::SkipList => Box::new(SkipList::new(dbconfig.skip_list_max_level)),
            MemoryTableType::HashMap => todo!(),
            MemoryTableType::BTree => Box::new(BTree::new(dbconfig.b_tree_order).unwrap())
        };
        for (key, entry) in merged_data {
            inner_mem.insert(&key, &entry.get_value(), TimeStamp::Custom(entry.get_timestamp()));
        }
        let mut merged_sstable = SSTable::new(merged_base_path, &*inner_mem, true)?;

        // Flush the new SSTable to disk
        merged_sstable.flush(dbconfig.summary_density)?;

        remove_dir_all(sstable1_base_path)?;
        remove_dir_all(sstable2_base_path)?;

        Ok(())
    }

    /// Merges two sorted SSTables into a new Vec of key-value pairs using merge sort based on keys and timestamps.
    ///
    /// The function reads entries from two SSTables identified by their base paths (`sstable1_base_path` and `sstable2_base_path`).
    /// It performs a merge sort based on the keys and timestamps of the entries. The resulting Vec contains tuples, where each tuple
    /// represents a key-value pair from the merged SSTables.
    ///
    /// # Arguments
    ///
    /// * `sstable1_base_path` - The base path of the first SSTable to merge.
    /// * `sstable2_base_path` - The base path of the second SSTable to merge.
    /// * `sstable1_in_single_file` - A boolean indicating whether the first SSTable is stored in a single file.
    /// * `sstable2_in_single_file` - A boolean indicating whether the second SSTable is stored in a single file.
    ///
    /// # Returns
    ///
    /// An `io::Result` containing a Vec of key-value pairs `(Box<[u8]>, MemoryEntry)` representing the merged entries.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there is an issue when reading from the SSTables or if deserialization fails.
    fn merge_sorted_entries(sstable1_base_path: &Path, sstable2_base_path: &Path, sstable1_in_single_file: bool, sstable2_in_single_file: bool) -> io::Result<Vec<(Box<[u8]>, MemoryEntry)>> {
        let mut cursor1 = SSTable::get_cursor_data(sstable1_base_path, sstable1_in_single_file, "-Data.db", 0, 1)?;
        let mut cursor2 = SSTable::get_cursor_data(sstable2_base_path, sstable2_in_single_file, "-Data.db", 0, 1)?;

        let mut merged_entries = Vec::new();

        // Read the first entry from each SSTable
        let mut entry1 = SSTable::read_entry_from_cursor(&mut cursor1)?;
        let mut entry2 = SSTable::read_entry_from_cursor(&mut cursor2)?;

        // Merge sort based on keys with timestamps
        while let (Some((k1, e1)), Some((k2, e2))) = (entry1.clone(), entry2.clone()) {
            let compare_result = k1.cmp(&k2);

            if compare_result == std::cmp::Ordering::Equal {
                // If keys are equal, choose the entry with the newer timestamp
                if e1.get_timestamp() > e2.get_timestamp() {
                    merged_entries.push((k1, e1));
                    entry1 = SSTable::read_entry_from_cursor(&mut cursor1)?;
                    entry2 = SSTable::read_entry_from_cursor(&mut cursor2)?;
                } else {
                    merged_entries.push((k2, e2));
                    entry1 = SSTable::read_entry_from_cursor(&mut cursor1)?;
                    entry2 = SSTable::read_entry_from_cursor(&mut cursor2)?;
                }
            } else if compare_result == std::cmp::Ordering::Less {
                // If key1 < key2, append entry1 to merged entries
                merged_entries.push((k1, e1));
                entry1 = SSTable::read_entry_from_cursor(&mut cursor1)?;
            } else {
                // If key1 > key2, append entry2 to merged entries
                merged_entries.push((k2, e2));
                entry2 = SSTable::read_entry_from_cursor(&mut cursor2)?;
            }
        }

        // Append remaining entries from SSTable1 if any
        while let Some((k1, e1)) = entry1 {
            merged_entries.push((k1, e1));
            entry1 = SSTable::read_entry_from_cursor(&mut cursor1)?;
        }

        // Append remaining entries from SSTable2 if any
        while let Some((k2, e2)) = entry2 {
            merged_entries.push((k2, e2));
            entry2 = SSTable::read_entry_from_cursor(&mut cursor2)?;
        }

        Ok(merged_entries)
    }

    /// Reads a serialized `MemoryEntry` from the provided cursor.
    ///
    /// # Arguments
    ///
    /// * `cursor` - A mutable reference to a `Cursor<Vec<u8>>` containing the serialized data.
    ///
    /// # Returns
    ///
    /// An `io::Result` containing either `Some((Box<[u8]>, MemoryEntry))` if a valid `MemoryEntry` is
    /// successfully read, or `None` if the cursor has reached the end.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when reading or deserializing the data.
    fn read_entry_from_cursor(cursor: &mut Cursor<Vec<u8>>) -> io::Result<Option<(Box<[u8]>, MemoryEntry)>> {
        let mut entry_len_bytes = [0u8; std::mem::size_of::<usize>()];
        if cursor.read_exact(&mut entry_len_bytes).is_err() {
            // If we can't read the entry length, assume it's the end of the cursor
            return Ok(None);
        }

        let entry_len = usize::from_ne_bytes(entry_len_bytes);

        let mut entry_bytes = vec![0u8; entry_len];
        cursor.read_exact(&mut entry_bytes)?;

        // Deserialize the entry bytes
        match MemoryEntry::deserialize(&entry_bytes) {
            Ok(entry) => Ok(Some(entry)),
            Err(_) => {
                Err(io::Error::new(io::ErrorKind::InvalidData, "Failed to deserialize memory entry"))
            }
        }
    }

    /// Checks if the given key is likely present in the Bloom filter.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check in the Bloom filter.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boolean indicating whether the key is likely present.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when reading or deserializing the bloom filter data.
    fn is_key_in_bloom_filter(&self, key: &[u8]) -> io::Result<bool> {
        // Use the get_cursor_data function to get the Bloom filter data cursor
        let mut filter_data_cursor = SSTable::get_cursor_data(self.base_path, self.in_single_file, "-BloomFilter.db", 3, 4)?;

        let mut filter_data = Vec::new();
        filter_data_cursor.read_to_end(&mut filter_data)?;

        // Attempt to deserialize BloomFilter
        match BloomFilter::deserialize(&filter_data) {
            Ok(bloom_filter) => {
                // Check if key is in the Bloom filter
                Ok(bloom_filter.contains(key))
            }
            Err(err) => {
                // Print or log the error for debugging
                eprintln!("Error deserializing BloomFilter: {:?}", err);
                Err(io::Error::new(io::ErrorKind::Other, "Bloom filter deserialization failed"))
            }
        }
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
        let mut summary_reader = SSTable::get_cursor_data(self.base_path, self.in_single_file, "-Summary.db", 2, 3).ok()?;

        // Read the min key length and min key from the summary file
        let mut min_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        summary_reader.read_exact(&mut min_key_len_bytes).unwrap();
        let min_key_len = usize::from_ne_bytes(min_key_len_bytes);
        let mut min_key = vec![0u8; min_key_len];
        summary_reader.read_exact(&mut min_key).unwrap();

        // Read the max key length and max key from the summary file
        let mut max_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        summary_reader.read_exact(&mut max_key_len_bytes).unwrap();
        let max_key_len = usize::from_ne_bytes(max_key_len_bytes);
        let mut max_key = vec![0u8; max_key_len];
        summary_reader.read_exact(&mut max_key).unwrap();

        // Check if the key is within the range of the lowest and highest keys in the summary
        if key < min_key.as_slice() || key > max_key.as_slice() {
            return None;
        }

        let mut current_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        let mut previous_offset_bytes = [0u8; 8];
        while summary_reader.read_exact(&mut current_key_len_bytes).is_ok() {
            let current_key_len = usize::from_ne_bytes(current_key_len_bytes);

            let mut current_key_bytes = vec![0u8; current_key_len];
            summary_reader.read_exact(&mut current_key_bytes).unwrap();

            let mut offset_bytes = [0u8; 8]; //u64
            summary_reader.read_exact(&mut offset_bytes).unwrap();

            // Key < current key, read starting from previous offset
            if key < current_key_bytes.as_slice() {
                let previous_offset = u64::from_ne_bytes(previous_offset_bytes);

                return self.read_data_offset_from_index_file(previous_offset, key);
            }

            previous_offset_bytes = offset_bytes;
        }

        let previous_offset = u64::from_ne_bytes(previous_offset_bytes);
        self.read_data_offset_from_index_file(previous_offset, key)
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
        let mut index_reader = SSTable::get_cursor_data(self.base_path, self.in_single_file, "-Index.db", 1, 2).ok()?;

        // Use the Seek trait to set the position
        index_reader.seek(io::SeekFrom::Start(seek_offset)).unwrap();

        let mut current_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        let mut result = index_reader.read_exact(&mut current_key_len_bytes);
        while result.is_ok() {
            let current_key_len = usize::from_ne_bytes(current_key_len_bytes);
            let mut current_key_bytes = vec![0u8; current_key_len];
            index_reader.read_exact(&mut current_key_bytes).unwrap();

            let mut offset_bytes = [0u8; 8]; //u64
            index_reader.read_exact(&mut offset_bytes).unwrap();
            if key == current_key_bytes {
                return Some(u64::from_ne_bytes(offset_bytes));
            }

            result = index_reader.read_exact(&mut current_key_len_bytes);
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
        let mut data_reader = SSTable::get_cursor_data(self.base_path, self.in_single_file, "-Data.db", 0, 1).ok()?;

        data_reader.seek(io::SeekFrom::Start(offset)).unwrap();

        let mut entry_len_bytes = [0u8; std::mem::size_of::<usize>()];
        data_reader.read_exact(&mut entry_len_bytes).unwrap();
        let entry_len = usize::from_ne_bytes(entry_len_bytes);

        let mut entry_bytes = vec![0u8; entry_len];
        data_reader.read_exact(&mut entry_bytes).unwrap();

        // Deserialize the entry bytes
        match MemoryEntry::deserialize(&entry_bytes) {
            Ok(entry) => Some(entry.1),
            Err(_) => None,
        }
    }

    /// Reads data from a file with the given postfix, either in a single file mode or from a separate file,
    /// and returns a cursor positioned at the specified range in the data.
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base path of the file.
    /// * `in_single_file` - Indicates whether the table is stored in a single or multiple files.
    /// * `path_postfix` - The postfix of the path of the file.
    /// * `first_offset_index` - The index of the first offset in the buffer (in single file mode).
    /// * `second_offset_index` - The index of the second offset in the buffer (in single file mode).
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Cursor<Vec<u8>>` positioned at the specified range in the data.
    /// The `Cursor` allows sequential reading from the data.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when reading the cursor data.
    fn get_cursor_data(base_path: &Path, in_single_file: bool, path_postfix: &str, first_offset_index: usize, second_offset_index: usize) -> io::Result<Cursor<Vec<u8>>> {
        let mut buffer = Vec::new();
        let data = if in_single_file {
            let mut file = SSTable::open_buf_writer(base_path,".db", true)?;

            // Seek to the first offset index
            file.seek(SeekFrom::Start((first_offset_index * std::mem::size_of::<usize>()) as u64))?;

            // Read the first offset value
            let mut first_offset_bytes = [0u8; std::mem::size_of::<usize>()];
            file.get_ref().read_exact(&mut first_offset_bytes)?;
            let first_offset = usize::from_ne_bytes(first_offset_bytes);

            // If reading from the merkle tree, seek to the first offset value and read to the end
            if second_offset_index == 5 {
                file.seek(SeekFrom::Start(first_offset as u64))?;
                file.get_ref().read_to_end(&mut buffer)?;
            } else {
                // Seek to the second offset index
                file.seek(SeekFrom::Start((second_offset_index * std::mem::size_of::<usize>()) as u64))?;

                // Read the second offset value
                let mut second_offset_bytes = [0u8; std::mem::size_of::<usize>()];
                file.get_ref().read_exact(&mut second_offset_bytes)?;
                let second_offset = usize::from_ne_bytes(second_offset_bytes);

                // Seek to the first offset value and read the data between the offsets
                file.seek(SeekFrom::Start(first_offset as u64))?;
                file.get_ref().take((second_offset - first_offset) as u64).read_to_end(&mut buffer)?;
            }

            buffer
        } else {
            // Open the file for reading
            let file = SSTable::open_buf_writer(base_path, path_postfix, true)?;
            file.get_ref().read_to_end(&mut buffer)?;
            buffer
        };

        Ok(Cursor::new(data))
    }

    /// Writes the serialized data to a file with the given postfix.
    ///
    /// # Arguments
    ///
    /// * `serialized_data` - The serialized data to be written.
    /// * `path_postfix` - The postfix of the path of the file.
    ///
    /// # Returns
    ///
    /// The result of the IO operation.
    fn write_to_file_with_postfix(&self, serialized_data: &[u8], path_postfix: &str) -> io::Result<()> {
        let mut file = SSTable::open_buf_writer(self.base_path, path_postfix, false)?;

        file.write_all(serialized_data)?;

        Ok(())
    }

    /// Opens a buffered writer for a fil with the given postfix, with an option to enable reading.
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base path of the file.
    /// * `path_postfix` - The postfix of the path of the file.
    /// * `read` - A flag indicating whether reading should be enabled.
    ///
    /// # Returns
    ///
    /// A `Result` containing the opened BufWriter.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when opening the buffered writer.
    fn open_buf_writer(base_path: &Path, path_postfix: &str, read: bool) -> io::Result<BufWriter<std::fs::File>> {
        let file_path = PathBuf::from(base_path).join(path_postfix);
        let mut open_options = OpenOptions::new();
        open_options.write(true).create(true);

        if read {
            open_options.read(true);
        }

        Ok(BufWriter::new(open_options.open(file_path)?))
    }
}