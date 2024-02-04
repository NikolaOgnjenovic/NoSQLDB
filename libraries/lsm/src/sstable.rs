pub(crate) mod sstable_element_type;

use std::cmp::{Ordering};
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions, remove_dir_all};
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use bloom_filter::BloomFilter;
use lru_cache::LRUCache;
use segment_elements::{deserialize_header, deserialize_usize_value, MemoryEntry};
use merkle_tree::merkle_tree::MerkleTree;
use compression::{CompressionDictionary, variable_encode};
use crate::lsm::ScanType;
use crate::memtable::MemoryTable;
use crate::sstable::sstable_element_type::SSTableElementType;

/// Struct representing an SSTable (Sorted String Table) for storing key-value pairs on disk.
pub struct SSTable {
    // Base directory path where the SSTable files will be stored.
    base_path: PathBuf,
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
    merkle_offset: usize,
    // Holds references to files for reading & writing
    file_handles: HashMap<String, File>,
}

impl SSTable {
    /// Opens an SSTable in the given base path.
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base directory path where SSTable files are be stored.
    /// * `in_single_file` - Flag indicating whether data is stored in a single file or multiple files.
    ///
    /// # Returns
    ///
    /// An `io::Result` containing the initialized SSTable instance or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there is an issue when creating directories.
    pub(crate) fn open(base_path: PathBuf, in_single_file: bool) -> io::Result<SSTable> {
        // Create directory if it doesn't exist
        create_dir_all(base_path.to_owned())?;

        Ok(Self {
            base_path,
            in_single_file,
            data_offset: 0,
            index_offset: 0,
            summary_offset: 0,
            bloom_filter_offset: 0,
            merkle_offset: 0,
            file_handles: HashMap::new(),
        })
    }

    /// Flushes the memory table to the SSTable files on disk.
    ///
    /// # Arguments
    ///
    /// * `mem_table` - The memory table to be flushed.
    /// * `summary_density` - The number of entries that will be skipped in the summary.
    /// * `index_density` - The number of entries that will be skipped in the index.
    ///
    /// # Returns
    ///
    /// An `io::Result` indicating success or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there is an issue flushing the data or serializing components.
    pub(crate) fn flush(&mut self, mem_table: MemoryTable, summary_density: usize, index_density: usize, lru_cache: Option<&mut LRUCache>, compression_dictionary: &mut Option<CompressionDictionary>, use_variable_encoding: bool) -> io::Result<()> {
        self.flush_to_disk(mem_table.iterator().collect(), summary_density, index_density, lru_cache, compression_dictionary, use_variable_encoding)
    }

    /// Flushes the memory table to the SSTable files on disk.
    ///
    /// # Arguments
    ///
    /// * `sstable_data` - The data Vec<(key, MemoryEntry)> to be flushed to the disk.
    /// * `summary_density` - The number of entries that will be skipped in the summary.
    /// * `index_density` - The number of entries that will be skipped in the index.
    ///
    /// # Returns
    ///
    /// An `io::Result` indicating success or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there is an issue flushing the data or serializing components.
    fn flush_to_disk(&mut self, sstable_data: Vec<(Box<[u8]>, MemoryEntry)>, summary_density: usize, index_density: usize, lru_cache: Option<&mut LRUCache>, compression_dictionary: &mut Option<CompressionDictionary>, use_variable_encoding: bool) -> io::Result<()> {
        // Build serialized data, index_builder, and bloom_filter
        let (serialized_data, index_builder, bloom_filter) = self.build_data_and_index_and_filter(sstable_data, lru_cache, compression_dictionary, use_variable_encoding);

        // Serialize the index, summary, bloom filter and merkle tree
        let serialized_index = self.get_serialized_index(&index_builder, index_density);
        let serialized_index_summary = self.get_serialized_summary(&index_builder, index_density, summary_density);
        let serialized_bloom_filter = bloom_filter.serialize();
        let serialized_merkle_tree = MerkleTree::new(&serialized_data).serialize();

        if self.in_single_file {
            self.write_to_single_file(&serialized_data, &serialized_index, &serialized_index_summary, &serialized_bloom_filter, &serialized_merkle_tree)?;
        } else {
            self.write_to_file(&serialized_data, "SSTable-Data.db")?;
            self.write_to_file(&serialized_index, "SSTable-Index.db")?;
            self.write_to_file(&serialized_index_summary, "SSTable-Summary.db")?;
            self.write_to_file(&serialized_bloom_filter, "SSTable-BloomFilter.db")?;
            self.write_to_file(&serialized_merkle_tree, "SSTable-MerkleTree.db")?;
        }

        Ok(())
    }

    /// Builds the SSTable data, index builder, and Bloom Filter.
    ///
    /// # Returns
    ///
    /// A tuple consisting of a data Vec<u8>, an index builder key pair Vec<(Vec<u8>, u64)>, and a Bloom filter.
    ///
    /// # Errors
    ///
    /// None.
    fn build_data_and_index_and_filter(&self, sstable_data: Vec<(Box<[u8]>, MemoryEntry)>, lru_cache: Option<&mut LRUCache>, compression_dictionary: &mut Option<CompressionDictionary>, use_variable_encoding: bool) -> (Vec<u8>, Vec<(Vec<u8>, usize)>, BloomFilter) {
        let mut index_builder = Vec::new();
        let mut bloom_filter = BloomFilter::new(0.01, 100_000);
        let mut data = Vec::new();

        if let Some(compression_dict) = compression_dictionary {
            let keys: Vec<Box<[u8]>> = sstable_data.clone().into_iter().map(|(boxed_slice, _)| boxed_slice).collect();
            compression_dict.add(&keys).expect("Failed to add keys to the dictionary!");
        }

        let mut offset = 0;
        for (key, entry) in sstable_data {
            let encoded_key = match compression_dictionary {
                Some(compression_dictionary) => compression_dictionary.encode(&key.clone()).unwrap(),
                None => key.clone()
            };
            let entry_data = entry.serialize(&encoded_key, use_variable_encoding);
            if let Some(&mut ref mut lru) = lru_cache {
                lru.update(&key, Some(entry));
            }

            data.extend_from_slice(&entry_data);
            index_builder.push((encoded_key.to_vec(), offset));
            bloom_filter.add(&encoded_key);

            offset += entry_data.len();
        }

        (data, index_builder, bloom_filter)
    }

    /// Serializes the index from the given key, offset pair array.
    ///
    /// # Arguments
    /// * `index_builder` - An array of key, offset pairs.
    /// * `index_density` - The number of entries that will be skipped in the index.
    ///
    /// # Returns
    ///
    /// Returns the serialized index.
    ///
    /// # Errors
    ///
    /// None.
    fn get_serialized_index(&self, index_builder: &[(Vec<u8>, usize)], index_density: usize) -> Vec<u8> {
        let mut index = Vec::new();

        // Add every step-th key and its offset to the summary
        for (key, offset) in index_builder.iter().step_by(index_density) {
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
    /// * `index_density` - The number of entries that will be skipped in the index.
    /// * `summary_density` - The number of entries that will be skipped in the summary.
    ///
    /// # Returns
    ///
    /// Returns the serialized index summary.
    ///
    /// # Errors
    ///
    /// None.
    fn get_serialized_summary(&self, index_builder: &[(Vec<u8>, usize)], index_density: usize, summary_density: usize) -> Vec<u8> {
        if index_builder.is_empty() || index_density < 1 || summary_density < 1 {
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

        let mut offset_accumulator: usize = 0;
        // Add every step-th key and its offset to the summary
        for i in (0..index_builder.len()).step_by(index_density) {
            let (key, _) = &index_builder[i];

            if i % (summary_density * index_density) == 0 {
                summary.extend_from_slice(&key.len().to_ne_bytes());
                summary.extend_from_slice(key);
                let offset_in_index = offset_accumulator;
                summary.extend_from_slice(&offset_in_index.to_ne_bytes());
            }

            offset_accumulator += key.len() + 2 * std::mem::size_of::<usize>();
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

        // Write the entire buffer to the .db file
        self.write_to_file(&buffer, "SSTable.db")?;

        Ok(())
    }

    /// Retrieves a MemoryEntry corresponding to the given key if it exists in the SSTable.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for in the SSTable.
    /// * `index_density` - The number of entries that will be skipped in the index.
    ///
    /// # Returns
    ///
    /// Returns an Option containing the MemoryEntry if the key is found, otherwise None.
    ///
    /// # Errors
    ///
    /// None.
    pub(crate) fn get(&mut self, key: &[u8], index_density: usize, compression_dictionary: &mut Option<CompressionDictionary>, use_variable_encoding: bool) -> Option<MemoryEntry> {
        let encoded_key = match compression_dictionary {
            Some(compression_dictionary) => compression_dictionary.encode(&key.to_vec().into_boxed_slice()).unwrap().clone(),
            None => key.to_vec().into_boxed_slice()
        };
        if self.bloom_filter_contains_key(&encoded_key).unwrap_or(false) {
            if let Some(offset) = self.get_data_offset_from_summary(&encoded_key, compression_dictionary) {
                return match self.get_entry_from_data_file(offset, Some(index_density), Some(&encoded_key), use_variable_encoding) {
                    Some(entry) => Some(entry.0.1),
                    None => None
                };
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
    pub(crate) fn check_merkle(&mut self, other_merkle: &MerkleTree) -> io::Result<Vec<usize>> {
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
    pub(crate) fn get_merkle(&mut self) -> io::Result<MerkleTree> {
        let mut merkle_cursor = self.get_cursor_data(self.in_single_file, "SSTable-MerkleTree.db", SSTableElementType::MerkleTree, None, false)?;

        let mut merkle_data = Vec::new();
        merkle_cursor.read_to_end(&mut merkle_data)?;

        let merkle_tree = MerkleTree::deserialize(merkle_data.as_slice());

        Ok(merkle_tree)
    }

    /// Merges multiple SSTables into a new SSTable using merge sort on keys and timestamps.
    /// Deletes the old SSTables and flushes the merged SSTable on completion.
    ///
    /// # Arguments
    ///
    /// * `sstable_paths` - Base paths to all SSTables.
    /// * `in_single_file` - Vector of booleans indicating whether corresponding SSTables are stored in a single file
    /// * `merged_base_path` - The base path where the merged SSTable files will be stored.
    /// * `merged_in_single_file` - A boolean indicating whether the merged SSTable is stored in a single file.
    /// * `summary_density` - The number of entries that will be skipped in the summary.
    /// * `index_density` - The number of entries that will be skipped in the index.
    ///
    /// # Returns
    ///
    /// Returns An `io::Result` indicating success or an `io::Error`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the merging process fails.
    pub(crate) fn merge(sstable_paths: Vec<PathBuf>, in_single_file: Vec<bool>, merged_base_path: &PathBuf, merged_in_single_file: bool, summary_density: usize, index_density: usize, use_variable_encoding: bool, compression_dictionary: &mut Option<CompressionDictionary>) -> io::Result<()> {
        create_dir_all(merged_base_path)?;

        let merged_data = SSTable::merge_entries(sstable_paths.clone(), in_single_file, None, use_variable_encoding, compression_dictionary)?;

        let mut merged_sstable = SSTable::open(merged_base_path.to_owned(), merged_in_single_file)?;

        // Flush the new SSTable to disk
        merged_sstable.flush_to_disk(merged_data, summary_density, index_density, None, &mut None, use_variable_encoding)?;

        let _ = sstable_paths
            .iter()
            .map(|path| remove_dir_all(path));

        Ok(())
    }

    /// Merges multiple SSTables into a new Vec of key-value pairs using merge sort based on keys and timestamps.
    ///
    /// The function reads entries from multiple SSTables identified by their base paths.
    /// It performs a merge sort based on the keys and timestamps of the entries. The resulting Vec contains tuples, where each tuple
    /// represents a key-value pair from the merged SSTables.
    ///
    /// # Arguments
    ///
    /// * `sstable_paths` - Base paths to all SSTables.
    /// * `in_single_file` - Vector of booleans indicating whether corresponding SSTables are stored in a single file
    ///
    /// # Returns
    ///
    /// An `io::Result` containing a Vec of key-value pairs `(Box<[u8]>, MemoryEntry)` representing the merged entries.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there is an issue when reading from the SSTables or if deserialization fails.
    pub(crate) fn merge_entries(sstable_paths: Vec<PathBuf>, in_single_file: Vec<bool>, total_entry_offsets: Option<Vec<u64>>, use_variable_encoding: bool, compression_dictionary: &mut Option<CompressionDictionary>) -> io::Result<Vec<(Box<[u8]>, MemoryEntry)>> {
        let number_of_tables = sstable_paths.len();

        // offsets for each sstable
        let mut total_entry_offsets = total_entry_offsets.unwrap_or(vec![0; number_of_tables]);
        let mut file_ref_sstables = Vec::with_capacity(number_of_tables);
        for i in 0..number_of_tables {
            file_ref_sstables.push(Self {
                base_path: sstable_paths[i].to_owned(),
                in_single_file: in_single_file[i],
                data_offset: 0,
                index_offset: 0,
                summary_offset: 0,
                bloom_filter_offset: 0,
                merkle_offset: 0,
                file_handles: HashMap::new(),
            })
        }
        let mut merged_entries = Vec::new();
        loop {
            // contains a tuple ((index, entry), offset) for each sstable
            let option_entries: Vec<Option<_>> = file_ref_sstables
                .iter_mut()
                .zip(total_entry_offsets.iter())
                .map(|(sstable, offset)| sstable.get_entry_from_data_file(*offset, None, None, use_variable_encoding))
                .collect();

            // if all entries are none, there is no more data
            if option_entries.iter().all(Option::is_none) {
                break;
            }

            // add indexes
            let entries: Vec<_> = option_entries
                .iter()
                .enumerate()
                .collect();

            // find the indexes of min keys
            let min_key_indexes = SSTable::find_min_keys(&entries, true, compression_dictionary);

            // filter only the entries containing min key
            let min_entries: Vec<_> = min_key_indexes
                .iter()
                .map(|index| entries[*index].clone())
                .collect();

            // update the offset only for entries with minimal keys
            let _ = min_entries
                .iter()
                .for_each(|(index, element)| {
                    total_entry_offsets[*index] += element.as_ref().unwrap().1.clone();
                });

            // insert entry with the biggest timestamp
            let max_index = SSTable::find_max_timestamp(&min_entries);
            merged_entries.push(entries[max_index].1.as_ref().unwrap().0.clone());
        }
        Ok(merged_entries)
    }


    /// Finds the index of entry with the biggest timestamp
    ///
    /// # Arguments
    ///
    /// * `entries` - vector containing entries with the smallest keys
    ///
    /// # Returns
    ///
    /// An index of entry with the biggest timestamp
    pub(crate) fn find_max_timestamp(entries: &Vec<(usize, &Option<((Box<[u8]>, MemoryEntry), u64)>)>) -> usize {
        let mut max_index = 0;
        let mut max_timestamp = 0;
        for (index, element) in entries {
            if element.is_none() { continue; }
            let timestamp = element.as_ref().unwrap().0.1.get_timestamp();
            if timestamp > max_timestamp {
                max_index = *index;
                max_timestamp = timestamp;
            }
        }
        max_index
    }


    /// Finds the indexes of entries with minimal keys
    ///
    /// # Arguments
    ///
    /// * entries - vector containing one entry from each sstable
    ///
    /// # Returns
    ///
    /// A Vector of indexes of entries with minimal keys
    pub(crate) fn find_min_keys(entries: &Vec<(usize, &Option<((Box<[u8]>, MemoryEntry), u64)>)>, merging: bool, compression_dictionary: &mut Option<CompressionDictionary>) -> Vec<usize> {
        let mut min_key: Box<[u8]> = Box::new([255u8; 255]);
        let mut min_indexes = vec![];
        for (index, element) in entries {
            if element.is_none() { continue; }
            let element = element.as_ref().unwrap();
            if !merging {
                if element.0.1.get_tombstone() {
                    continue;
                }
            }
            let key = &element.0.0;
            let decoded_key = match compression_dictionary {
                Some(compression_dictionary) => compression_dictionary.decode(&key.to_vec().into_boxed_slice()).unwrap().clone(),
                None => key.to_vec().into_boxed_slice()
            };
            let compare_result = min_key.cmp(&decoded_key);
            if compare_result == Ordering::Equal {
                min_indexes.push(*index);
            }
            if compare_result == Ordering::Greater {
                min_indexes.clear();
                min_indexes.push(*index);
                min_key = decoded_key;
            }
        }

        min_indexes
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
    fn bloom_filter_contains_key(&mut self, key: &[u8]) -> io::Result<bool> {
        // Use the get_cursor_data function to get the Bloom filter data cursor
        let mut filter_data_cursor = self.get_cursor_data(self.in_single_file, "SSTable-BloomFilter.db", SSTableElementType::BloomFilter, None, false)?;

        let mut filter_data = Vec::new();
        filter_data_cursor.read_to_end(&mut filter_data)?;

        // Attempt to deserialize BloomFilter
        match BloomFilter::deserialize(&filter_data) {
            Ok(bloom_filter) => {
                // Check if key is in the Bloom filter
                Ok(bloom_filter.contains(key))
            }
            Err(err) => {
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
    fn get_data_offset_from_summary(&mut self, key: &[u8], compression_dictionary: &mut Option<CompressionDictionary>) -> Option<u64> {
        let mut total_entry_offset = 0;
        let mut summary_reader = self.get_cursor_data(self.in_single_file, "SSTable-Summary.db", SSTableElementType::Summary, Some(total_entry_offset), false).ok()?;

        // Read the min key length and min key from the summary file
        let mut min_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        summary_reader.read_exact(&mut min_key_len_bytes).unwrap();
        total_entry_offset += std::mem::size_of::<usize>() as u64;

        let min_key_len = usize::from_ne_bytes(min_key_len_bytes);
        let mut min_key = vec![0u8; min_key_len];
        summary_reader.read_exact(&mut min_key).unwrap();
        total_entry_offset += min_key_len as u64;

        // Read the max key length and max key from the summary file
        let mut max_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        summary_reader.read_exact(&mut max_key_len_bytes).unwrap();
        total_entry_offset += std::mem::size_of::<usize>() as u64;

        let max_key_len = usize::from_ne_bytes(max_key_len_bytes);
        let mut max_key = vec![0u8; max_key_len];
        summary_reader.read_exact(&mut max_key).unwrap();
        total_entry_offset += max_key_len as u64;

        let decoded_key = match compression_dictionary {
            Some(compression_dictionary) => compression_dictionary.decode(&key.to_vec().into_boxed_slice()).unwrap().clone(),
            None => key.to_vec().into_boxed_slice()
        };

        let decoded_min_key = match compression_dictionary {
            Some(compression_dictionary) => compression_dictionary.decode(&min_key.to_vec().into_boxed_slice()).unwrap().clone(),
            None => min_key.to_vec().into_boxed_slice()
        };

        let decoded_max_key = match compression_dictionary {
            Some(compression_dictionary) => compression_dictionary.decode(&max_key.to_vec().into_boxed_slice()).unwrap().clone(),
            None => max_key.to_vec().into_boxed_slice()
        };

        // Check if the key is within the range of the lowest and highest keys in the summary
        if decoded_key.as_ref().cmp(decoded_min_key.as_ref()) == Ordering::Less || decoded_key.as_ref().cmp(decoded_max_key.as_ref()) == Ordering::Greater {
            return None;
        }

        let mut current_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        let mut previous_offset_bytes = [0u8; std::mem::size_of::<usize>()];

        summary_reader = self.get_cursor_data(self.in_single_file, "SSTable-Summary.db", SSTableElementType::Summary, Some(total_entry_offset), false).ok()?;
        while summary_reader.read_exact(&mut current_key_len_bytes).is_ok() {
            total_entry_offset += std::mem::size_of::<usize>() as u64;

            let current_key_len = usize::from_ne_bytes(current_key_len_bytes);
            let mut current_key_bytes = vec![0u8; current_key_len];
            summary_reader.read_exact(&mut current_key_bytes).unwrap();
            total_entry_offset += current_key_len as u64;

            let mut offset_bytes = [0u8; std::mem::size_of::<usize>()];
            summary_reader.read_exact(&mut offset_bytes).unwrap();
            total_entry_offset += std::mem::size_of::<usize>() as u64;

            let decoded_current_key = match compression_dictionary {
                Some(compression_dictionary) => compression_dictionary.decode(&current_key_bytes.to_vec().into_boxed_slice()).unwrap().clone(),
                None => current_key_bytes.to_vec().into_boxed_slice()
            };

            // Key < current key, read starting from previous offset previous_
            if decoded_key.as_ref().cmp(decoded_current_key.as_ref()) == Ordering::Less {
                return self.get_data_offset_from_index(u64::from_ne_bytes(previous_offset_bytes), key, compression_dictionary);
            }

            previous_offset_bytes = offset_bytes;
            summary_reader = self.get_cursor_data(self.in_single_file, "SSTable-Summary.db", SSTableElementType::Summary, Some(total_entry_offset), false).ok()?;
        }

        return self.get_data_offset_from_index(u64::from_ne_bytes(previous_offset_bytes), key, compression_dictionary);
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
    fn get_data_offset_from_index(&mut self, seek_offset: u64, key: &[u8], compression_dictionary: &mut Option<CompressionDictionary>) -> Option<u64> {
        let mut total_entry_offset = seek_offset;
        let mut index_reader = self.get_cursor_data(self.in_single_file, "SSTable-Index.db", SSTableElementType::Index, Some(total_entry_offset), false).ok()?;

        let decoded_key = match compression_dictionary {
            Some(compression_dictionary) => compression_dictionary.decode(&key.to_vec().into_boxed_slice()).unwrap().clone(),
            None => key.to_vec().into_boxed_slice()
        };

        let mut current_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        let mut previous_offset_bytes = [0u8; std::mem::size_of::<usize>()];
        while index_reader.read_exact(&mut current_key_len_bytes).is_ok() {
            total_entry_offset += std::mem::size_of::<usize>() as u64;

            let current_key_len = usize::from_ne_bytes(current_key_len_bytes);
            let mut current_key_bytes = vec![0u8; current_key_len];
            index_reader.read_exact(&mut current_key_bytes).unwrap();
            total_entry_offset += current_key_len as u64;

            let mut offset_bytes = [0u8; std::mem::size_of::<usize>()];
            index_reader.read_exact(&mut offset_bytes).unwrap();
            total_entry_offset += std::mem::size_of::<usize>() as u64;

            let decoded_current_key = match compression_dictionary {
                Some(compression_dictionary) => compression_dictionary.decode(&current_key_bytes.to_vec().into_boxed_slice()).unwrap().clone(),
                None => current_key_bytes.to_vec().into_boxed_slice()
            };

            // Key < current key, return previous offset
            if decoded_key.as_ref().cmp(decoded_current_key.as_ref()) == Ordering::Less {
                return Some(u64::from_ne_bytes(previous_offset_bytes));
            }

            previous_offset_bytes = offset_bytes;
            index_reader = self.get_cursor_data(self.in_single_file, "SSTable-Index.db", SSTableElementType::Index, Some(total_entry_offset), false).ok()?;
        }

        // Return previous offset for the last entry in the index file
        return Some(u64::from_ne_bytes(previous_offset_bytes));
    }

    /// Reads the MemoryEntry from the data file based on the given offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset in the data file to read the MemoryEntry from.
    /// * `index_density` - The number of entries that are read before returning None if the key is not found.
    /// * `expected_key` - The key that is being searched for.
    ///
    /// # Returns
    ///
    /// An Option containing a pair of the key & MemoryEntry pair and the memory entry bytes length if successful, otherwise None.
    pub(crate) fn get_entry_from_data_file(&mut self, offset: u64, index_density: Option<usize>, expected_key: Option<&[u8]>, use_variable_encoding: bool) -> Option<((Box<[u8]>, MemoryEntry), u64)> {
        let (mut crc, mut timestamp, mut tombstone, mut key_len, mut offset_to_key_len, mut length) = (0u32, 0u128, false, 0usize, 0usize, 0usize);
        let mut traversed_offset = 0;

        let mut unwrapped_key = vec![];

        // Merge reads a single entry from the given offset without looping through index_density number of entries
        // Traverse through index_density entries to find the given key only if both are not None
        if let (Some(index_density), Some(key)) = (index_density, expected_key) {
            if index_density < 1 {
                return None;
            }

            let mut traversed_entries: usize = 0;
            while traversed_entries <= index_density {
                let buffer = self.get_cursor_data(self.in_single_file, "SSTable-Data.db", SSTableElementType::DataEntryWithoutValue, Some(offset + traversed_offset), use_variable_encoding).ok()?.into_inner();
                if buffer.len() == 0 {
                    return None;
                }
                let (entry_length, mut buffer_offset) = deserialize_usize_value(&buffer, false);
                (offset_to_key_len, length) = deserialize_usize_value(&buffer[buffer_offset..], false);
                buffer_offset += length;
                (crc, timestamp, tombstone, key_len, _, length, _) = deserialize_header(&buffer[buffer_offset..], false);
                buffer_offset += length;

                if &buffer[buffer_offset..] == key {
                    unwrapped_key.extend_from_slice(&buffer[buffer_offset..]);
                    break;
                }

                traversed_entries += 1;
                traversed_offset += entry_length as u64;
            }

            // If all index_density entries have been traversed and the key hasn't been found, return None
            if traversed_entries == 1 + index_density {
                return None;
            }
        } else {
            let buffer = self.get_cursor_data(self.in_single_file, "SSTable-Data.db", SSTableElementType::DataEntryWithoutValue, Some(offset), use_variable_encoding).ok()?.into_inner();
            if buffer.len() == 0 {
                return None;
            }
            let (_, mut buffer_offset) = deserialize_usize_value(&buffer, false);
            (offset_to_key_len, length) = deserialize_usize_value(&buffer[buffer_offset..], false);
            buffer_offset += length;
            (crc, timestamp, tombstone, key_len, _, length, _) = deserialize_header(&buffer[buffer_offset..], false);
            buffer_offset += length;

            unwrapped_key.extend_from_slice(&buffer[buffer_offset..]);
        }

        // Deserialize the last read memory entry bytes
        let data_entry_value = if tombstone { Vec::new() } else { self.get_cursor_data(self.in_single_file, "SSTable-Data.db", SSTableElementType::DataEntryValue, Some(offset + traversed_offset + offset_to_key_len as u64), use_variable_encoding).ok()?.into_inner() };

        let mut data_entry_bytes = Vec::new();

        let crc_bytes = if use_variable_encoding {
            variable_encode(crc as u128)
        } else {
            Box::new(crc.to_ne_bytes())
        };

        data_entry_bytes.extend(crc_bytes.to_vec().as_slice());

        let timestamp_bytes = if use_variable_encoding { variable_encode(timestamp) } else { Box::new(timestamp.to_ne_bytes()) };
        data_entry_bytes.extend_from_slice(&timestamp_bytes);

        data_entry_bytes.extend((tombstone as u8).to_ne_bytes());

        let key_len_bytes = if use_variable_encoding { variable_encode(unwrapped_key.len() as u128) } else { Box::new(unwrapped_key.len().to_ne_bytes()) };
        data_entry_bytes.extend_from_slice(&key_len_bytes);

        if !tombstone {
            let value_len_bytes = if use_variable_encoding { variable_encode(data_entry_value.len() as u128) } else { Box::new(data_entry_value.len().to_ne_bytes()) };
            data_entry_bytes.extend_from_slice(&value_len_bytes);
        }

        data_entry_bytes.extend(unwrapped_key);

        if !tombstone {
            data_entry_bytes.extend(data_entry_value);
        }

        match MemoryEntry::deserialize(&data_entry_bytes, use_variable_encoding) {
            Ok(entry) => Some((entry, data_entry_bytes.len() as u64)),
            Err(_) => None,
        }
    }

    /// Reads data from a file with the given postfix, either in a single file mode or from a separate file,
    /// and returns a cursor positioned at the specified range in the data.
    ///
    /// # Arguments
    ///
    /// * `in_single_file` - Indicates whether the table is stored in a single or multiple files.
    /// * `path_postfix` - The postfix of the path of the file.
    /// * `sstable_element_type` - The type of SSTable element to read (in single file mode).
    /// * `total_entry_offset` - The total entry offset used to read some files entry by entry.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Cursor<Vec<u8>>` positioned at the specified range in the data.
    /// The `Cursor` allows sequential reading from the data.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when reading the cursor data.
    pub(crate) fn get_cursor_data(&mut self, in_single_file: bool, path_postfix: &str, sstable_element_type: SSTableElementType, total_entry_offset: Option<u64>, use_variable_encoding: bool) -> io::Result<Cursor<Vec<u8>>> {
        let mut buffer = Vec::new();
        let total_entry_offset = total_entry_offset.unwrap_or(0);

        let file = if in_single_file {
            self.write_to_file(&[], "SSTable.db")?
        } else {
            self.write_to_file(&[], path_postfix)?
        };

        let file_element_offset = if in_single_file {
            // Seek to the file element offset index
            file.seek(SeekFrom::Start((sstable_element_type.get_id() * std::mem::size_of::<usize>()) as u64))?;

            // Read the first offset value
            let mut file_element_offset_bytes = [0u8; std::mem::size_of::<usize>()];
            file.read_exact(&mut file_element_offset_bytes)?;
            usize::from_ne_bytes(file_element_offset_bytes) as u64
        } else {
            0
        };

        let next_file_element_offset = if in_single_file && sstable_element_type != SSTableElementType::MerkleTree {
            file.seek(SeekFrom::Start(((sstable_element_type.get_id() + 1) * std::mem::size_of::<usize>()) as u64))?;

            // Read the first offset value
            let mut file_element_offset_bytes = [0u8; std::mem::size_of::<usize>()];
            file.read_exact(&mut file_element_offset_bytes)?;
            usize::from_ne_bytes(file_element_offset_bytes) as u64
        } else {
            0
        };

        match sstable_element_type {
            SSTableElementType::DataEntryWithoutValue => {
                let result = file.seek(SeekFrom::Start(file_element_offset + total_entry_offset));

                if let Err(err) = result {
                    eprintln!("Error seeking in file: {}", err);
                    return Err(err.into());
                }

                if in_single_file && file_element_offset + total_entry_offset + std::mem::size_of::<usize>() as u64 >= next_file_element_offset {
                    return Ok(Cursor::new(Vec::new()));
                }

                // CRC + timestamp + tombstone + key_length + value_length
                let mut header_max_length = 4 + 16 + 1 + 2 * std::mem::size_of::<usize>();
                if use_variable_encoding {
                    header_max_length = ((header_max_length * 8) as f64 / 7.0).ceil() as usize;
                }
                let mut header_bytes = vec![0u8; header_max_length];
                // If EOF, return empty vec
                let result = file.read(&mut header_bytes);
                let result_len = match result {
                    Ok(len) => {
                        if len == 0 {
                            return Ok(Cursor::new(Vec::new()));
                        }
                        len
                    }
                    Err(_) => return Ok(Cursor::new(Vec::new())), // Handle error or zero bytes read
                };

                let (crc, timestamp, tombstone, key_len, value_len, header_len, offset_to_key_len) = deserialize_header(&header_bytes, use_variable_encoding);
                let entry_length = header_len + key_len + value_len;
                file.seek(SeekFrom::Current(header_len as i64 - result_len as i64)).ok();
                let mut key = vec![0u8; key_len];
                file.read_exact(&mut key).ok();
                buffer.extend_from_slice(&entry_length.to_ne_bytes());
                buffer.extend_from_slice(&offset_to_key_len.to_ne_bytes());
                buffer.extend_from_slice(&crc.to_ne_bytes());
                buffer.extend_from_slice(&timestamp.to_ne_bytes());
                buffer.extend_from_slice(&(tombstone as u8).to_ne_bytes());
                buffer.extend_from_slice(&key_len.to_ne_bytes());
                if !tombstone {
                    buffer.extend_from_slice(&0usize.to_ne_bytes());
                }
                buffer.extend_from_slice(&key);
            }
            SSTableElementType::DataEntryValue => {
                let result = file.seek(SeekFrom::Start(file_element_offset + total_entry_offset));

                if let Err(err) = result {
                    eprintln!("Error seeking in file: {}", err);
                    return Err(err.into());
                }

                if in_single_file && file_element_offset + total_entry_offset + std::mem::size_of::<usize>() as u64 >= next_file_element_offset {
                    return Ok(Cursor::new(Vec::new()));
                }

                // key_length + value_length
                let mut key_val_len_max_len = 2 * std::mem::size_of::<usize>();
                if use_variable_encoding {
                    key_val_len_max_len = ((key_val_len_max_len * 8) as f64 / 7.0).ceil() as usize;
                }
                let mut key_val_len_bytes = vec![0u8; key_val_len_max_len];

                // If EOF, return empty vec
                let result = file.read(&mut key_val_len_bytes);
                let result_len = match result {
                    Ok(len) => {
                        if len == 0 {
                            return Ok(Cursor::new(Vec::new()));
                        }
                        len
                    }
                    Err(_) => return Ok(Cursor::new(Vec::new())), // Handle error or zero bytes read
                };

                let mut offset = 0;

                let (key_len, length) = deserialize_usize_value(&key_val_len_bytes[offset..], use_variable_encoding);
                offset += length;

                let (value_len, length) = deserialize_usize_value(&key_val_len_bytes[offset..], use_variable_encoding);
                offset += length;
                let seek_backwards_offset = offset as i64 - result_len as i64;

                file.seek(SeekFrom::Current(seek_backwards_offset)).ok();
                let mut key_bytes = vec![0u8; key_len];
                file.read_exact(&mut key_bytes)?;
                let mut value = vec![0u8; value_len];
                file.read_exact(&mut value).ok();

                buffer.extend(value);
            }
            SSTableElementType::Index => {
                file.seek(SeekFrom::Start(file_element_offset + total_entry_offset))?;

                // Read key len bytes, key len and offset bytes
                let mut key_len_bytes = [0u8; std::mem::size_of::<usize>()];
                let result = file.read_exact(&mut key_len_bytes);
                if in_single_file {
                    if file_element_offset + total_entry_offset + std::mem::size_of::<usize>() as u64 >= next_file_element_offset {
                        return Ok(Cursor::new(Vec::new()));
                    }
                } else {
                    if result.is_err() {
                        return Ok(Cursor::new(Vec::new()));
                    }
                }
                buffer.extend_from_slice(&key_len_bytes);

                let key_len = usize::from_ne_bytes(key_len_bytes);

                let mut key_bytes = vec![0u8; key_len];
                file.read_exact(&mut key_bytes)?;
                buffer.extend_from_slice(&key_bytes);

                let mut offset_bytes = [0u8; 8];
                file.read_exact(&mut offset_bytes)?;
                buffer.extend_from_slice(&offset_bytes);
            }
            SSTableElementType::Summary => {
                file.seek(SeekFrom::Start(file_element_offset + total_entry_offset))?;

                if total_entry_offset == 0 {
                    // When reading the start of the summary, read min key len, max key len
                    let mut min_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
                    file.read_exact(&mut min_key_len_bytes)?;
                    buffer.extend_from_slice(&min_key_len_bytes);

                    let min_key_len = usize::from_ne_bytes(min_key_len_bytes);

                    let mut min_key_bytes = vec![0u8; min_key_len];
                    file.read_exact(&mut min_key_bytes)?;
                    buffer.extend_from_slice(&min_key_bytes);

                    let mut max_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
                    file.read_exact(&mut max_key_len_bytes)?;
                    buffer.extend_from_slice(&max_key_len_bytes);

                    let max_key_len = usize::from_ne_bytes(max_key_len_bytes);

                    let mut max_key_bytes = vec![0u8; max_key_len];
                    file.read_exact(&mut max_key_bytes)?;
                    buffer.extend_from_slice(&max_key_bytes);
                } else {
                    // Read key len, key and offset
                    let mut key_len_bytes = [0u8; std::mem::size_of::<usize>()];
                    let result = file.read_exact(&mut key_len_bytes);
                    if in_single_file {
                        if file_element_offset + total_entry_offset + std::mem::size_of::<usize>() as u64 >= next_file_element_offset {
                            return Ok(Cursor::new(Vec::new()));
                        }
                    } else {
                        if result.is_err() {
                            return Ok(Cursor::new(Vec::new()));
                        }
                    }
                    buffer.extend_from_slice(&key_len_bytes);

                    let key_len = usize::from_ne_bytes(key_len_bytes);

                    let mut key_bytes = vec![0u8; key_len];
                    file.read_exact(&mut key_bytes)?;
                    buffer.extend_from_slice(&key_bytes);

                    let mut offset_bytes = [0u8; std::mem::size_of::<usize>()];
                    file.read_exact(&mut offset_bytes)?;
                    buffer.extend_from_slice(&offset_bytes);
                }
            }
            SSTableElementType::BloomFilter => {
                if in_single_file {
                    file.seek(SeekFrom::Start(file_element_offset))?;
                    file.take(next_file_element_offset - file_element_offset).read_to_end(&mut buffer)?;
                } else {
                    file.seek(SeekFrom::Start(0))?;
                    file.read_to_end(&mut buffer)?;
                }
            }
            SSTableElementType::MerkleTree => {
                if in_single_file {
                    file.seek(SeekFrom::Start(file_element_offset))?;
                }
                file.seek(SeekFrom::Start(0))?;
                file.read(&mut buffer)?;
            }
        };

        Ok(Cursor::new(buffer))
    }


    /// Reads min and max key from SSTable at a given path.
    ///
    ///
    /// # Arguments
    ///
    /// * `sstable_base_path` - base path to SSTable.
    /// * `in_single_file` - Indicates whether the table is stored in a single or multiple files.
    ///
    /// # Returns
    ///
    /// A `Result` containing tuple of boxed slices. First position in tuple represents min key and second position represents max key
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when reading contents of SStable file.
    pub(crate) fn get_key_range(sstable_base_path: PathBuf, in_single_file: bool) -> io::Result<(Box<[u8]>, Box<[u8]>)> {
        let mut open_options = OpenOptions::new();
        open_options.read(true).write(false).create(false);

        // Adjust the file path accordingly to in_single_file argument
        let file_path = if in_single_file {
            sstable_base_path.join("SSTable.db")
        } else {
            sstable_base_path.join("SSTable-Summary.db")
        };
        let mut file_handle = open_options.open(file_path)?;

        // Position the file cursor on beginning of summary data
        if in_single_file {
            file_handle.seek(SeekFrom::Start((SSTableElementType::Summary.get_id() * std::mem::size_of::<usize>()) as u64))?;

            let mut summary_offset_bytes = [0u8; std::mem::size_of::<usize>()];
            file_handle.read_exact(&mut summary_offset_bytes)?;
            let summary_offset_bytes = usize::from_ne_bytes(summary_offset_bytes) as u64;

            file_handle.seek(SeekFrom::Start(summary_offset_bytes))?;
        }

        // Read min and max key from summary
        let mut min_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        file_handle.read_exact(&mut min_key_len_bytes)?;
        let min_key_len = usize::from_ne_bytes(min_key_len_bytes);

        let mut min_key_bytes = vec![0u8; min_key_len];
        file_handle.read_exact(&mut min_key_bytes)?;
        let min_key = min_key_bytes.into_boxed_slice();

        let mut max_key_len_bytes = [0u8; std::mem::size_of::<usize>()];
        file_handle.read_exact(&mut max_key_len_bytes)?;
        let max_key_len = usize::from_ne_bytes(max_key_len_bytes);

        let mut max_key_bytes = vec![0u8; max_key_len];
        file_handle.read_exact(&mut max_key_bytes)?;
        let max_key = max_key_bytes.into_boxed_slice();


        Ok((min_key, max_key))
    }

    /// Function that returns the offset from index file of a first key that meets the search criteria for either range or prefix scan
    ///
    ///
    /// # Arguments
    ///
    /// * `sstable_base_path` - base path to SSTable.
    /// * `in_single_file` - Indicates whether the table is stored in a single or multiple files.
    /// * `searched_key` - Can either be min key or prefix based on the scan type.
    /// * `scan_type` - The type of scan operation, range or prefix scan.
    /// * `non_existent_thresh` - if this value is returned that means that sstable doesn't have keys that meet the search criteria
    ///
    /// # Returns
    ///
    /// A `Result` containing offset of first key in Index file that meets the search criteria.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when reading contents of SStable file.
    pub(crate) fn get_sstable_offset(sstable_base_path: PathBuf, in_single_file: bool, searched_key: &[u8], scan_type: ScanType, non_existent_thresh: Option<u64>, compression_dictionary: &mut Option<CompressionDictionary>) -> io::Result<u64> {
        let mut offset = 0;

        let mut open_options = OpenOptions::new();
        open_options.read(true).write(false).create(false);

        // Adjust the file path accordingly to in_single_file argument
        let file_path = if in_single_file {
            sstable_base_path.join("SSTable.db")
        } else {
            sstable_base_path.join("SSTable-Index.db")
        };
        let mut file_handle = open_options.open(file_path)?;

        // Position the file cursor on beginning of summary data
        if in_single_file {
            file_handle.seek(SeekFrom::Start(std::mem::size_of::<usize>() as u64))?;

            let mut index_offset_bytes = [0u8; std::mem::size_of::<usize>()];
            let result = file_handle.read_exact(&mut index_offset_bytes);
            if result.is_err() {
                return Ok(non_existent_thresh.unwrap());
            }
            let index_offset = usize::from_ne_bytes(index_offset_bytes) as u64;

            file_handle.seek(SeekFrom::Start(index_offset))?;
        }

        loop {
            let mut key_len_bytes = [0u8; std::mem::size_of::<usize>()];
            let result = file_handle.read_exact(&mut key_len_bytes);
            if result.is_err() {
                return Ok(non_existent_thresh.unwrap());
            }

            let key_len = usize::from_ne_bytes(key_len_bytes);

            let mut key_bytes = vec![0u8; key_len];
            let result = file_handle.read_exact(&mut key_bytes);
            if result.is_err() {
                return Ok(non_existent_thresh.unwrap());
            }

            let decoded_key = match compression_dictionary {
                Some(compression_dictionary) => compression_dictionary.decode(&key_bytes.to_vec().into_boxed_slice()).unwrap().clone(),
                None => key_bytes.to_vec().into_boxed_slice()
            };

            let mut offset_bytes = [0u8; std::mem::size_of::<usize>()];
            let result = file_handle.read_exact(&mut offset_bytes);
            if result.is_err() {
                return Ok(non_existent_thresh.unwrap());
            }

            let current_offset = usize::from_ne_bytes(offset_bytes);

            match scan_type {
                ScanType::RangeScan => {
                    if decoded_key.as_ref() >= searched_key {
                        break;
                    }
                    offset = current_offset;
                }
                ScanType::PrefixScan => {
                    if decoded_key.as_ref().starts_with(searched_key) {
                        break;
                    }
                    if decoded_key.as_ref() > searched_key {
                        return Ok(non_existent_thresh.unwrap());
                    }
                    offset = current_offset;
                }
            }
        }

        Ok(offset as u64)
    }

    /// Function that returns the offset from data file of a first key that meets the search criteria for either range or prefix scan
    ///
    ///
    /// # Arguments
    ///
    /// * `sstables` - Vector containing sstables
    /// * `current_offsets` - offsets form index file of a first key that meets the search criteria
    /// * `searched_key` - Can either be min key or prefix based on the scan type.
    /// * `scan_type` - The type of scan operation, range or prefix scan.
    /// * `use_variable_encoding` - set to true if the user decided to use variable encoding
    ///
    /// # Returns
    ///
    /// A `Result` containing updated offsets of all sstables of first key that meets the serach criteria
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when reading contents of SStable file.
    pub(crate) fn update_sstable_offsets(sstables: &mut Vec<SSTable>, mut current_offsets: Vec<u64>, searched_key: &[u8], scan_type: ScanType, use_variable_encoding: bool, compression_dictionary: &mut Option<CompressionDictionary>) -> io::Result<Vec<u64>> {
        for (index, sstable) in sstables.iter_mut().enumerate() {
            loop {
                let data = sstable.get_entry_from_data_file(current_offsets[index], None, None, use_variable_encoding);
                if let Some(((key, _), offset)) = data {
                    let decoded_key = match compression_dictionary {
                        Some(compression_dictionary) => compression_dictionary.decode(&key.to_vec().into_boxed_slice()).unwrap().clone(),
                        None => key.to_vec().into_boxed_slice()
                    };

                    match scan_type {
                        ScanType::RangeScan => {
                            if decoded_key.as_ref() >= searched_key {
                                break;
                            }
                            current_offsets[index] += offset;
                        }
                        ScanType::PrefixScan => {
                            if decoded_key.starts_with(searched_key) {
                                break;
                            }
                            current_offsets[index] += offset;
                        }
                    }
                } else {
                    break;
                }
            }
        }
        Ok(current_offsets)
    }

    /// Writes the provided data to a file with the given path postfix and returns a mutable reference to the file.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be written to the file.
    /// * `path_postfix` - The postfix of the path of the file.
    ///
    /// # Returns
    ///
    /// A `Result` containing a mutable reference to the opened file.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when writing to the file or flushing the buffer.
    fn write_to_file(&mut self, data: &[u8], path_postfix: &str) -> io::Result<&mut File> {
        // Open the file directly
        let file = self.open_file(path_postfix)?;

        if data.len() > 0 {
            file.write_all(data)?;
        }

        file.flush()?;

        Ok(file)
    }

    /// Opens a file with the given postfix in read, write & create mode.
    ///
    /// # Arguments
    ///
    /// * `path_postfix` - The postfix of the path of the file.
    ///
    /// # Returns
    ///
    /// A `Result` containing a mutable reference to the opened `File`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there's an issue when opening the file.
    fn open_file(&mut self, path_postfix: &str) -> io::Result<&mut File> {
        let file_path = self.base_path.join(path_postfix);

        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true).create(true);

        // Insert the default File into the map
        let file_handle = self
            .file_handles
            .entry(path_postfix.to_string())
            .or_insert_with(|| {
                open_options.open(&file_path).expect("Failed to open file")
            });

        // Return a mutable reference to the File in the map
        Ok(file_handle)
    }
}