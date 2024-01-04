use std::fs::{create_dir_all, OpenOptions};
use std::io;
use std::io::{BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use bloom_filter::BloomFilter;
use segment_elements::MemoryEntry;
use merkle_tree::merkle_tree::MerkleTree;

pub struct SSTable<'a> {
    dir: &'a Path,
    inner_mem: &'a Box<dyn segment_elements::SegmentTrait + Send>
}

impl<'a> SSTable<'a> {
    pub fn new(dir: &'a Path, inner_mem: &'a Box<dyn segment_elements::SegmentTrait + Send>) -> io::Result<SSTable<'a>> {
        // Create directory if it doesn't exist
        create_dir_all(dir)?;

        let sstable = Self {
            dir,
            inner_mem
        };

        Ok(sstable)
    }

    pub fn flush(&mut self, summary_density: usize) -> io::Result<()> {

        let (data, index_builder, bloom_filter) = self.build_data_and_index_and_filter()?;
        self.write_to_index_file(&index_builder)?;
        self.write_to_filter_file(&bloom_filter)?;

        let index_summary = self.build_summary(&index_builder, summary_density);
        self.write_to_summary_file(&index_summary)?;
        self.write_to_data_file(&data)?;

        let serialized_merkle_tree = MerkleTree::new(&data).serialize();
        self.write_to_merkle_file(&serialized_merkle_tree)?;

        Ok(())
    }

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

    fn build_summary(&self, index_builder: &[(Vec<u8>, u64)], summary_density: usize) -> Vec<Box<[u8]>> {
        if index_builder.len() == 0 || summary_density < 1 {
            return vec![];
        }

        let mut summary: Vec<Box<[u8]>> = Vec::new();

        // Find the min and max keys
        let (min_key, _) = index_builder.first().unwrap();
        let (max_key, _) = index_builder.last().unwrap();

        // Add the min and max keys to the summary
        summary.push(Box::from(min_key.len().to_ne_bytes()));
        summary.push(min_key.as_slice().into());
        summary.push(Box::from(max_key.len().to_ne_bytes()));
        summary.push(max_key.as_slice().into());

        // Add every step-th key and its offset to the summary
        for i in (0..index_builder.len()).step_by(summary_density) {
            let (key, _) = &index_builder[i];
            summary.push(Box::from(key.len().to_ne_bytes()));
            summary.push(key.clone().into_boxed_slice());
            let offset_index = if i == 0 {
                0
            } else {
                (key.len() + key.len().to_ne_bytes().len() + std::mem::size_of::<u64>()) * i
            };
            summary.push(Box::from(offset_index.to_ne_bytes()));
        }

        summary
    }

    fn write_to_data_file(&self, data: &[u8]) -> io::Result<()> {
        let mut data_file = self.open_buf_writer("-Data.db", false)?;

        data_file.write_all(&data)?;

        Ok(())
    }

    fn write_to_merkle_file(&self, serialized_merkle_tree: &[u8]) -> io::Result<()> {
        let mut merkle_file = self.open_buf_writer("-Merkle.db", false)?;

        merkle_file.write_all(&serialized_merkle_tree)?;

        Ok(())
    }

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

    fn write_to_summary_file(&self, index_summary: &[Box<[u8]>]) -> io::Result<()> {
        let mut summary_file = self.open_buf_writer("-Summary.db", false)?;

        for el in index_summary {
            summary_file.write_all(el)?;
        }

        Ok(())
    }

    fn write_to_filter_file(&self, bloom_filter: &BloomFilter) -> io::Result<()> {
        let mut filter_file = self.open_buf_writer("-Filter.db", false)?;
        filter_file.write_all(&bloom_filter.serialize())?;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<MemoryEntry> {
        if self.is_key_in_bloom_filter(key).unwrap_or(false) {
            if let Some(offset) = self.find_offset_in_summary(key) {
                return self.read_entry_from_data_file(offset);
            }
        }

        None
    }

    fn is_key_in_bloom_filter(&self, key: &[u8]) -> io::Result<bool> {
        // Read the entire filter file into memory (assuming it's not too large)
        let mut filter_data = Vec::new();

        let filter_file = self.open_buf_writer("-Filter.db", true)?;
        filter_file.get_ref().read_to_end(&mut filter_data).unwrap();

        // Use the deserialize method of BloomFilter
        let bloom_filter = BloomFilter::deserialize(&filter_data).unwrap();

        Ok(bloom_filter.contains(key))
    }

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