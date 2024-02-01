use segment_elements::MemoryEntry;
use crate::LSM;
use crate::lsm::ScanType;

pub struct Paginator<'a>{
    lsm: &'a LSM,
    current_iter_entry: usize,
    cached_iter_entries: Vec<(Box<[u8]>, MemoryEntry)>
}

impl<'a> Paginator<'a> {
    pub fn new(lsm: &'a LSM) -> Self {
        Self {
            lsm,
            current_iter_entry: 0,
            cached_iter_entries: vec![]
        }
    }

    pub fn prefix_scan(&mut self, prefix: &[u8], page_number: usize, page_size: usize, use_variable_encoding: bool) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>> {
        // Extract parameters from LSM
        let (merged_memory_entries, mut memory_offset, mut sstables, mut offsets) = self.lsm.get_prefix_scan_parameters(prefix)?;

        let mut result = vec![];
        let mut entries_read = 0; // Keep track of the read entries
        while let Some((memory_entry, updated_offsets)) = LSM::next(&merged_memory_entries, memory_offset, &mut sstables, offsets, prefix, ScanType::PrefixScan, use_variable_encoding) {
            // If finished reading page, break
            if entries_read > page_number * page_size {
                break;
            }

            // If in current page, appent to result
            if entries_read > (page_number - 1) * page_size {
                result.push(memory_entry);
            }

            // Iterate further
            memory_offset = updated_offsets[updated_offsets.len() - 1] as usize;
            offsets = updated_offsets[..updated_offsets.len() - 1].to_vec();

            entries_read += 1;
        }

        Ok(result)
    }

    pub fn prefix_iterate(&mut self, prefix: &[u8], use_variable_encoding: bool) -> std::io::Result<(Box<[u8]>, MemoryEntry)> {
        // self.cached_iter_entries.append()
    }
}