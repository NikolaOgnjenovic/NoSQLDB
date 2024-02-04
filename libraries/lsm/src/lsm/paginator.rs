use segment_elements::MemoryEntry;
use crate::LSM;
use crate::lsm::ScanType;

/// A Paginator provides paginated access to entries in an LSM (Log-Structured Merge) tree.
pub struct Paginator<'a>{
    lsm: &'a mut LSM,
    cached_entry_index: usize,
    cached_entries: Vec<(Box<[u8]>, MemoryEntry)>
}

impl<'a> Paginator<'a> {
    /// Creates a new Paginator instance associated with the provided LSM.
    ///
    /// # Arguments
    ///
    /// * `lsm` - A reference to the LSM instance.
    ///
    /// # Returns
    ///
    /// A new Paginator instance.
    pub fn new(lsm: &'a mut LSM) -> Self {
        Self {
            lsm,
            cached_entry_index: 0,
            cached_entries: vec![]
        }
    }

    /// Internal method to perform prefix or range scan based on parameters.
    ///
    /// # Arguments
    ///
    /// * `start_key` - Optional start key for the scan range.
    /// * `end_key` - Optional end key for the scan range.
    /// * `prefix` - Optional prefix for prefix scan.
    /// * `scan_type` - Type of the scan (Prefix or Range).
    /// * `page_number` - The page number to retrieve.
    /// * `page_size` - The size of each page.
    ///
    /// # Returns
    ///
    /// A vector of entries based on the scan criteria.
    fn scan_entries(
        &mut self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        prefix: Option<&[u8]>,
        scan_type: ScanType,
        page_number: usize,
        page_size: usize,
    ) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>> {
        let mut result: Vec<(Box<[u8]>, MemoryEntry)> = vec![];

        let mut entries_traversed = 0;
        let mut iter = self
            .lsm
            .iter(start_key, end_key, prefix, scan_type)
            .expect("Failed to get LSM iterator");

        while let Some((key, memory_entry)) = iter.next() {
            if entries_traversed >= (page_number + 1) * page_size {
                break;
            }

            if entries_traversed >= page_number * page_size {
                result.push((key, memory_entry));
            }
            entries_traversed += 1;
        }

        Ok(result)
    }

    /// Performs a prefix scan and retrieves entries for a specific page (0-indexed).
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to scan for.
    /// * `page_number` - The page number to retrieve.
    /// * `page_size` - The size of each page.
    ///
    /// # Returns
    ///
    /// A vector of entries based on the prefix scan criteria.
    pub fn prefix_scan(
        &mut self,
        prefix: &[u8],
        page_number: usize,
        page_size: usize,
    ) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>> {
        self.scan_entries(None, None, Some(prefix), ScanType::PrefixScan, page_number, page_size)
    }

    /// Performs a range scan and retrieves entries for a specific page.
    ///
    /// # Arguments
    ///
    /// * `min_key` - The minimum key of the range.
    /// * `max_key` - The maximum key of the range.
    /// * `page_number` - The page number to retrieve.
    /// * `page_size` - The size of each page.
    ///
    /// # Returns
    ///
    /// A vector of entries based on the range scan criteria.
    pub fn range_scan(
        &mut self,
        min_key: &[u8],
        max_key: &[u8],
        page_number: usize,
        page_size: usize,
    ) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>> {
        self.scan_entries(Some(min_key), Some(max_key), None, ScanType::RangeScan, page_number, page_size)
    }

    /// Retrieves the next entry based on prefix scan.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to scan for.
    ///
    /// # Returns
    ///
    /// An optional tuple containing the key and memory entry of the next entry, or `None` if no more entries are available.
    pub fn prefix_iterate_next(&mut self, prefix: &[u8]) -> std::io::Result<Option<(Box<[u8]>, MemoryEntry)>> {
        self.iterate_next_impl(|this| this.prefix_scan(prefix, this.cached_entry_index, 1))
    }

    /// Retrieves the next entry based on range scan.
    ///
    /// # Arguments
    ///
    /// * `min_key` - The minimum key of the range.
    /// * `max_key` - The maximum key of the range.
    ///
    /// # Returns
    ///
    /// An optional tuple containing the key and memory entry of the next entry, or `None` if no more entries are available.
    pub fn range_iterate_next(
        &mut self,
        min_key: &[u8],
        max_key: &[u8],
    ) -> std::io::Result<Option<(Box<[u8]>, MemoryEntry)>> {
        self.iterate_next_impl(|this| this.range_scan(min_key, max_key, this.cached_entry_index, 1))
    }

    /// Internal method to implement the common logic for fetching the next entry.
    ///
    /// # Arguments
    ///
    /// * `scan_fn` - A closure that performs the specific scan operation.
    ///
    /// # Returns
    ///
    /// An optional tuple containing the key and memory entry of the next entry, or `None` if no more entries are available.
    fn iterate_next_impl<F>(
        &mut self,
        scan_fn: F,
    ) -> std::io::Result<Option<(Box<[u8]>, MemoryEntry)>>
        where
            F: FnOnce(&mut Self) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>>,
    {
        // Check if by calling prev next returns a cached entry
        if self.cached_entry_index > 0 && self.cached_entry_index < self.cached_entries.len() - 1 {
            let next_cached_entry = self.cached_entries[self.cached_entry_index].clone();
            self.cached_entry_index += 1;
            return Ok(Some(next_cached_entry.to_owned()));
        }

        let scan_result = scan_fn(self)?;
        let scanned_entry = scan_result.get(0);

        if scanned_entry.is_none() {
            return Ok(None);
        }

        let scanned_entry = scanned_entry.unwrap();
        self.cached_entries.push(scanned_entry.clone());
        self.cached_entry_index += 1;

        Ok(Some(scanned_entry.to_owned()))
    }

    /// Retrieves the previous entry in the paginator cache.
    ///
    /// # Returns
    ///
    /// An optional tuple containing the key and memory entry of the previous entry, or `None` if no more entries are available.
    pub fn iterate_prev(&mut self) -> std::io::Result<Option<(Box<[u8]>, MemoryEntry)>> {
        // If the cache is empty or the cached index is 0, there is no previous entry
        if self.cached_entries.is_empty() || self.cached_entry_index == 0 || self.cached_entry_index > self.cached_entries.len() {
            return Ok(None);
        }

        // Find the previous entry in the cache and return it
        let previous_entry = self.cached_entries[self.cached_entry_index - 1].clone();
        self.cached_entry_index -= 1;

        return Ok(Some(previous_entry));
    }

    /// Clears the cache and resets the cached entry index, stopping further iteration.
    pub fn iterate_stop(&mut self) {
        self.cached_entries.clear();
        self.cached_entry_index = 0;
    }
}