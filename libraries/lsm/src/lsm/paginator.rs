use crate::lsm::ScanType;
use crate::LSM;
use segment_elements::MemoryEntry;
use crate::lsm::iterator::LSMIterator;

/// A Paginator provides paginated access to entries in an LSM (Log-Structured Merge) tree.
pub struct Paginator<'a> {
    lsm: &'a mut LSM,
    cached_entry_index: usize,
    cached_entries: Vec<(Box<[u8]>, MemoryEntry)>,
    lsm_iter: Option<LSMIterator<'a>>,
    entries_itered_count: usize
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
            cached_entries: vec![],
            lsm_iter: None,
            entries_itered_count: 0
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
    // fn scan_entries(
    fn scan_entries(
        &mut self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        prefix: Option<&[u8]>,
        scan_type: ScanType,
        page_number: usize,
        page_size: usize,
    ) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>>
    {
        let mut result: Vec<(Box<[u8]>, MemoryEntry)> = vec![];

        let mut entries_traversed = 0;

        // Get the lsm iter (cached when calling range_iter, prefix_iter or create a new one)
        let mut lsm_iter_unwrapped = std::mem::replace(&mut self.lsm_iter, None)
            .unwrap_or_else(|| self.lsm.iter(start_key, end_key, prefix, scan_type)
                .expect("Failed to get LSM iterator"));

        // Iterate until the correct page is found
        while let Some((key, memory_entry)) = lsm_iter_unwrapped.next() {
            if entries_traversed >= (page_number - self.entries_itered_count + 1) * page_size {
                break;
            }

            if entries_traversed >= page_number * page_size {
                result.push((key, memory_entry));
            }
            entries_traversed += 1;
            if self.lsm_iter.is_some() {
                self.entries_itered_count += 1;
            }
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
        page_index: usize,
        page_size: usize,
    ) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>>
    {
        self.iterate_stop();

        self.scan_entries(
            None,
            None,
            Some(prefix),
            ScanType::PrefixScan,
            page_index,
            page_size,
        )
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
    ) -> std::io::Result<Vec<(Box<[u8]>, MemoryEntry)>>
    {
        self.iterate_stop();

        self.scan_entries(
            Some(min_key),
            Some(max_key),
            None,
            ScanType::RangeScan,
            page_number,
            page_size,
        )
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
    pub fn prefix_iterate_next(
        &mut self,
        prefix: &[u8],
    ) -> std::io::Result<Option<(Box<[u8]>, MemoryEntry)>>
    {
        let next_is_in_cache = self.check_next_in_cache();
        if next_is_in_cache.is_some() {
            return Ok(next_is_in_cache);
        }

        let scan_result = self.scan_entries(
            None,
            None,
            Some(prefix),
            ScanType::PrefixScan,
            self.cached_entry_index,
            1,
        )?;

        Ok(self.process_scan_result(scan_result))
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
    ) -> std::io::Result<Option<(Box<[u8]>, MemoryEntry)>>
    {
        let next_is_in_cache = self.check_next_in_cache();
        if next_is_in_cache.is_some() {
            return Ok(next_is_in_cache);
        }

        let scan_result = self.scan_entries(
            Some(min_key),
            Some(max_key),
            None,
            ScanType::RangeScan,
            self.cached_entry_index,
            1,
        )?;

        Ok(self.process_scan_result(scan_result))
    }

    /// Checks if the next entry is in the cache and returns it if available.
    ///
    /// # Returns
    ///
    /// An optional tuple containing the key and memory entry of the next entry, or `None` if no more entries are in the cache.
    fn check_next_in_cache(&mut self) -> Option<(Box<[u8]>, MemoryEntry)> {
        // Check if by previously calling prev, next now returns a cached entry
        if !self.cached_entries.is_empty() && self.cached_entry_index < self.cached_entries.len() - 1 {
            let next_cached_entry = self.cached_entries[self.cached_entry_index].clone();
            self.cached_entry_index += 1;
            return Some(next_cached_entry.to_owned());
        }

        return None;
    }

    /// Processes the scan result and updates the cache.
    ///
    /// # Arguments
    ///
    /// * `scan_result` - The result of a scan operation.
    ///
    /// # Returns
    ///
    /// An optional tuple containing the key and memory entry of the first entry in the scan result, or `None` if the scan result is empty.
    fn process_scan_result(&mut self, scan_result: Vec<(Box<[u8]>, MemoryEntry)>) -> Option<(Box<[u8]>, MemoryEntry)> {
        if let Some(scanned_entry) = scan_result.get(0).cloned() {
            self.cached_entries.push(scanned_entry.clone());
            self.cached_entry_index += 1;
            Some(scanned_entry.to_owned())
        } else {
            None
        }
    }

    /// Retrieves the previous entry in the paginator cache.
    ///
    /// # Returns
    ///
    /// An optional tuple containing the key and memory entry of the previous entry, or `None` if no more entries are available.
    pub fn iterate_prev(&mut self) -> std::io::Result<Option<(Box<[u8]>, MemoryEntry)>> {
        // If the cache is empty or the cached index is 0, there is no previous entry
        if self.cached_entries.is_empty() || (!self.cached_entries.is_empty() && self.cached_entry_index == 0)
        {
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
        self.lsm_iter = None;
        self.entries_itered_count = 0;
    }
}
