use crate::lsm::ScanType;
use crate::sstable::SSTable;
use compression::CompressionDictionary;
use segment_elements::MemoryEntry;

/// Struct for iterating over entries in memory tables and sstables
pub struct LSMIterator<'a> {
    memory_table_entries: Vec<(Box<[u8]>, MemoryEntry)>,
    memory_offset: usize,
    sstables: Vec<SSTable>,
    offsets: Vec<u64>,
    scan_type: ScanType,
    use_variable_encoding: bool,
    upper_bound: Box<[u8]>,
    compression_dictionary: &'a mut Option<CompressionDictionary>,
}

impl<'a> LSMIterator<'a> {
    pub(crate) fn new(
        memory_table_entries: Vec<(Box<[u8]>, MemoryEntry)>,
        memory_offset: usize,
        sstables: Vec<SSTable>,
        offsets: Vec<u64>,
        scan_type: ScanType,
        use_variable_encoding: bool,
        upper_bound: Box<[u8]>,
        compression_dictionary: &'a mut Option<CompressionDictionary>,
    ) -> Self {
        LSMIterator {
            memory_table_entries,
            memory_offset,
            sstables,
            offsets,
            scan_type,
            use_variable_encoding,
            upper_bound,
            compression_dictionary,
        }
    }
}

impl<'a> Iterator for LSMIterator<'a> {
    type Item = (Box<[u8]>, MemoryEntry);

    fn next(&mut self) -> Option<Self::Item> {
        //pushed indicates whether we have more entries from memory tables
        let mut pushed = false;
        let mut copy_offsets = self.offsets.clone();
        let memory_table_entry = if self.memory_offset < self.memory_table_entries.len() {
            pushed = true;
            copy_offsets.push(self.memory_offset as u64);
            let (key, entry) = self.memory_table_entries[self.memory_offset].clone();
            let encoded_key = match self.compression_dictionary {
                Some(compression_dictionary) => compression_dictionary
                    .encode(&key.to_vec().into_boxed_slice())
                    .unwrap()
                    .clone(),
                None => key.to_vec().into_boxed_slice(),
            };
            Option::from(((encoded_key, entry), 1u64))
        } else {
            None
        };

        // option entry contains one entry from each sstable and we later combine it with entries from memory tables
        // also if we stumble upon entry with tombstone=true we just skip it and move on to the next one immediately
        let mut option_entries: Vec<Option<_>> = self
            .sstables
            .iter_mut()
            .zip(self.offsets.iter())
            .map(|(sstable, offset)| {
                let return_value;
                let mut new_offset = *offset; // new offset from which we continue reading in sstable
                let mut added_offset = 0; // how many bytes we have read from stable
                loop {
                    if let Some((option_entry, length)) = sstable.get_entry_from_data_file(
                        new_offset,
                        None,
                        None,
                        self.use_variable_encoding,
                    ) {
                        added_offset += length;
                        new_offset += length;
                        let _ = &*option_entry.0;
                        if option_entry.1.get_tombstone() {
                            continue;
                        } else {
                            return_value = Option::from((option_entry, added_offset));
                            break;
                        }
                    } else {
                        return_value = None;
                        break;
                    }
                }
                return_value
            })
            .collect();

        option_entries.push(memory_table_entry);

        // if all entries are none return None and drop the iterator
        if option_entries.iter().all(Option::is_none) {
            return None;
        }

        // need all indexes from entries in original vector
        let enumerated_entries: Vec<_> = option_entries.iter().enumerate().collect();

        // find indexes of entries with minimum keys
        let min_indexes =
            SSTable::find_min_keys(&enumerated_entries, false, self.compression_dictionary);

        let min_entries: Vec<_> = min_indexes
            .iter()
            .map(|index| enumerated_entries[*index].clone())
            .collect();

        // update offsets
        let _ = min_entries.iter().for_each(|(index, element)| {
            copy_offsets[*index] += element.as_ref().unwrap().1.clone();
        });

        // find entry with the biggest timestamp
        let max_index = SSTable::find_max_timestamp(&min_entries);

        let (key, entry) = enumerated_entries[max_index].1.as_ref().unwrap().0.clone();
        let decoded_key = match self.compression_dictionary {
            Some(compression_dictionary) => compression_dictionary
                .decode(&key.to_vec().into_boxed_slice())
                .unwrap()
                .clone(),
            None => key.to_vec().into_boxed_slice(),
        };
        let return_entry = (decoded_key, entry);
        let _ = &*return_entry.0;

        // update offsets in LSMIterator
        if pushed {
            self.memory_offset = copy_offsets.pop().unwrap() as usize;
            self.offsets = copy_offsets;
        } else {
            self.offsets = copy_offsets;
        }

        // check if we surpassed the upper bound, if so return None and drop iterator
        match self.scan_type {
            ScanType::RangeScan => {
                if return_entry.0 > self.upper_bound {
                    return None;
                }
            }
            ScanType::PrefixScan => {
                if !return_entry.0.starts_with(&self.upper_bound) {
                    return None;
                }
            }
        }

        Some(return_entry)
    }
}
