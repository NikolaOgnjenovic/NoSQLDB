use std::cmp::{min, Ordering};
use std::error::Error;
use std::fs::{File, read_dir};
use std::io;
use std::io::BufRead;
use std::path::PathBuf;
use segment_elements::{MemoryEntry, TimeStamp};
use compression::CompressionDictionary;
use crate::sstable::SSTable;
use db_config::{ DBConfig, CompactionAlgorithmType };
use write_ahead_log::WriteAheadLog;
use lru_cache::LRUCache;
use crate::lsm::ScanType::RangeScan;

use crate::mem_pool::MemoryPool;
use crate::memtable::MemoryTable;

struct LSMConfig {
    // Base directory path where all other SSTable directories will be stored
    parent_dir: PathBuf,
    // Maximum number of levels
    max_level: usize,
    // Maximum number of SSTables per level
    max_per_level: usize,
    // The compaction algorithm in use
    compaction_algorithm: CompactionAlgorithmType,
    in_single_file: bool,
    summary_density: usize,
    index_density: usize,
    compaction_enabled: bool,
}

impl LSMConfig {
    fn from(dbconfig: &DBConfig) -> Self {
        Self {
            parent_dir: PathBuf::from(&dbconfig.sstable_dir),
            max_level: dbconfig.lsm_max_level,
            max_per_level: dbconfig.lsm_max_per_level,
            compaction_algorithm: dbconfig.compaction_algorithm_type,
            compaction_enabled: dbconfig.compaction_enabled,
            in_single_file: dbconfig.sstable_single_file,
            summary_density: dbconfig.summary_density,
            index_density: dbconfig.index_density
        }
    }
}

/// LSM(Log-Structured Merge Trees) struct for optimizing write-intensive workloads
pub struct LSM {
    // Each vector represents one level containing directory names for SSTables
    sstable_directory_names: Vec<Vec<PathBuf>>,
    wal: WriteAheadLog,
    mem_pool: MemoryPool,
    lru_cache: LRUCache,
    compression_dictionary: Option<CompressionDictionary>,
    config: LSMConfig
}

impl LSM {
    /// Creates a new LSM instance.
    ///
    /// # Arguments
    ///
    /// * `parent_dir` - The base directory path where all SSTable folders will be
    /// * `db_config` - Configuration file.
    ///
    /// # Returns
    ///
    /// LSM instance
    pub fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let lru_cache = LRUCache::new(dbconfig.cache_max_size);
        let mem_pool = MemoryPool::new(dbconfig)?;
        let wal = WriteAheadLog::new(dbconfig)?;

        let mut sstable_directory_names = vec![vec![]; dbconfig.lsm_max_level];

        let dirs = read_dir(&dbconfig.sstable_dir)?
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|entry| entry.is_dir()).collect::<Vec<PathBuf>>();

        for dir in dirs {
            let level = dir.to_str().unwrap().split("_").collect::<Vec<&str>>()[1].parse::<usize>().unwrap();
            sstable_directory_names[level-1].push(dir);
        }

        Ok(LSM {
            config: LSMConfig::from(dbconfig),
            wal,
            mem_pool,
            lru_cacheies/lsm/src/lib.rs
libraries/lsm/src/lsm.rs
libraries/lsm/src/mem_pool.rs
libraries/lsm/src/sstable.rs
libraries/segment_elements/src/lib.rs
libraries/segment_elements/src/memory_entry.rs ,
            compression_dictionary: match dbconfig.use_compression {
                true => Some(CompressionDictionary::load(dbconfig.compression_dictionary_path.as_str()).unwrap()),
                false => None
            },
            sstable_directory_names,
        })
    }



    /// Creates directory name for new SSTable. Suffix is determined by the in_single_file parameter.
    ///
    /// # Arguments
    ///
    /// * `level` - The level of new SSTable
    /// * `in_single_file` - Boolean containing information whether SSTable is in one file
    ///
    /// # Returns
    ///
    /// Folder name of our new SSTable.
    pub fn get_directory_name(level: usize, in_single_file: bool) -> PathBuf {
        let suffix = String::from(if in_single_file { "s" } else { "m" });

        PathBuf::from(format!("sstable_{}_{}_{}", level + 1, TimeStamp::Now.get_time(), suffix))
    }


    /// Determines whether sstable is in a single file by reading its path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the SSTable
    ///
    /// # Returns
    ///
    /// Boolean indicating the construction of SSTable
    fn is_in_single_file(path: &PathBuf) -> bool {
        path.to_str().unwrap().chars().last().unwrap() == 's'
    }

    /// Function that returns full path to a sstable and wether or not is it in single file
    ///
    /// # Arguments
    ///
    /// * `sstable_directory` - The directory of sstable
    ///
    /// # Returns
    ///
    /// Full path and boolean indicating its structure
    fn get_sstable_path(&self, sstable_directory: &PathBuf) -> (PathBuf, bool) {
        let in_single_file = LSM::is_in_single_file(sstable_directory);
        let full_path = self.config.parent_dir.join(sstable_directory);
        (full_path, in_single_file)
    }


    /// Finds SSTables with similar key ranges as the SSTable that started compaction process.
    ///
    /// # Arguments
    ///
    /// * `sstable_directory_names` - Directory names of the sstables
    /// * `parent_dir` - Parent directory
    /// * `main_min_key` - Min key from main SSTable
    /// * `main_max_key` - Max key from main SSTable
    /// * `level` - One level below our main SSTable that started compaction process
    ///
    /// # Returns
    ///
    /// A `Result` containing vector with tuple as elements.
    /// Each tuple contains index of a path in sstable_directory_names[level] as well as an actual path to that SSTable.
    /// The purpose of an index is to be able to quickly delete all the tables involved in compaction process later.
    fn find_similar_key_ranges<'a>(sstable_directory_names: &'a Vec<Vec<PathBuf>>, parent_dir: &'a PathBuf, main_min_key: &[u8], main_max_key: &[u8], level:usize) -> io::Result<Vec<(usize, &'a PathBuf)>> {
        let base_paths = sstable_directory_names[level]
            .iter()
            .enumerate()
            .filter(|(_, path)| {
                let sstable_base_path = parent_dir.join(path);
                let in_single_file = LSM::is_in_single_file(path);
                match SSTable::get_key_range(sstable_base_path, in_single_file) {
                    Ok((min_key, max_key)) => max_key >= Box::from(main_max_key) && min_key <= Box::from(main_min_key),
                    Err(_) => false,
                }
            })
            .collect();

        Ok(base_paths)
    }


    /// Function that returns bytes representing entry that is associated with a given key if it exists
    /// Also it inserts the record in lru cache if it exists.
    /// If record doesn't exist, it still gets inserted into cache as unsuccessful get request
    ///
    /// # Arguments
    ///
    /// * `key` - The key that user passed to our program
    ///
    /// # Returns
    ///
    /// An io::Result containing bytes representing data associated with a given key.
    /// Bytes are wrapped in option because key may not be present in our database
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<MemoryEntry>> {
        // todo!() da li deserijalizacija bajtova moze biti problem ako se radi o probabilistickim strukturama??
        if let Some(memory_entry) = self.mem_pool.get(key) {
            self.lru_cache.insert(&key, Some(memory_entry.clone()));
            return Ok(Some(memory_entry.clone()));
        }
        if let Some(data) = self.lru_cache.get(key) {
            let (key, memory_entry) = match MemoryEntry::deserialize(data.as_ref()) {
                Ok((key, memory_entry)) => (key, memory_entry),
                Err(_) => return Ok(None),
            };
            self.lru_cache.insert(&key, Some(memory_entry.clone()));
            return Ok(Some(memory_entry.clone()));
        }
        for level in &self.sstable_directory_names {
            for sstable_dir in level.iter().rev() {
                let (path, in_single_file) = self.get_sstable_path(sstable_dir);
                let mut sstable = SSTable::open(path, in_single_file)?;
                if let Some(data) = sstable.get(key, self.config.index_density, &mut self.compression_dictionary) {
                    self.lru_cache.insert(key, Some(data.clone()));
                    return Ok(Some(data));
                }
            }
        }
        self.lru_cache.insert(key, None);
        Ok(None)
    }

    /// Function that inserts entry into database, First it gets inserted into wal and then into read/write memory table
    /// Also gives signal for flushing process if needed
    ///
    /// # Arguments
    ///
    /// * `key` - The key that user passed to our program
    /// * `value` - The value that user passed to our program
    /// * `time_stamp` - the time when event took place
    ///
    /// # Returns
    ///
    /// An io::Result representing the success of operation
    pub fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> io::Result<()> {
        self.wal.insert(key, value, time_stamp)?;
        if let Some(memory_table) = self.mem_pool.insert(key, value, time_stamp)? {
            self.flush(memory_table)?;
        }


        Ok(())
    }


    /// Function that delets entry into database, First we put this record in wal and the in read/write memory table
    /// Also gives signal for flushing process if needed
    ///
    /// # Arguments
    ///
    /// * `key` - The key that user passed to our program
    /// * `time_stamp` - the time when event took place
    ///
    /// # Returns
    ///
    /// An io::Result representing the success of operation
    pub fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> io::Result<()> {
        self.wal.delete(key, time_stamp)?;
        if let Some(memory_table) = self.mem_pool.delete(key, time_stamp)? {
            self.flush(memory_table)?;
        }
        Ok(())
    }

    /// FLushes MemTable onto disk and starts the compaction process if necessary.
    ///
    /// # Arguments
    ///
    /// * `inner_mem` - The MemTable that needs to be flushed.
    /// * `db_config` - Configuration file.
    ///
    /// # Returns
    ///
    /// io::Result indicating success of flushing process
    pub(crate) fn flush<'a>(&mut self, mem_table: MemoryTable) -> io::Result<()> {
        let in_single_file = self.config.in_single_file;
        let summary_density = self.config.summary_density;
        let index_density = self.config.index_density;
        let directory_name = LSM::get_directory_name(0, in_single_file);
        let sstable_base_path = self.config.parent_dir.join(directory_name.as_path());

        let mut sstable = SSTable::open(sstable_base_path, in_single_file)?;
        let flush_bytes = sstable.flush(mem_table, summary_density, index_density, Some(&mut self.lru_cache), &mut self.compression_dictionary)?;
        let mem_table_byte_size = flush_bytes.get_data_len();
        self.wal.add_to_starting_byte(mem_table_byte_size).unwrap();
        self.wal.remove_flushed_wals().unwrap();

        self.sstable_directory_names[0].push(PathBuf::from(directory_name));
        if self.config.compaction_enabled && self.sstable_directory_names[0].len() > self.config.max_per_level {
            if self.config.compaction_algorithm == CompactionAlgorithmType::SizeTiered {
                self.size_tiered_compaction(0)?;
            } else {
                self.leveled_compaction(0)?;
            }
        }

        Ok(())
    }

    /// Size-tiered compaction algorithm. Deletes all SSTables on current level and makes one bigger table located one level below
    /// This process can be propagated through levels
    ///
    /// # Arguments
    ///
    /// * `level` - The level where compactions started
    ///
    /// # Returns
    ///
    /// io::Result indicating success of SSTable merging process
    fn size_tiered_compaction(&mut self, mut level: usize) -> io::Result<()> {
        let merged_in_single_file = self.config.in_single_file;

        while self.sstable_directory_names[level].len() > self.config.max_per_level {
            let mut sstable_base_paths = Vec::new();
            let mut sstable_single_file = Vec::new();

            // Find all SSTables that need to be merged and create vector of booleans indicating whether each SSTable is in a single file
            for path in &self.sstable_directory_names[level] {
                let base_path = self.config.parent_dir.join(path);
                sstable_base_paths.push(base_path);
                sstable_single_file.push(LSM::is_in_single_file(path));
            }

            // Make a name for new SSTable and convert PathBuf into Path
            let sstable_base_paths:Vec<_> = sstable_base_paths.iter().map(|path_buf| path_buf.to_owned()).collect();
            let merged_directory = PathBuf::from(LSM::get_directory_name(level+1, merged_in_single_file));
            let merged_base_path = self.config.parent_dir.join(merged_directory.clone());

            // Merge them all together, push merged SSTable into sstable_directory_names and delete all SSTables involved in merging process
            SSTable::merge(sstable_base_paths, sstable_single_file, &merged_base_path, merged_in_single_file, self.config.summary_density, self.config.index_density, &mut self.compression_dictionary)?;
            self.sstable_directory_names[level].clear();
            self.sstable_directory_names[level+1].push(merged_directory);

            // Check for possibility of another compaction occurring
            level += 1;
            if level >= self.config.max_level {
                break;
            }
        }
        Ok(())
    }


    /// Leveled compaction algorithm.
    /// Chooses oldest table on current level and merges it with sstables that have similar key ranges from one level below
    /// This process creates one bigger sstable that gets placed one level below current
    /// This process can be propagated through levels
    ///
    /// # Arguments
    ///
    /// * `level` - The level where compactions started
    ///
    /// # Returns
    ///
    /// io::Result indicating success of SSTable merging process
    fn leveled_compaction(&mut self, mut level: usize) -> io::Result<()> {
        let merged_in_single_file = self.config.in_single_file;

        while self.sstable_directory_names[level].len() > self.config.max_per_level {
            // Choose first SStable from given level
            let main_sstable_base_path = self.config.parent_dir.join(self.sstable_directory_names[level].remove(0));
            let in_single_file = LSM::is_in_single_file(&main_sstable_base_path);
            let (main_min_key, main_max_key) = SSTable::get_key_range(main_sstable_base_path.to_owned(), in_single_file)?;

            // Find SStables with keys in similar range one level below
            let in_range_paths = LSM::find_similar_key_ranges(&self.sstable_directory_names, &self.config.parent_dir, &main_min_key, &main_max_key, level+1)?;
            let mut sstable_base_paths: Vec<_> = in_range_paths.clone()
                .into_iter()
                .map(|(_, path)| path.to_owned())
                .collect();

            // Put main SStable in vector and create vector of booleans indicating whether each SSTable is in a single file
            sstable_base_paths.push(main_sstable_base_path.to_owned());
            let mut sstable_single_file = Vec::new();
            for path in &sstable_base_paths {
                sstable_single_file.push(LSM::is_in_single_file(&path.to_owned().to_path_buf()));
            }

            // Make a name for new SSTable
            let merged_directory = PathBuf::from(LSM::get_directory_name(level+1, merged_in_single_file));
            let merged_base_path = self.config.parent_dir.join(merged_directory.clone());

            // Merge them all together
            SSTable::merge(sstable_base_paths, sstable_single_file, &merged_base_path.to_path_buf(), merged_in_single_file, self.config.summary_density, self.config.index_density, &mut self.compression_dictionary)?;

            // Extract indexes of SSTable that need to be removed
            let indexes_to_delete: Vec<_> = in_range_paths
                .iter()
                .map(|(index, _)| index)
                .collect();

            // Make new vector for sstable_directory_names one level below main that contains only SStables that weren't involved in compactions
            let mut kept_sstable_directories: Vec<PathBuf> = self.sstable_directory_names[level + 1]
                .iter()
                .enumerate()
                .filter(|&(index, _)| !indexes_to_delete.contains(&&index))
                .map(|(_, &ref elem)| elem.clone())
                .collect();

            // Replace this vector with existing in sstable_directory_names and append merged directory to it
            kept_sstable_directories.push(merged_directory);
            self.sstable_directory_names[level+1] = kept_sstable_directories;

            // Check for possibility of another compaction occurring
            level += 1;
            if level >= self.config.max_level {
                break;
            }
        }
        Ok(())
    }
  
    fn get_keys_from_mem_table(memory_table: &MemoryTable, min_key: Option<&[u8]>, max_key: Option<&[u8]>, searched_key: Option<&[u8]>, scan_type: ScanType) -> Vec<(Box<[u8]>, MemoryEntry)> {
        let mut entries = Vec::new();
        let mut iterator = memory_table.iterator();
        let mut flag = false;

        while let Some(entry) = iterator.next() {
            let curr_key = entry.0.clone();
            match scan_type {
                ScanType::RangeScan => {
                    let (min_key, max_key) = (min_key.unwrap(), max_key.unwrap());
                    if curr_key.as_ref() >= min_key && curr_key.as_ref()<= max_key {
                        entries.push(entry);
                    }
                    if curr_key.as_ref() > max_key {
                        break;
                    }
                }
                ScanType::PrefixScan => {
                    let searched_key = searched_key.unwrap();
                    if curr_key.starts_with(searched_key) {
                        flag = true;
                        entries.push(entry);
                    }
                    if !curr_key.starts_with(searched_key) && flag {
                        break;
                    }
                }
            }

        }

        entries
    }

    fn find_max_timestamp(entries: &Vec<(usize, &(Box<[u8]>, MemoryEntry))>) -> usize {
        let mut max_index = 0;
        let mut max_timestamp = entries[max_index].1.1.get_timestamp();
        for (index, element) in entries {
            let timestamp = element.1.get_timestamp();
            if timestamp > max_timestamp {
                max_index = *index;
                max_timestamp = timestamp;
            }
        }
        max_index
    }

    fn find_min_keys(entries: &Vec<(usize, &(Box<[u8]>, MemoryEntry))>) -> Vec<usize> {
        let mut min_key:Box<[u8]> = Box::new([255u8;255]);
        let mut min_indexes = vec![];
        for (index, element) in entries {
            if element.1.get_tombstone() {
                continue
            }
            let key = &element.0;
            let compare_result = min_key.cmp(key);
            if compare_result == Ordering::Equal {
                min_indexes.push(*index);
            }
            if compare_result == Ordering::Greater {
                min_indexes.clear();
                min_indexes.push(*index);
                min_key = key.clone();
            }

        }

        min_indexes
    }

    fn merge_scanned_entries(all_entries: Vec<Vec<(Box<[u8]>, MemoryEntry)>>) -> Vec<(Box<[u8]>, MemoryEntry)> {
        //ovde mergujem sve memtabele po istom principu kao sto se merguju sstabele samo sto se gleda i timestamp
        let mut scanned_entries = Vec::new();
        let mut positions: Vec<usize> = vec![0; all_entries.len()];

        let entries: Vec<_> = all_entries
            .iter()
            .zip(positions.iter())
            .enumerate()
            .map(|(index, (vector, position) )| (index, &vector[*position]))
            .collect();

        let min_key_indexes = LSM::find_min_keys(&entries);

        let min_entries: Vec<_> =  min_key_indexes
            .iter()
            .map(|index| entries[*index].clone())
            .collect();

        let _ = min_entries
            .iter()
            .for_each(|(index, _)| {
                positions[*index] += 1;
            });

        let max_index = LSM::find_max_timestamp(&min_entries);
        scanned_entries.push(entries[max_index].1.clone());

        scanned_entries
    }

    fn get_range_scan_parameters(&self, min_key: &[u8], max_key: &[u8]) -> io::Result<(Vec<(Box<[u8]>, MemoryEntry)>, usize, Vec<SSTable>, Vec<(u64)>)> {
        //dobavi prvo merged memory entries
        let memory_tables = self.mem_pool.get_all_tables();
        let mut entries: Vec<_> = memory_tables
            .iter()
            .map(|table| LSM::get_keys_from_mem_table(table, Some(min_key), Some(max_key), None, ScanType::RangeScan))
            .collect();
        let merged_memory_entries = LSM::merge_scanned_entries(entries);



        //dobavi sve sstabele koji imaju key u zadatom opsegu
        let mut sstable_paths = Vec::new();
        for level in 0..self.config.max_level {
            sstable_paths.extend(LSM::find_similar_key_ranges(&self.sstable_directory_names, &self.config.parent_dir, min_key, max_key, level)?);
        }
        let sstable_base_paths: Vec<_> = sstable_paths
            .into_iter()
            .map(|(_, path)| path.as_path())
            .collect();

        let in_single_files: Vec<bool> = sstable_base_paths
            .iter()
            .map(|path| LSM::is_in_single_file(&path.to_path_buf()))
            .collect();

        // otvori sve sstabele
        let mut sstables: Vec<_> = sstable_base_paths
            .iter()
            .zip(in_single_files.iter())
            .map(|(path, bool)| {
                match SSTable::open(path.to_path_buf(), *bool) {
                    Ok(table) => table,
                    Err(err) => panic!("{}", err),
                }
            })
            .collect();

        //dobavi sve offsete iz indexa i updateuj ih iz data dela
        let data_offsets: Vec<_> = sstable_base_paths
            .iter()
            .zip(in_single_files.iter())
            .map(|(path, in_single_file)| {
                SSTable::get_sstable_offset(path.to_path_buf(), *in_single_file, min_key, ScanType::RangeScan).unwrap_or_else(|err| panic!("{}", err))
            })
            .collect();

        let updated_offsets = SSTable::update_sstable_offsets(&mut sstables, in_single_files, data_offsets, min_key, ScanType::RangeScan)?;
        let memory_offset = 0;

        //vrati potrebne podatke za scan
        Ok((merged_memory_entries, memory_offset, sstables, updated_offsets))
    }

    fn get_prefix_scan_parameters(&self, prefix: &[u8]) -> io::Result<(Vec<(Box<[u8]>, MemoryEntry)>, usize, Vec<SSTable>, Vec<(u64)>)> {
        //dobavi prvo merged memory entries
        let memory_tables = self.mem_pool.get_all_tables();
        let mut entries: Vec<_> = memory_tables
            .iter()
            .map(|table| LSM::get_keys_from_mem_table(table, None, None, Some(prefix), ScanType::PrefixScan))
            .collect();
        let merged_memory_entries = LSM::merge_scanned_entries(entries);

        let sstable_directories: Vec<_> = self
            .sstable_directory_names
            .iter()
            .flat_map(|vec_path| vec_path.iter())
            .collect();

        let sstable_paths: Vec<_> = sstable_directories
            .iter()
            .map(|path| self.config.parent_dir.join(path))
            .collect();

        let in_single_files: Vec<bool> = sstable_paths
            .iter()
            .map(|path| LSM::is_in_single_file(path))
            .collect();

        // otvori sve sstabele
        let mut sstables: Vec<_> = sstable_paths
            .iter()
            .zip(in_single_files.iter())
            .map(|(path, bool)| {
                match SSTable::open(path.to_path_buf(), *bool) {
                    Ok(table) => table,
                    Err(err) => panic!("{}", err),
                }
            })
            .collect();

        //dobavi sve offsete iz indexa i updateuj ih iz data dela
        let data_offsets: Vec<_> = sstable_paths
            .iter()
            .zip(in_single_files.iter())
            .map(|(path, in_single_file)| {
                SSTable::get_sstable_offset(path.to_path_buf(), *in_single_file, prefix, ScanType::PrefixScan).unwrap_or_else(|err| panic!("{}", err))
            })
            .collect();

        let updated_offsets = SSTable::update_sstable_offsets(&mut sstables, in_single_files, data_offsets, prefix, ScanType::PrefixScan)?;
        let memory_offset = 0;

        //vrati potrebne podatke za scan
        Ok((merged_memory_entries, memory_offset, sstables, updated_offsets))
    }

    fn next(merged_memory_entries: Vec<(Box<[u8]>, MemoryEntry)>, memory_offset: usize, mut sstables: Vec<SSTable>, mut offsets: Vec<(u64)>, searched_key: &[u8], scan_type: ScanType) -> Option<((Box<[u8]>, MemoryEntry), Vec<u64>)> {
        //za offset memory entrija stoji 1 jer se pomeramo 1 po jedan u vektoru
        let memory_table_entry = if memory_offset < merged_memory_entries.len() {
            Option::from((merged_memory_entries[memory_offset].clone(), 1u64))
        } else {
            None
        };

        // procitati iz svih sstabela i spojiti to sa ovim jednim entrijem iz spojenih memtabela
        let mut option_entries: Vec<Option<_>> = sstables
            .iter_mut()
            .zip(offsets.iter())
            .map(|(sstable, offset)| sstable.get_entry_from_data_file(*offset, None, None))
            .collect();

        option_entries.push(memory_table_entry);

        //ako su svi none vrati nazad
        if option_entries.iter().all(Option::is_none) {
            return None;
        }

        //trebaju mi indexi od svih u vektoru da znam koje offsete da updateujem
        let enumerated_entries: Vec<_> = option_entries
            .iter()
            .enumerate()
            .filter(|(_, elem)| elem.is_some())
            .collect();

        //od svih koji su najmanji daj najnoviji timestamp
        let min_indexes = SSTable::find_min_keys(&enumerated_entries, false);

        let min_entries: Vec<_> =  min_indexes
            .iter()
            .map(|index| enumerated_entries[*index].clone())
            .collect();

        let max_index = SSTable::find_max_timestamp(&min_entries);

        //trebam da updateujem offsete
        offsets.push(memory_offset as u64);

        let _ = min_entries
            .iter()
            .for_each(|(index, element)| {
                offsets[*index] += element.as_ref().unwrap().1.clone();
            });

        //provera da li smo izasli iz opsega, sa donje strane smo se ogranicili ali sa gornje nismo
        let return_entry = min_entries[max_index].1.as_ref().unwrap().0.clone();
        match scan_type {
            ScanType::RangeScan => {
                if return_entry.0.as_ref() > searched_key {
                    return None;
                }
            }
            ScanType::PrefixScan => {
                if !return_entry.0.as_ref().starts_with(searched_key) {
                    return None;
                }
            }
        }


        return Some((return_entry, offsets))
    }

    pub(crate) enum ScanType {
        RangeScan,
        PrefixScan,
    }

    pub fn load_from_dir(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>>{
        let lru_cache = LRUCache::new(dbconfig.cache_max_size);
        let mem_pool = MemoryPool::load_from_dir(dbconfig)?; // todo luka nez sta god
        let wal = WriteAheadLog::new(dbconfig)?;
        Ok(LSM {
            config: LSMConfig::from(dbconfig),
            wal,
            mem_pool,
            lru_cache,
            compression_dictionary: match dbconfig.use_compression {
                true => Some(CompressionDictionary::load(dbconfig.compression_dictionary_path.as_str()).unwrap()),
                false => None
            },
            sstable_directory_names: Vec::with_capacity(dbconfig.lsm_max_level)
        })
    }
}