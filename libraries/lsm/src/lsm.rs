extern crate num_traits;

use num_traits::pow;
use std::cmp::Ordering;
use std::error::Error;
use std::fs::{create_dir_all, read_dir, remove_dir_all};
use std::io;
use std::io::BufRead;
use std::path::PathBuf;
use segment_elements::{MemoryEntry, TimeStamp};
use compression::CompressionDictionary;
use crate::sstable::SSTable;
use db_config::{ DBConfig, CompactionAlgorithmType };
use write_ahead_log::WriteAheadLog;
use lru_cache::LRUCache;
use crate::mem_pool::MemoryPool;
use crate::memtable::MemoryTable;

#[derive(Clone, Copy)]
pub enum ScanType {
    RangeScan,
    PrefixScan,
}

struct LSMConfig {
    // Base directory path where all other SSTable directories will be stored
    parent_dir: PathBuf,
    // Maximum number of levels
    max_level: usize,
    // Maximum number of SSTables per level
    max_per_level: usize,
    leveled_amplification: usize,
    // The compaction algorithm in use
    compaction_algorithm: CompactionAlgorithmType,
    in_single_file: bool,
    summary_density: usize,
    index_density: usize,
    compaction_enabled: bool,
    use_variable_encoding: bool,
}

impl LSMConfig {
    fn from(dbconfig: &DBConfig) -> Self {
        Self {
            parent_dir: PathBuf::from(&dbconfig.sstable_dir),
            max_level: dbconfig.lsm_max_level,
            max_per_level: dbconfig.lsm_max_per_level,
            leveled_amplification: dbconfig.lsm_leveled_amplification,
            compaction_algorithm: dbconfig.compaction_algorithm_type,
            compaction_enabled: dbconfig.compaction_enabled,
            use_variable_encoding: dbconfig.use_variable_encoding,
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

        create_dir_all(&dbconfig.sstable_dir)?;
        let dirs = read_dir(&dbconfig.sstable_dir)?
            .map(|dir_entry| dir_entry.unwrap().path())
            .filter(|entry| entry.is_dir()).collect::<Vec<PathBuf>>();

        for dir in dirs {
            let level = dir.file_name().unwrap().to_str().unwrap().split("_").collect::<Vec<&str>>()[1].parse::<usize>().unwrap();
            let path = PathBuf::from(dir.to_str().unwrap().split("/").last().unwrap());
            sstable_directory_names[level-1].push(path);
        }

        Ok(LSM {
            config: LSMConfig::from(dbconfig),
            wal,
            mem_pool,
            lru_cache,
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
                    Ok((min_key, max_key)) => {
                        let max_key_slice = &*max_key;
                        let min_key_slice = &*min_key;
                        let main_max_key_slice = &*main_max_key;
                        let main_min_key_slice = &*main_min_key;
                        let left = max_key >= Box::from(main_min_key);
                        let right =  min_key <= Box::from(main_max_key);
                        let condition = left && right;
                        condition
                    },
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
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Box<[u8]>>> {
        if let Some(memory_entry) = self.mem_pool.get(key) {
            self.lru_cache.insert(&key, Some(memory_entry.clone()));

            return if !memory_entry.get_tombstone() {
                Ok(Some(memory_entry.get_value()))
            } else {
                Ok(None)
            }
        }

        if let Some(memory_entry) = self.lru_cache.get(key) {
            self.lru_cache.insert(&key, Some(memory_entry.clone()));

            return if !memory_entry.get_tombstone() {
                Ok(Some(memory_entry.get_value()))
            } else {
                Ok(None)
            }
        }

        for level in &self.sstable_directory_names {
            for sstable_dir in level.iter().rev() {
                let (path, in_single_file) = self.get_sstable_path(sstable_dir);
                let mut sstable = SSTable::open(path, in_single_file)?;
                if let Some(memory_entry) = sstable.get(key, self.config.index_density, &mut self.compression_dictionary, self.config.use_variable_encoding) {
                    self.lru_cache.insert(&key, Some(memory_entry.clone()));

                    return if !memory_entry.get_tombstone() {
                        Ok(Some(memory_entry.get_value()))
                    } else {
                        Ok(None)
                    }
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
        if let Some(memory_table) = self.mem_pool.insert(key, value, time_stamp) {
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
        if let Some(memory_table) = self.mem_pool.delete(key, time_stamp) {
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
        let use_variable_encoding = self.config.use_variable_encoding;
        let memtable_wal_bytes_len = mem_table.wal_size();

        let mut sstable = SSTable::open(sstable_base_path.to_owned(), in_single_file)?;
        sstable.flush(mem_table, summary_density, index_density, Some(&mut self.lru_cache), &mut self.compression_dictionary, use_variable_encoding)?;

        self.wal.remove_logs_until(memtable_wal_bytes_len)?;

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

    fn remove_all_compacted(sstable_base_paths: Vec<PathBuf>) -> io::Result<()> {
        for dir in sstable_base_paths {
            remove_dir_all(dir)?;
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
        let use_variable_encoding = self.config.use_variable_encoding;
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
            SSTable::merge(sstable_base_paths.clone(), sstable_single_file, &merged_base_path, merged_in_single_file, self.config.summary_density, self.config.index_density, &mut self.compression_dictionary, use_variable_encoding)?;
            self.sstable_directory_names[level].clear();
            Self::remove_all_compacted(sstable_base_paths)?;
            self.sstable_directory_names[level+1].push(merged_directory);

            // Check for possibility of another compaction occurring
            level += 1;
            if level >= self.config.max_level - 1 {
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
        let use_variable_encoding = self.config.use_variable_encoding;
        let merged_in_single_file = self.config.in_single_file;

        while self.sstable_directory_names[level].len() > self.config.max_per_level * pow(self.config.leveled_amplification, level) {
            // Choose first SStable from given level
            let main_sstable_base_path = self.config.parent_dir.join(self.sstable_directory_names[level].remove(0));
            let in_single_file = LSM::is_in_single_file(&main_sstable_base_path);
            let (main_min_key, main_max_key) = SSTable::get_key_range(main_sstable_base_path.to_owned(), in_single_file)?;

            // Find SStables with keys in similar range one level below
            let in_range_paths = LSM::find_similar_key_ranges(&self.sstable_directory_names, &self.config.parent_dir, &main_min_key, &main_max_key, level+1)?;
            let mut sstable_base_paths: Vec<_> = in_range_paths.clone()
                .into_iter()
                .map(|(_, path)| self.config.parent_dir.join(path.to_owned()))
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
            SSTable::merge(sstable_base_paths.clone(), sstable_single_file, &merged_base_path.to_path_buf(), merged_in_single_file, self.config.summary_density, self.config.index_density, &mut self.compression_dictionary, use_variable_encoding)?;

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
            Self::remove_all_compacted(sstable_base_paths)?;
            self.sstable_directory_names[level+1] = kept_sstable_directories;

            // Check for possibility of another compaction occurring
            level += 1;
            if level >= self.config.max_level - 1 {
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

    fn find_max_timestamp(entries: &Vec<(usize, &(Box<[u8]>, MemoryEntry))>, default: usize) -> usize {
        let mut max_index = default;
        let mut max_timestamp = 0;
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

    fn merge_scanned_entries(all_entries: Vec<Vec<(Box<[u8]>, MemoryEntry)>>,min_key: Option<&[u8]>, max_key: Option<&[u8]>, prefix: Option<&[u8]>, scan_type: ScanType) -> Vec<(Box<[u8]>, MemoryEntry)> {
        //ovde mergujem sve memtabele po istom principu kao sto se merguju sstabele samo sto se gleda i timestamp
        let mut scanned_entries = Vec::new();
        let mut positions: Vec<usize> = vec![0; all_entries.len()];

        loop {
            // vektor booleana koji za svaki vektor kaze da li i dalje ima entrija u njemu
            let has_elements: Vec<_> = all_entries
                .iter()
                .zip(positions.iter())
                .map(|(vector, position)| vector.len() > *position)
                .collect();

            // ako ni u jednom vektoru nema entrija break
            if !has_elements.iter().any(|&x| x) {
                break;
            }

            //updateovanje offseta jer kad naidjem na tombstone true ja idem dok ne dodjem do tombstone false
            let mut update_positions = vec![0; all_entries.len()];

            //izvlacim entrije samo iz vektora koji imaju jos elemenata i zadrzavam stare indexe radi updateovanja korektnih offseta
            let entries: Vec<_> = all_entries
                .iter()
                .zip(positions.iter())
                .enumerate()
                .filter_map(|(index, (vector, position))| {
                    if has_elements[index] {
                        let mut return_entry = None;
                        let mut curr_position = *position;
                        while curr_position < vector.len() {
                            let entry = &vector[curr_position];
                            if entry.1.get_tombstone() {
                                update_positions[index] += 1;
                                curr_position += 1;
                                continue;
                            } else {
                                return_entry = Some((index, entry));
                                break
                            }
                        }
                        return_entry
                    } else {
                        None
                    }
                })
                .collect();

            //updateuj offsete kako treba
            for i in 0..positions.len() {
                positions[i] += update_positions[i];
            }

            //trazimo indexe najmanjih
            let min_key_indexes = LSM::find_min_keys(&entries);

            //zadrzavam samo entrije na min indexima
            let min_entries: Vec<_> = entries
                .iter()
                .filter(|(index, _)| min_key_indexes.contains(index))
                .cloned()
                .collect();

            //povecaj odgovarajuce offsete
            let _ = min_entries
                .iter()
                .for_each(|(index, _)| {
                    positions[*index] += 1;
                });

            //updateuj offsete entrija koji su obrisani jer ih find min nikada nece pronaci a i trebaju da se preskoce
            // let _ = entries
            //     .iter()
            //     .for_each(|(index, entry)| {
            //         if entry.1.get_tombstone() { positions[*index] += 1; }
            //     });

            //od najmanjih kljuceva nadji koji ima najveci timestamp
            let max_index = LSM::find_max_timestamp(&min_entries, entries.len()+1);

            //ovo znaci da niti jedan entry nije zadovoljio kriterijume(svi su obrisani)
            if max_index == entries.len() + 1 {
                continue;
            }
            let pushed_entry = entries
                .iter()
                .filter(|(index, _)| max_index == *index)
                .collect::<Vec<_>>()[0];


            match scan_type {
                ScanType::RangeScan => {
                    if let (Some(min_key), Some(max_key)) = (min_key, max_key) {
                        if pushed_entry.1.0.as_ref() < min_key || pushed_entry.1.0.as_ref() > max_key {
                            continue;
                        }
                    }
                }
                ScanType::PrefixScan => {
                    if let Some(prefix) = prefix {
                        if !pushed_entry.1.0.as_ref().starts_with(prefix) {
                            continue;
                        }
                    }
                }
            }

            scanned_entries.push(pushed_entry.1.clone());
        }
        scanned_entries
    }

    pub fn load_from_dir(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let (mem_pool, tables_to_be_flushed) =
            MemoryPool::load_from_dir(dbconfig)?;

        let mut new_lsm = LSM::new(&dbconfig)?;
        new_lsm.mem_pool = mem_pool;
        new_lsm.wal = WriteAheadLog::from_dir(&dbconfig)?;

        for table in tables_to_be_flushed {
            new_lsm.flush(table)?;
        }

        Ok(new_lsm)
    }

    pub(crate) fn iter(&self, min_key: Option<&[u8]>, max_key: Option<&[u8]>, prefix: Option<&[u8]>, scan_type: ScanType) -> io::Result<LSMIterator> {

        let prefix = if let Some(prefix) = prefix {
            Some(extract_prefix(prefix))
        } else {
            None
        };

        let entries = self.mem_pool.get_all_tables();
        let merged_memory_entries = LSM::merge_scanned_entries(entries, min_key, max_key, prefix, scan_type);

        let sstable_base_paths = if let (Some(min_key), Some(max_key)) = (min_key, max_key) {
            let mut sstable_paths = Vec::new();
            for level in 0..self.config.max_level {
                sstable_paths.extend(LSM::find_similar_key_ranges(&self.sstable_directory_names, &self.config.parent_dir, min_key, max_key, level)?);
            }
            let sstable_base_paths: Vec<_> = sstable_paths
                .into_iter()
                .map(|(_, path)| self.config.parent_dir.join(path))
                .collect();
            sstable_base_paths
        } else {
            let sstable_paths: Vec<_> = self
                .sstable_directory_names
                .iter()
                .flat_map(|vec_path| vec_path.iter())
                .collect();
            let sstable_base_paths: Vec<_> = sstable_paths
                .into_iter()
                .map(|path| self.config.parent_dir.join(path))
                .collect();
            sstable_base_paths
        };

        let in_single_files: Vec<bool> = sstable_base_paths
            .iter()
            .map(|path| LSM::is_in_single_file(&path.to_path_buf()))
            .collect();

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

        let value_to_remove: u64 = 1_000_000_000;

        let data_offsets = if let Some(min_key) = min_key {
            let data_offsets: Vec<_> = sstable_base_paths
                .iter()
                .zip(in_single_files.iter())
                .map(|(path, in_single_file)| {
                    SSTable::get_sstable_offset(path.to_path_buf(), *in_single_file, min_key, scan_type, None).unwrap_or_else(|err| panic!("{}", err))
                })
                .collect();
            data_offsets
        } else {
            let data_offsets: Vec<_> = sstable_base_paths
                .iter()
                .zip(in_single_files.iter())
                .map(|(path, in_single_file)| {
                    SSTable::get_sstable_offset(path.to_path_buf(), *in_single_file, prefix.unwrap(), scan_type, Some(value_to_remove)).unwrap_or_else(|err| panic!("{}", err))
                })
                .collect();
            data_offsets
        };

        let (mut sstables, data_offsets): (Vec<_>, Vec<_>) = if let Some(_) = prefix {
            let (mut sstables, data_offsets) = sstables
                .into_iter()
                .zip(data_offsets)
                .filter(|(_, offset)| *offset != value_to_remove)
                .map(|(table, offset)| (table, offset))
                .unzip();

            (sstables, data_offsets)
        } else {
            (sstables, data_offsets)
        };

        let updates_offsets = if let Some(min_key) = min_key {
            SSTable::update_sstable_offsets(&mut sstables, data_offsets, min_key, scan_type, self.config.use_variable_encoding)?
        } else {
            SSTable::update_sstable_offsets(&mut sstables, data_offsets, prefix.unwrap(), scan_type, self.config.use_variable_encoding)?
        };

        let memory_offset = 0;

        let upper_bound = if let Some(max_key) = max_key {
            max_key.to_vec().into_boxed_slice()
        } else {
            prefix.unwrap().to_vec().into_boxed_slice()
        };

        Ok(LSMIterator::new(merged_memory_entries, memory_offset, sstables, updates_offsets, scan_type, self.config.use_variable_encoding, upper_bound))
    }

    pub fn finalize(self) {
        self.wal.close();
        // when adding concurrent sstable flushes, join all threads here
    }
}

fn extract_prefix(slice: &[u8]) -> &[u8] {
    for (i, &value) in slice.iter().enumerate().rev() {
        if value != 0 {
            return &slice[..=i];
        }
    }
    &[0]
}

pub struct LSMIterator {
    memory_table_entries: Vec<(Box<[u8]>, MemoryEntry)>,
    memory_offset: usize,
    sstables: Vec<SSTable>,
    offsets: Vec<u64>,
    scan_type: ScanType,
    use_variable_encoding: bool,
    upper_bound: Box<[u8]>
}

impl LSMIterator {
    fn new(memory_table_entries: Vec<(Box<[u8]>, MemoryEntry)>, memory_offset: usize, sstables: Vec<SSTable>, offsets: Vec<u64>, scan_type: ScanType, use_variable_encoding: bool, upper_bound: Box<[u8]>) -> Self {
        LSMIterator {
            memory_table_entries,
            memory_offset,
            sstables,
            offsets,
            scan_type,
            use_variable_encoding,
            upper_bound
        }
    }
}

impl Iterator for LSMIterator {
    type Item = Option<(Box<[u8]>, MemoryEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut pushed = false;
        let mut copy_offsets = self.offsets.clone();
        while self.memory_offset < self.memory_table_entries.len() {
            let entry = self.memory_table_entries[self.memory_offset].clone();
            if entry.1.get_tombstone() {
                self.memory_offset += 1;
            } else {
                break;
            }
        }
        let memory_table_entry = if self.memory_offset < self.memory_table_entries.len() {
            pushed = true;
            copy_offsets.push(self.memory_offset as u64);
            Option::from((self.memory_table_entries[self.memory_offset].clone(), 1u64))
        } else {
            None
        };

        //ovo je za updateovanje offseta ako ne je neko tombstone true
        //let mut updated_offsets = vec![0; self.offsets.len()];

        // procitati iz svih sstabela i spojiti to sa ovim jednim entrijem iz spojenih memtabela
        let mut option_entries: Vec<Option<_>> = self.sstables
            .iter_mut()
            .zip(self.offsets.iter())
            .enumerate()
            .map(|(index,(sstable, offset))|{
                //new_offset je novi ukupan offset sa kojeg citamo
                let mut return_value;
                let mut new_offset = *offset;
                //added offset je koliko smo dodali na entry
                let mut added_offset = 0;
                loop {
                    if let Some((option_entry, length)) = sstable.get_entry_from_data_file(new_offset, None, None, self.use_variable_encoding) {
                        added_offset += length;
                        new_offset += length;
                        let _ = &*option_entry.0;
                        if option_entry.1.get_tombstone() {
                            //updated_offsets[index] += offset;
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

        // if copy_offsets.len() > self.offsets.len() {
        //     updated_offsets.push(0);
        // }
        // for i in 0..updated_offsets.len() {
        //     copy_offsets[i] += updated_offsets[i];
        // }

        option_entries.push(memory_table_entry);

        //ako su svi none vrati nazad
        if option_entries.iter().all(Option::is_none) {
            return None;
        }

        //trebaju mi indexi od svih u vektoru da znam koje offsete da updateujem
        let enumerated_entries: Vec<_> = option_entries
            .iter()
            .enumerate()
            .collect();

        let min_indexes = SSTable::find_min_keys(&enumerated_entries, false);

        let min_entries: Vec<_> =  min_indexes
            .iter()
            .map(|index| enumerated_entries[*index].clone())
            .collect();


        //trebam da updateujem offsete
        let _ = min_entries
            .iter()
            .for_each(|(index, element)| {
                copy_offsets[*index] += element.as_ref().unwrap().1.clone();
            });

        //updateuj offsete onih koji su obrisani
        // let _ = enumerated_entries
        //     .iter()
        //     .for_each(|(index, entry)| {
        //         if let Some(entry) = entry {
        //             if entry.0.1.get_tombstone() {
        //                 copy_offsets[*index] += entry.1;
        //             }
        //         }
        //     });

        //od svih koji su najmanji daj najnoviji timestamp
        let max_index = SSTable::find_max_timestamp(&min_entries);
        let return_entry = enumerated_entries[max_index].1.as_ref().unwrap().0.clone();
        let _ = &*return_entry.0;

        if pushed {
            self.memory_offset = copy_offsets.pop().unwrap() as usize;
            self.offsets = copy_offsets;
        }
        else {
            self.offsets = copy_offsets;
        }

        // znaci da su svi trenutni entriji logicki obrisani i rekurzivno pozivamo funk sa updateovanim parametrima
        if min_entries.is_empty() {
            return Some(None);
        }

        //provera da li smo izasli iz opsega, sa donje strane smo se ogranicili ali sa gornje nismo
        //takodje provera da li je entry koji smo dobili logicki obrisan jer ovde korisitm funk iz sstabele koja mora vratiti 0
        match self.scan_type {
            ScanType::RangeScan => {
                if (return_entry.0 > self.upper_bound) {
                    return None;
                }
            }
            ScanType::PrefixScan => {
                if !return_entry.0.starts_with(&self.upper_bound) {
                    return None;
                }
            }
        }

        Some(Some(return_entry))
    }
}