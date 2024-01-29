use std::error::Error;
use std::io;
use std::path::PathBuf;
use segment_elements::{MemoryEntry, TimeStamp};
use crate::sstable::SSTable;
use db_config::{ DBConfig, CompactionAlgorithmType };
use write_ahead_log::WriteAheadLog;
use lru_cache::LRUCache;

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
            summary_density: dbconfig.summary_density
        }
    }
}

/// LSM(Log-Structured Merge Trees) struct for optimizing write-intensive workloads
pub(crate) struct LSM {
    // Each vector represents one level containing directory names for SSTables
    sstable_directory_names: Vec<Vec<PathBuf>>,
    wal: WriteAheadLog,
    mem_pool: MemoryPool,
    lru_cache: LRUCache,
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
    fn new(dbconfig: &DBConfig) -> Result<Self, Box<dyn Error>> {
        let lru_cache = LRUCache::new(dbconfig.cache_max_size);
        let mem_pool = MemoryPool::new(dbconfig)?;
        let wal = WriteAheadLog::new(dbconfig)?;
        Ok(LSM {
            config: LSMConfig::from(dbconfig),
            wal,
            mem_pool,
            lru_cache,
            sstable_directory_names: Vec::with_capacity(dbconfig.lsm_max_level)
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
    fn get_directory_name(level: usize, in_single_file: bool) -> PathBuf {
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
    /// * `main_min_key` - Min key from main SSTable
    /// * `main_max_key` - Max key from main SSTable
    /// * `level` - One level below our main SSTable that started compaction process
    ///
    /// # Returns
    ///
    /// A `Result` containing vector with tuple as elements.
    /// Each tuple contains index of a path in sstable_directory_names[level] as well as an actual path to that SSTable.
    /// The purpose of an index is to be able to quickly delete all the tables involved in compaction process later.
    fn find_similar_key_ranges(&self, main_min_key: &[u8], main_max_key: &[u8], level:usize) -> io::Result<Vec<(usize, &PathBuf)>> {
        let base_paths: Vec<_> = self.sstable_directory_names[level]
            .iter()
            .enumerate()
            .filter(|(index, path)| {
                let sstable_base_path = self.config.parent_dir.join(path);
                let in_single_file = LSM::is_in_single_file(path);
                match SSTable::get_key_range(sstable_base_path.as_path(), in_single_file) {
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
    fn get(&mut self, key: &[u8]) -> io::Result<Option<Box<[u8]>>> {
        // todo!() da li vratiti bajte ili memory entry?
        // todo!() da li deserijalizacija bajtova moze biti problem ako se radi o probabilistickim strukturama??
        if let Some(data) = self.mem_pool.get(key) {
            let (key, memory_entry) = MemoryEntry::deserialize(data.as_ref())?;
            self.lru_cache.insert(&key, Some(memory_entry));
            return Ok(Some(data));
        }
        if let Some(data) = self.lru_cache.get(key) {
            let (key, memory_entry) = MemoryEntry::deserialize(data.as_ref())?;
            self.lru_cache.insert(&key, Some(memory_entry));
            return Ok(Some(data));
        }
        for level in &self.sstable_directory_names {
            for sstable_dir in level.iter().rev() {
                let (path, in_single_file) = self.get_sstable_path(sstable_dir);
                let mut sstable = SSTable::open(path.as_path(), in_single_file)?;
                if let Some(data) = sstable.get(key) {
                    self.lru_cache.insert(key, Some(data.clone()));
                    return Ok(Some(data.serialize(key)));
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
    fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> io::Result<()> {
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
    fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> io::Result<()> {
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
        let directory_name = LSM::get_directory_name(0, in_single_file);
        let sstable_base_path = self.config.parent_dir.join(directory_name.as_path());

        let mut sstable = SSTable::open(sstable_base_path.as_path(), in_single_file)?;
        let flush_bytes = sstable.flush(mem_table, summary_density, Some(&mut self.lru_cache))?;
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
            let sstable_base_paths:Vec<_> = sstable_base_paths.iter().map(|path_buf| path_buf.as_path()).collect();
            let merged_directory = PathBuf::from(LSM::get_directory_name(level+1, merged_in_single_file));
            let merged_base_path = self.config.parent_dir.join(merged_directory.clone());

            // Merge them all together, push merged SSTable into sstable_directory_names and delete all SSTables involved in merging process
            SSTable::merge(sstable_base_paths, sstable_single_file, merged_base_path.as_path(), merged_in_single_file, self.config.summary_density)?;
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
            let (main_min_key, main_max_key) = SSTable::get_key_range(main_sstable_base_path.as_path(), in_single_file)?;

            // Find SStables with keys in similar range one level below
            let in_range_paths = self.find_similar_key_ranges(&main_min_key, &main_max_key, level+1)?;
            let mut sstable_base_paths: Vec<_> = in_range_paths.clone()
                .into_iter()
                .map(|(_, path)| path.as_path())
                .collect();

            // Put main SStable in vector and create vector of booleans indicating whether each SSTable is in a single file
            sstable_base_paths.push(main_sstable_base_path.as_path());
            let mut sstable_single_file = Vec::new();
            for path in &sstable_base_paths {
                sstable_single_file.push(LSM::is_in_single_file(&path.to_owned().to_path_buf()));
            }

            // Make a name for new SSTable
            let merged_directory = PathBuf::from(LSM::get_directory_name(level+1, merged_in_single_file));
            let merged_base_path = self.config.parent_dir.join(merged_directory.clone());

            // Merge them all together
            SSTable::merge(sstable_base_paths, sstable_single_file, merged_base_path.as_path(), merged_in_single_file, self.config.summary_density)?;

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
}