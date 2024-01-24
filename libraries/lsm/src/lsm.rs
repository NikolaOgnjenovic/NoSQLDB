use std::io;
use std::path::{Path, PathBuf};
use segment_elements::{ TimeStamp, SegmentTrait };
use sstable::SSTable;
use db_config::{ DBConfig, CompactionAlgorithmType };

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
struct LSM {
    // Each vector represents one level containing directory names for SSTables
    sstable_directory_names: Vec<Vec<PathBuf>>,
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
    fn new(dbconfig: &DBConfig) -> Self {
        LSM {
            config: LSMConfig::from(dbconfig),
            sstable_directory_names: Vec::with_capacity(dbconfig.lsm_max_level)
        }
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
    fn flush<'a>(&mut self, inner_mem: &'a Box<dyn SegmentTrait + Send>) -> io::Result<()> {
        let in_single_file = self.config.in_single_file;
        let summary_density = self.config.summary_density;
        let directory_name = LSM::get_directory_name(0, in_single_file);
        let sstable_base_path = self.config.parent_dir.join(directory_name.as_str());

        let mut sstable = SSTable::new(sstable_base_path.as_path(), inner_mem, in_single_file)?;
        sstable.flush(summary_density)?;

        self.sstable_directory_names[0].push(PathBuf::from(directory_name.as_str()));
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
    /// * `db_config` - Configuration file.
    ///
    /// # Returns
    ///
    /// io::Result indicating success of SSTable merging process
    fn size_tiered_compaction(&mut self, mut level: usize) -> io::Result<()> {
        let merged_in_single_file = self.config.in_single_file;

        while self.sstable_directory_names[level].len() > self.config.max_per_level {

            let mut sstable_base_paths = Vec::new();
            let mut sstable_single_file = Vec::new();

            // Find all SSTables that need to be merged and create vector of booleans indicating whether or not is each SSTable in single file
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
            SSTable::merge_sstable_multiple(sstable_base_paths, sstable_single_file, merged_base_path.as_path(), merged_in_single_file, db_config)?;
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

            // Put main SStable in vector and create vector of booleans indicating whether or not is each SSTable in single file
            sstable_base_paths.push(main_sstable_base_path.as_path());
            let mut sstable_single_file = Vec::new();
            for path in &sstable_base_paths {
                sstable_single_file.push(LSM::is_in_single_file(&path.to_owned().to_path_buf()));
            }

            // Make a name for new SSTable
            let merged_directory = PathBuf::from(LSM::get_directory_name(level+1, merged_in_single_file));
            let merged_base_path = self.config.parent_dir.join(merged_directory.clone());

            // Merge them all together
            SSTable::merge_sstable_multiple(sstable_base_paths, sstable_single_file, merged_base_path.as_path(), merged_in_single_file, db_config)?;

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