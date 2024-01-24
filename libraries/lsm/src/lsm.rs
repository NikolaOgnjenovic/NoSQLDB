use std::io;
use std::path::{Path, PathBuf};
use segment_elements::{ TimeStamp, SegmentTrait };
use sstable::SSTable;
use db_config::{ DBConfig, CompactionAlgorithmType };


/// LSM(Log-Structured Merge Trees) struct for optimizing write-intensive workloads
struct LSM {
    // Base directory path where all other SSTable directories will be stored
    parent_dir: PathBuf,
    // Each vector represents one level containing directory names for SSTables
    sstable_directory_names: Vec<Vec<PathBuf>>,
    // Maximum number of levels
    number_of_levels: usize,
    // Maximum number of SSTables per level
    max_per_level: usize,
    // The compaction algorithm in use
    compaction_algorithm: CompactionAlgorithmType,

    compaction_enabled: bool,
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
    fn new(parent_dir: PathBuf, dbconfig: &DBConfig) -> Self {
        LSM {
            parent_dir,
            sstable_directory_names: Vec::with_capacity(dbconfig.lsm_max_level),
            number_of_levels: dbconfig.lsm_max_level,
            max_per_level: dbconfig.lsm_max_per_level,
            compaction_algorithm: dbconfig.compaction_algorithm_type,
            compaction_enabled: dbconfig.compaction_enabled,
        }
    }

    /// Creates directory name for new SSTable. Suffix is determined by the in_single_file parameter
    ///
    /// # Arguments
    ///
    /// * `level` - The level of new SSTable
    /// * in_single_file` - Boolean containing information wheteher or not is SSTable in one file
    ///
    /// # Returns
    ///
    /// Folder name of our new SSTable
    fn get_directory_name(&self, level: usize, in_single_file: bool) -> String {
        let prefix = "sstable_";
        let suffix = if in_single_file {
            "s"
        } else {
            "m"
        };
        prefix.to_string() + (level+1).to_string().as_str() + "_" + &TimeStamp::Now.get_time().to_string() + "_" + suffix
    }



    /// FLushes MemTable onto disk and starts the compaction process if necessary.
    ///
    /// # Arguments
    ///
    /// * inner_mem - The MemTable that needs to be flushed.
    /// * `db_config` - Configuration file.
    ///
    /// # Returns
    ///
    /// io::Result indicating success of flushing process
    fn flush<'a>(&mut self, inner_mem: &'a Box<dyn SegmentTrait + Send>, db_config: &DBConfig) -> io::Result<()> {
        let in_single_file = db_config.sstable_single_file;
        let summary_density = db_config.summary_density;
        let directory_name = self.get_directory_name(0, in_single_file);
        let sstable_base_path = self.parent_dir.join(directory_name.as_str());
        let mut sstable = SSTable::new(sstable_base_path.as_path(), inner_mem, in_single_file)?;
        sstable.flush(summary_density)?;
        self.sstable_directory_names[0].push(PathBuf::from(directory_name.as_str()));
        if self.compaction_enabled && self.sstable_directory_names[0].len() > self.max_per_level {
            if self.compaction_algorithm == CompactionAlgorithmType::SizeTiered {
                self.size_tiered_compaction(0, db_config)?;
            } else {
                self.leveled_compaction(0, db_config);
            }
        }
        Ok(())
    }

    /// Size-tiered compaction algorithm. Deletes all SSTables on current level and makes one bigger table located one level below
    /// This process can be propagated through levels
    ///
    /// # Arguments
    ///
    /// * level - The level where compactions started
    /// * `db_config` - Configuration file.
    ///
    /// # Returns
    ///
    /// io::Result indicating success of SSTable merging process
    fn size_tiered_compaction(&mut self, mut level: usize, db_config: &DBConfig) -> io::Result<()> {
        let merged_in_single_file = db_config.sstable_single_file;
        while self.sstable_directory_names[level].len() > self.max_per_level {
            let mut sstable_base_paths = Vec::new();
            let mut sstable_single_file = Vec::new();
            for path in &self.sstable_directory_names[level] {
                let base_path = self.parent_dir.join(path);
                sstable_base_paths.push(base_path);
                if path.to_str().unwrap().chars().last().unwrap() == 's' {
                    sstable_single_file.push(true);
                } else {
                    sstable_single_file.push(false);
                }
            }
            let sstable_base_paths:Vec<_> = sstable_base_paths.iter().map(|path_buf| path_buf.as_path()).collect();
            let merged_directory = PathBuf::from(self.get_directory_name(level+1, merged_in_single_file));
            let merged_base_path = self.parent_dir.join(merged_directory.clone());
            SSTable::merge_sstable_multiple(sstable_base_paths, sstable_single_file, merged_base_path.as_path(), merged_in_single_file, db_config)?;
            self.sstable_directory_names[level].clear();
            self.sstable_directory_names[level+1].push(merged_directory);

            level += 1;
            if level >= self.number_of_levels {
                break;
            }
        }
        Ok(())
    }

    fn leveled_compaction(&mut self, level: usize, db_config: &DBConfig) {
        //todo()!
    }


}