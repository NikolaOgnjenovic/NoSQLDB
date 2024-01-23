use std::io;
use std::path::PathBuf;
use segment_elements::{ TimeStamp, SegmentTrait };
use sstable::SSTable;
use db_config::{ DBConfig, CompactionAlgorithmType };



struct LSM {
    parent_dir: PathBuf,
    sstable_directory_names: Vec<Vec<PathBuf>>,
    number_of_levels: usize,
    max_per_level: usize,
    compaction_algorithm: CompactionAlgorithmType,
    compaction_enabled: bool,
}

impl LSM {
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

    fn get_directory_name(&self, level: usize) -> String {
        "sstable_".to_string() + (level+1).to_string().as_str() + "_" + &TimeStamp::Now.get_time().to_string()
    }

    fn flush<'a>(&mut self, in_single_file: bool, summary_density: usize, inner_mem: &'a Box<dyn SegmentTrait + Send>) -> io::Result<()> {
        let directory_name = self.get_directory_name(0);
        let sstable_base_path = self.parent_dir.join(directory_name.as_str());
        let mut sstable = SSTable::new(sstable_base_path.as_path(), inner_mem, in_single_file)?;
        sstable.flush(summary_density)?;
        self.sstable_directory_names[0].push(PathBuf::from(directory_name.as_str()));
        if self.compaction_enabled && self.sstable_directory_names[0].len() > self.max_per_level {
            if self.compaction_algorithm == CompactionAlgorithmType::SizeTiered {
                self.size_tiered_compaction(0);
            } else {
                self.leveled_compaction(0);
            }
        }
        Ok(())
    }

    fn size_tiered_compaction(&mut self, level: usize) {
        while self.sstable_directory_names[level].len() > self.max_per_level {
            let num_of_tables = self.sstable_directory_names[level].len();
            //todo()!
        }
    }

    fn leveled_compaction(&mut self, level: usize) {}


}