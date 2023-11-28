mod memtable;
mod record_iterator;
mod crc_error;

// todo make the memory pool available instead of MemoryTable
pub use memtable::MemoryTable;