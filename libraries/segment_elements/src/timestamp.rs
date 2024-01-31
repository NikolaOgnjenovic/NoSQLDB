use std::time::{SystemTime, UNIX_EPOCH};

/// Wrapper enum for easier current time retrieval.
#[derive(Clone, Copy)]
pub enum TimeStamp {
    Now,
    Custom(u128),
}

impl TimeStamp {
    pub fn get_time(self) -> u128 {
        match self {
            TimeStamp::Now => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros(),
            TimeStamp::Custom(custom_time) => custom_time,
        }
    }
}
