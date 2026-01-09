use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use hanshiro_core::{Event, EventId};

#[derive(Debug, Clone)]
pub struct MemTableEntry {
    pub event: Event,
    pub sequence: u64,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MemTableKey {
    pub timestamp_ns: u64,
    pub event_id: EventId,
}

impl MemTableKey {
    pub fn new(event_id: EventId) -> Self {
        Self {
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            event_id,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemTableStats {
    pub entry_count: usize,
    pub size_bytes: usize,
    pub oldest_entry_age: Option<Duration>,
    pub newest_entry_age: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct MemTableConfig {
    pub max_size: usize, // Maximum size before flush (bytes)
    pub max_entries: usize, // Maximum number of entries
    pub max_age: Duration, // Maximum age before flush
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            max_size: 256 * 1024 * 1024,      // 256MB
            max_entries: 1_000_000,            // 1M entries
            max_age: Duration::from_secs(300), // 5 minutes
        }
    }
}

#[derive(Debug)]
pub struct MemTableManagerStats {
    pub active: MemTableStats,
    pub immutable: Vec<MemTableStats>,
}