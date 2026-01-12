use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use hanshiro_core::{Event, EventId, HanshiroValue, KeyPrefix, StorageKey};

/// Entry stored in the MemTable
#[derive(Debug, Clone)]
pub struct MemTableEntry {
    pub value: HanshiroValue,
    pub sequence: u64,
    pub timestamp: Instant,
}

impl MemTableEntry {
    /// Get the EventId from the stored value
    pub fn id(&self) -> EventId {
        match &self.value {
            HanshiroValue::Log(e) => e.id,
            HanshiroValue::Code(c) => c.id,
            HanshiroValue::Index(i) => {
                // Index entries don't have a natural ID, generate from field+value
                let hash = gxhash::gxhash64(format!("{}:{}", i.field, i.value).as_bytes(), 0);
                EventId { hi: hash, lo: 0 }
            }
        }
    }

    /// Get as Event if this is a Log entry
    pub fn as_event(&self) -> Option<&Event> {
        self.value.as_log()
    }
}

/// Key for MemTable storage - wraps StorageKey for ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MemTableKey {
    pub prefix: u8,
    pub timestamp_ns: u64,
    pub id_hi: u64,
    pub id_lo: u64,
}

impl MemTableKey {
    pub fn new(prefix: KeyPrefix, id: EventId) -> Self {
        Self {
            prefix: prefix.as_byte(),
            timestamp_ns: crate::cached_time::now_ms() * 1_000_000, // ms to ns approximation
            id_hi: id.hi,
            id_lo: id.lo,
        }
    }

    pub fn from_storage_key(key: &StorageKey) -> Self {
        Self {
            prefix: key.prefix.as_byte(),
            timestamp_ns: key.timestamp_ns,
            id_hi: key.id.hi,
            id_lo: key.id.lo,
        }
    }

    pub fn to_storage_key(&self) -> StorageKey {
        StorageKey {
            prefix: KeyPrefix::from_byte(self.prefix).unwrap_or(KeyPrefix::Log),
            timestamp_ns: self.timestamp_ns,
            id: EventId { hi: self.id_hi, lo: self.id_lo },
        }
    }

    pub fn event_id(&self) -> EventId {
        EventId { hi: self.id_hi, lo: self.id_lo }
    }

    /// Create key for Event (backward compatibility)
    pub fn for_event(id: EventId) -> Self {
        Self::new(KeyPrefix::Log, id)
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemTableStats {
    pub entry_count: usize,
    pub size_bytes: usize,
    pub oldest_entry_age: Option<Duration>,
    pub newest_entry_age: Option<Duration>,
    pub log_count: usize,
    pub code_count: usize,
    pub index_count: usize,
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