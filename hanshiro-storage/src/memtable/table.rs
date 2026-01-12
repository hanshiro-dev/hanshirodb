use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_skiplist::SkipMap;
use gxhash::GxBuildHasher;
use parking_lot::RwLock;
use std::collections::HashMap;
use tracing::{debug, info};

use hanshiro_core::{
    error::{Error, Result},
    metrics::Metrics,
    Event, EventId, HanshiroValue, KeyPrefix, CodeArtifact, IndexEntry,
};

use super::types::{MemTableConfig, MemTableEntry, MemTableKey, MemTableStats};

type FastHashMap<K, V> = HashMap<K, V, GxBuildHasher>;

pub struct MemTable {
    pub(crate) data: Arc<SkipMap<MemTableKey, MemTableEntry>>,
    pub(crate) id_index: RwLock<FastHashMap<EventId, MemTableKey>>,
    pub(crate) size_bytes: Arc<AtomicUsize>,
    pub(crate) entry_count: Arc<AtomicUsize>,
    pub(crate) log_count: Arc<AtomicUsize>,
    pub(crate) code_count: Arc<AtomicUsize>,
    pub(crate) index_count: Arc<AtomicUsize>,
    pub(crate) sequence: Arc<AtomicU64>,
    pub(crate) created_at: Instant,
    pub(crate) config: MemTableConfig,
    metrics: Arc<Metrics>,
    read_only: Arc<AtomicU64>,
}

impl MemTable {
    pub fn new(config: MemTableConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            data: Arc::new(SkipMap::new()),
            id_index: RwLock::new(HashMap::with_hasher(GxBuildHasher::default())),
            size_bytes: Arc::new(AtomicUsize::new(0)),
            entry_count: Arc::new(AtomicUsize::new(0)),
            log_count: Arc::new(AtomicUsize::new(0)),
            code_count: Arc::new(AtomicUsize::new(0)),
            index_count: Arc::new(AtomicUsize::new(0)),
            sequence: Arc::new(AtomicU64::new(0)),
            created_at: Instant::now(),
            config,
            metrics,
            read_only: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Insert any HanshiroValue
    pub fn insert_value(&self, value: HanshiroValue) -> Result<u64> {
        if self.read_only.load(Ordering::Acquire) != 0 {
            return Err(Error::MemTable {
                message: "MemTable is read-only (being flushed)".to_string(),
            });
        }

        if self.should_flush() {
            return Err(Error::MemTable {
                message: "MemTable is full".to_string(),
            });
        }

        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let prefix = value.prefix();
        let entry_id = self.get_value_id(&value);
        let key = MemTableKey::new(prefix, entry_id);
        let entry_size = Self::estimate_value_size(&value);

        let entry = MemTableEntry {
            value,
            sequence,
            timestamp: Instant::now(),
        };

        // Update type-specific counter
        match prefix {
            KeyPrefix::Log => { self.log_count.fetch_add(1, Ordering::Relaxed); }
            KeyPrefix::Code => { self.code_count.fetch_add(1, Ordering::Relaxed); }
            KeyPrefix::Index => { self.index_count.fetch_add(1, Ordering::Relaxed); }
        }

        self.data.insert(key.clone(), entry);
        self.id_index.write().insert(entry_id, key);

        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.entry_count.fetch_add(1, Ordering::Relaxed);

        Ok(sequence)
    }

    /// Fast path for inserting Events (logs) - avoids enum overhead
    #[inline]
    pub fn insert(&self, event: Event) -> Result<u64> {
        if self.read_only.load(Ordering::Acquire) != 0 {
            return Err(Error::MemTable {
                message: "MemTable is read-only (being flushed)".to_string(),
            });
        }

        if self.should_flush() {
            return Err(Error::MemTable {
                message: "MemTable is full".to_string(),
            });
        }

        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let event_id = event.id;
        let key = MemTableKey::new(KeyPrefix::Log, event_id);
        
        // Inline size estimation for events
        let entry_size = std::mem::size_of::<MemTableKey>() 
            + std::mem::size_of::<MemTableEntry>()
            + event.raw_data.len()
            + event.metadata_json.len()
            + event.vector.as_ref().map(|v| v.data.len() * 4).unwrap_or(0);

        let entry = MemTableEntry {
            value: HanshiroValue::Log(event),
            sequence,
            timestamp: Instant::now(),
        };

        self.data.insert(key.clone(), entry);
        self.id_index.write().insert(event_id, key);

        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        self.log_count.fetch_add(1, Ordering::Relaxed);

        Ok(sequence)
    }

    /// Insert CodeArtifact
    pub fn insert_code(&self, artifact: CodeArtifact) -> Result<u64> {
        self.insert_value(HanshiroValue::Code(artifact))
    }

    /// Insert IndexEntry
    pub fn insert_index(&self, index: IndexEntry) -> Result<u64> {
        self.insert_value(HanshiroValue::Index(index))
    }

    /// Get value by EventId
    pub fn get_value(&self, id: &EventId) -> Option<HanshiroValue> {
        let key = self.id_index.read().get(id).cloned()?;
        self.data.get(&key).map(|e| e.value().value.clone())
    }

    /// Get Event by EventId (backward compatible)
    pub fn get(&self, event_id: &EventId) -> Option<Event> {
        self.get_value(event_id)?.as_log().cloned()
    }

    /// Get CodeArtifact by ID
    pub fn get_code(&self, id: &EventId) -> Option<CodeArtifact> {
        self.get_value(id)?.as_code().cloned()
    }

    /// Scan entries by prefix (virtual table)
    pub fn scan_prefix(&self, prefix: KeyPrefix) -> Vec<HanshiroValue> {
        self.data
            .iter()
            .filter(|e| e.key().prefix == prefix.as_byte())
            .map(|e| e.value().value.clone())
            .collect()
    }

    /// Scan logs only
    pub fn scan_logs(&self) -> Vec<Event> {
        self.scan_prefix(KeyPrefix::Log)
            .into_iter()
            .filter_map(|v| v.as_log().cloned())
            .collect()
    }

    /// Scan code artifacts only
    pub fn scan_code(&self) -> Vec<CodeArtifact> {
        self.scan_prefix(KeyPrefix::Code)
            .into_iter()
            .filter_map(|v| v.as_code().cloned())
            .collect()
    }

    /// Scan by time range (logs only, backward compatible)
    pub fn scan(&self, start_ns: u64, end_ns: u64) -> Vec<Event> {
        let start_key = MemTableKey {
            prefix: KeyPrefix::Log.as_byte(),
            timestamp_ns: start_ns,
            id_hi: 0,
            id_lo: 0,
        };

        let end_key = MemTableKey {
            prefix: KeyPrefix::Log.as_byte(),
            timestamp_ns: end_ns,
            id_hi: u64::MAX,
            id_lo: u64::MAX,
        };

        self.data
            .range(start_key..=end_key)
            .filter_map(|entry| entry.value().value.as_log().cloned())
            .collect()
    }

    pub fn should_flush(&self) -> bool {
        let size = self.size_bytes.load(Ordering::Relaxed);
        let count = self.entry_count.load(Ordering::Relaxed);
        let age = self.created_at.elapsed();

        size >= self.config.max_size
            || count >= self.config.max_entries
            || age >= self.config.max_age
    }

    pub fn set_read_only(&self) {
        self.read_only.store(1, Ordering::Release);
        info!(
            "MemTable marked as read-only (size: {} bytes, entries: {})",
            self.size_bytes.load(Ordering::Relaxed),
            self.entry_count.load(Ordering::Relaxed)
        );
    }

    pub fn get_all_entries(&self) -> Vec<(MemTableKey, MemTableEntry)> {
        self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    pub fn clear(&self) {
        self.data.clear();
        self.id_index.write().clear();
        self.size_bytes.store(0, Ordering::Relaxed);
        self.entry_count.store(0, Ordering::Relaxed);
        self.log_count.store(0, Ordering::Relaxed);
        self.code_count.store(0, Ordering::Relaxed);
        self.index_count.store(0, Ordering::Relaxed);
        info!("MemTable cleared after flush");
    }

    pub fn stats(&self) -> MemTableStats {
        let now = Instant::now();

        let (oldest, newest) = if let Some(first) = self.data.front() {
            let oldest = Some(now - first.value().timestamp);
            let newest = if let Some(last) = self.data.back() {
                Some(now - last.value().timestamp)
            } else {
                oldest
            };
            (oldest, newest)
        } else {
            (None, None)
        };

        MemTableStats {
            entry_count: self.entry_count.load(Ordering::Relaxed),
            size_bytes: self.size_bytes.load(Ordering::Relaxed),
            oldest_entry_age: oldest,
            newest_entry_age: newest,
            log_count: self.log_count.load(Ordering::Relaxed),
            code_count: self.code_count.load(Ordering::Relaxed),
            index_count: self.index_count.load(Ordering::Relaxed),
        }
    }

    fn get_value_id(&self, value: &HanshiroValue) -> EventId {
        match value {
            HanshiroValue::Log(e) => e.id,
            HanshiroValue::Code(c) => c.id,
            HanshiroValue::Index(i) => {
                let hash = gxhash::gxhash64(format!("{}:{}", i.field, i.value).as_bytes(), 0);
                EventId { hi: hash, lo: 0 }
            }
        }
    }

    fn estimate_value_size(value: &HanshiroValue) -> usize {
        let base = std::mem::size_of::<MemTableKey>() + std::mem::size_of::<MemTableEntry>();

        match value {
            HanshiroValue::Log(event) => {
                base + event.raw_data.len()
                    + event.metadata_json.len()
                    + event.vector.as_ref().map(|v| v.data.len() * 4).unwrap_or(0)
            }
            HanshiroValue::Code(artifact) => {
                base + 32 // hash
                    + artifact.imports.iter().map(|s| s.len()).sum::<usize>()
                    + artifact.strings.iter().map(|s| s.len()).sum::<usize>()
                    + artifact.vector.as_ref().map(|v| v.data.len() * 4).unwrap_or(0)
                    + artifact.related_logs.len() * 16
            }
            HanshiroValue::Index(idx) => {
                base + idx.field.len() + idx.value.len() + idx.refs.len() * 16
            }
        }
    }
}
