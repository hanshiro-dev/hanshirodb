//! Core MemTable implementation

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_skiplist::SkipMap;
use tracing::{debug, info};

use hanshiro_core::{
    error::{Error, Result},
    metrics::Metrics,
    Event, EventId,
};

use super::types::{MemTableConfig, MemTableEntry, MemTableKey, MemTableStats};

/// In-memory storage using concurrent skip list
pub struct MemTable {
    /// Skip list for ordered storage
    pub(crate) data: Arc<SkipMap<MemTableKey, MemTableEntry>>,
    /// Current size in bytes
    pub(crate) size_bytes: Arc<AtomicUsize>,
    /// Current number of entries
    pub(crate) entry_count: Arc<AtomicUsize>,
    /// Sequence counter
    pub(crate) sequence: Arc<AtomicU64>,
    /// Creation time
    pub(crate) created_at: Instant,
    /// Configuration
    pub(crate) config: MemTableConfig,
    /// Metrics
    metrics: Arc<Metrics>,
    /// Read-only flag (set when flushing)
    read_only: Arc<AtomicU64>,
}

impl MemTable {
    /// Create new MemTable
    pub fn new(config: MemTableConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            data: Arc::new(SkipMap::new()),
            size_bytes: Arc::new(AtomicUsize::new(0)),
            entry_count: Arc::new(AtomicUsize::new(0)),
            sequence: Arc::new(AtomicU64::new(0)),
            created_at: Instant::now(),
            config,
            metrics,
            read_only: Arc::new(AtomicU64::new(0)),
        }
    }
    
    /// Insert event into MemTable
    pub fn insert(&self, event: Event) -> Result<u64> {
        // Check if read-only
        if self.read_only.load(Ordering::Acquire) != 0 {
            return Err(Error::MemTable {
                message: "MemTable is read-only (being flushed)".to_string(),
            });
        }
        
        // Check size limits
        if self.should_flush() {
            return Err(Error::MemTable {
                message: "MemTable is full".to_string(),
            });
        }
        
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let key = MemTableKey::new(event.id);
        let entry_size = Self::estimate_entry_size(&event);
        
        let entry = MemTableEntry {
            event,
            sequence,
            timestamp: Instant::now(),
        };
        
        // Insert into skip list
        self.data.insert(key, entry);
        
        // Update counters
        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        
        debug!(
            "Inserted event {} into MemTable (size: {} bytes, entries: {})",
            sequence,
            self.size_bytes.load(Ordering::Relaxed),
            self.entry_count.load(Ordering::Relaxed)
        );
        
        Ok(sequence)
    }
    
    /// Get event by ID
    pub fn get(&self, event_id: &EventId) -> Option<Event> {
        // Since we don't know the exact timestamp, we need to scan
        // This is not optimal but acceptable for a MemTable
        for entry in self.data.iter() {
            if entry.key().event_id == *event_id {
                return Some(entry.value().event.clone());
            }
        }
        None
    }
    
    /// Scan events in time range
    pub fn scan(&self, start_ns: u64, end_ns: u64) -> Vec<Event> {
        let start_key = MemTableKey {
            timestamp_ns: start_ns,
            event_id: EventId::new(),
        };
        
        let end_key = MemTableKey {
            timestamp_ns: end_ns,
            event_id: EventId::new(),
        };
        
        self.data
            .range(start_key..=end_key)
            .map(|entry| entry.value().event.clone())
            .collect()
    }
    
    /// Check if MemTable should be flushed
    pub fn should_flush(&self) -> bool {
        let size = self.size_bytes.load(Ordering::Relaxed);
        let count = self.entry_count.load(Ordering::Relaxed);
        let age = self.created_at.elapsed();
        
        size >= self.config.max_size
            || count >= self.config.max_entries
            || age >= self.config.max_age
    }
    
    /// Mark MemTable as read-only (preparation for flush)
    pub fn set_read_only(&self) {
        self.read_only.store(1, Ordering::Release);
        info!(
            "MemTable marked as read-only (size: {} bytes, entries: {})",
            self.size_bytes.load(Ordering::Relaxed),
            self.entry_count.load(Ordering::Relaxed)
        );
    }
    
    /// Get all entries for flushing
    pub fn get_all_entries(&self) -> Vec<(MemTableKey, MemTableEntry)> {
        self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }
    
    /// Clear the MemTable (after successful flush)
    pub fn clear(&self) {
        self.data.clear();
        self.size_bytes.store(0, Ordering::Relaxed);
        self.entry_count.store(0, Ordering::Relaxed);
        info!("MemTable cleared after flush");
    }
    
    /// Get MemTable statistics
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
        }
    }
    
    /// Estimate size of an entry in bytes
    fn estimate_entry_size(event: &Event) -> usize {
        // Base size: key + entry overhead
        let mut size = std::mem::size_of::<MemTableKey>() + std::mem::size_of::<MemTableEntry>();
        
        // Add event data size
        size += event.raw_data.len();
        
        // Add metadata size (approximate - JSON string length)
        size += event.metadata_json.len();
        
        // Add vector size if present
        if let Some(ref vector) = event.vector {
            size += vector.data.len() * std::mem::size_of::<f32>();
        }
        
        size
    }
}