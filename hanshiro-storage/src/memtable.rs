//! # MemTable - In-Memory Storage
//!
//! The MemTable is an in-memory data structure that holds recent writes
//! before they are flushed to disk as SSTables. It uses a concurrent
//! skip list for fast, lock-free operations.
//!
//! ## MemTable Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        MemTable                              │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │  ┌─────────────┐    Insert    ┌─────────────────────────┐  │
//! │  │   Events    │─────────────>│     Skip List           │  │
//! │  └─────────────┘              │                         │  │
//! │                               │  Level 3: 8 ---------> 25 │  │
//! │                               │  Level 2: 3 -> 8 ----> 25 │  │
//! │                               │  Level 1: 3 -> 8 -> 19->25│  │
//! │                               │  Level 0: 3->5->8->19->25 │  │
//! │                               └─────────────────────────┘  │
//! │                                         │                   │
//! │                                         ▼                   │
//! │                               ┌─────────────────┐           │
//! │                               │   Size Limit    │           │
//! │                               │   Reached?      │           │
//! │                               └────────┬────────┘           │
//! │                                        │ Yes                │
//! │                                        ▼                    │
//! │                               ┌─────────────────┐           │
//! │                               │  Flush to       │           │
//! │                               │  SSTable        │           │
//! │                               └─────────────────┘           │
//! └─────────────────────────────────────────────────────────────┘
//!
//! ## Skip List Properties
//!
//! - O(log n) search, insert, delete
//! - Lock-free concurrent access
//! - Natural ordering by key
//! - Probabilistic balancing (no rebalancing needed)
//! ```

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use hanshiro_core::{
    error::{Error, Result},
    metrics::Metrics,
    Event, EventId,
};

/// MemTable entry
#[derive(Debug, Clone)]
pub struct MemTableEntry {
    pub event: Event,
    pub sequence: u64,
    pub timestamp: Instant,
}

/// MemTable key (combines sequence number and event ID for uniqueness)
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

/// MemTable statistics
#[derive(Debug, Clone, Default)]
pub struct MemTableStats {
    pub entry_count: usize,
    pub size_bytes: usize,
    pub oldest_entry_age: Option<Duration>,
    pub newest_entry_age: Option<Duration>,
}

/// In-memory storage using concurrent skip list
pub struct MemTable {
    /// Skip list for ordered storage
    data: Arc<SkipMap<MemTableKey, MemTableEntry>>,
    /// Current size in bytes
    size_bytes: Arc<AtomicUsize>,
    /// Current number of entries
    entry_count: Arc<AtomicUsize>,
    /// Sequence counter
    sequence: Arc<AtomicU64>,
    /// Creation time
    created_at: Instant,
    /// Configuration
    config: MemTableConfig,
    /// Metrics
    metrics: Arc<Metrics>,
    /// Read-only flag (set when flushing)
    read_only: Arc<AtomicU64>,
}

/// MemTable configuration
#[derive(Debug, Clone)]
pub struct MemTableConfig {
    /// Maximum size before flush (bytes)
    pub max_size: usize,
    /// Maximum number of entries
    pub max_entries: usize,
    /// Maximum age before flush
    pub max_age: Duration,
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
        
        // Add metadata size (approximate)
        size += event.metadata.len() * 64; // Rough estimate per metadata entry
        
        // Add vector size if present
        if let Some(ref vector) = event.vector {
            size += vector.data.len() * std::mem::size_of::<f32>();
        }
        
        size
    }
}

/// MemTable manager for handling multiple tables
pub struct MemTableManager {
    /// Active MemTable for writes
    pub active: Arc<RwLock<Arc<MemTable>>>,
    /// Immutable MemTables awaiting flush
    pub immutable: Arc<RwLock<Vec<Arc<MemTable>>>>,
    /// Configuration
    config: MemTableConfig,
    /// Metrics
    metrics: Arc<Metrics>,
}

impl MemTableManager {
    /// Create new MemTable manager
    pub fn new(config: MemTableConfig, metrics: Arc<Metrics>) -> Self {
        let active = Arc::new(MemTable::new(config.clone(), metrics.clone()));
        
        Self {
            active: Arc::new(RwLock::new(active)),
            immutable: Arc::new(RwLock::new(Vec::new())),
            config,
            metrics,
        }
    }
    
    /// Insert event
    pub fn insert(&self, event: Event) -> Result<u64> {
        loop {
            // Try to insert into active table
            let active = self.active.read();
            match active.insert(event.clone()) {
                Ok(seq) => return Ok(seq),
                Err(Error::MemTable { .. }) => {
                    // Table is full, need to rotate
                    drop(active);
                    self.rotate_memtable()?;
                    // Retry insert
                }
                Err(e) => return Err(e),
            }
        }
    }
    
    /// Get event by ID
    pub fn get(&self, event_id: &EventId) -> Option<Event> {
        // Check active table
        if let Some(event) = self.active.read().get(event_id) {
            return Some(event);
        }
        
        // Check immutable tables
        for table in self.immutable.read().iter() {
            if let Some(event) = table.get(event_id) {
                return Some(event);
            }
        }
        
        None
    }
    
    /// Rotate active MemTable to immutable
    fn rotate_memtable(&self) -> Result<()> {
        let mut active_lock = self.active.write();
        
        // Double-check that rotation is still needed
        if !active_lock.should_flush() {
            return Ok(());
        }
        
        info!("Rotating MemTable");
        
        // Mark current table as read-only
        active_lock.set_read_only();
        
        // Move to immutable list
        let old_table = active_lock.clone();
        self.immutable.write().push(old_table);
        
        // Create new active table
        *active_lock = Arc::new(MemTable::new(self.config.clone(), self.metrics.clone()));
        
        // Record metric
        self.metrics.record_flush();
        
        Ok(())
    }
    
    /// Get next immutable MemTable for flushing
    pub fn get_immutable_for_flush(&self) -> Option<Arc<MemTable>> {
        self.immutable.write().pop()
    }
    
    /// Get statistics for all MemTables
    pub fn stats(&self) -> MemTableManagerStats {
        let active_stats = self.active.read().stats();
        let immutable_stats: Vec<_> = self.immutable
            .read()
            .iter()
            .map(|table| table.stats())
            .collect();
            
        MemTableManagerStats {
            active: active_stats,
            immutable: immutable_stats,
        }
    }
}

/// MemTable manager statistics
#[derive(Debug)]
pub struct MemTableManagerStats {
    pub active: MemTableStats,
    pub immutable: Vec<MemTableStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hanshiro_core::types::*;
    
    fn create_test_event(id: u64) -> Event {
        let mut event = Event::new(
            EventType::NetworkConnection,
            EventSource {
                host: format!("host-{}", id),
                ip: None,
                collector: "test".to_string(),
                format: IngestionFormat::Raw,
            },
            format!("test-data-{}", id).into_bytes(),
        );
        event.add_metadata("id", id);
        event
    }
    
    #[test]
    fn test_memtable_insert_and_get() {
        let metrics = Arc::new(Metrics::new());
        let memtable = MemTable::new(MemTableConfig::default(), metrics);
        
        let event = create_test_event(1);
        let event_id = event.id;
        
        // Insert event
        let seq = memtable.insert(event.clone()).unwrap();
        assert_eq!(seq, 0);
        
        // Get event
        let retrieved = memtable.get(&event_id).unwrap();
        assert_eq!(retrieved.id, event_id);
    }
    
    #[test]
    fn test_memtable_scan() {
        let metrics = Arc::new(Metrics::new());
        let memtable = MemTable::new(MemTableConfig::default(), metrics);
        
        // Insert multiple events
        for i in 0..10 {
            let event = create_test_event(i);
            memtable.insert(event).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        
        // Scan all
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
            
        let events = memtable.scan(0, now_ns);
        assert_eq!(events.len(), 10);
    }
    
    #[test]
    fn test_memtable_size_limit() {
        let metrics = Arc::new(Metrics::new());
        let config = MemTableConfig {
            max_size: 1024, // 1KB limit for testing
            max_entries: 1000,
            max_age: Duration::from_secs(300),
        };
        let memtable = MemTable::new(config, metrics);
        
        // Insert until full
        let mut count = 0;
        loop {
            let event = create_test_event(count);
            match memtable.insert(event) {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        
        assert!(memtable.should_flush());
        assert!(count > 0);
    }
    
    #[test]
    fn test_memtable_manager_rotation() {
        let metrics = Arc::new(Metrics::new());
        let config = MemTableConfig {
            max_size: 1024, // Small size for testing
            max_entries: 10,
            max_age: Duration::from_secs(300),
        };
        let manager = MemTableManager::new(config, metrics);
        
        // Insert events to trigger rotation
        for i in 0..20 {
            let event = create_test_event(i);
            manager.insert(event).unwrap();
        }
        
        // Should have at least one immutable table
        assert!(manager.get_immutable_for_flush().is_some());
    }
}