//! MemTable manager for handling multiple tables

use std::sync::Arc;
use parking_lot::RwLock;
use tracing::info;

use hanshiro_core::{
    error::{Error, Result},
    metrics::Metrics,
    Event, EventId,
};

use super::table::MemTable;
use super::types::{MemTableConfig, MemTableManagerStats};

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