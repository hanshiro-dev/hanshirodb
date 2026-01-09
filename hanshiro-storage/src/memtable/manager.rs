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

pub struct MemTableManager {
    pub active: Arc<RwLock<Arc<MemTable>>>, // Active MemTable for writes
    pub immutable: Arc<RwLock<Vec<Arc<MemTable>>>>, // Immutable MemTables awaiting flush
    config: MemTableConfig,
    metrics: Arc<Metrics>,
}

impl MemTableManager {
    pub fn new(config: MemTableConfig, metrics: Arc<Metrics>) -> Self {
        let active = Arc::new(MemTable::new(config.clone(), metrics.clone()));
        
        Self {
            active: Arc::new(RwLock::new(active)),
            immutable: Arc::new(RwLock::new(Vec::new())),
            config,
            metrics,
        }
    }
    
    pub fn insert(&self, event: Event) -> Result<u64> {
        for _ in 0..5 {
            let active = self.active.read();
            match active.insert(event.clone()) {
                Ok(seq) => return Ok(seq),
                Err(Error::MemTable { .. }) => {
                    drop(active);
                    self.rotate_memtable()?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::MemTable { message: "Insert failed after 3 rotation attempts".into() })
    }
    
    pub fn get(&self, event_id: &EventId) -> Option<Event> {
        if let Some(event) = self.active.read().get(event_id) {
            return Some(event);
        }
        
        for table in self.immutable.read().iter() {
            if let Some(event) = table.get(event_id) {
                return Some(event);
            }
        }
        
        None
    }
    
    fn rotate_memtable(&self) -> Result<()> {
        let mut active_lock = self.active.write();
        
        if !active_lock.should_flush() {
            return Ok(());
        }
        
        info!("Rotating MemTable");
        
        active_lock.set_read_only();
        let old_table = active_lock.clone();
        self.immutable.write().push(old_table);
        
        *active_lock = Arc::new(MemTable::new(self.config.clone(), self.metrics.clone()));
        
        self.metrics.record_flush();
        
        Ok(())
    }
    
    pub fn get_immutable_for_flush(&self) -> Option<Arc<MemTable>> {
        self.immutable.write().pop()
    }
    
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