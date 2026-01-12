use std::sync::Arc;
use parking_lot::RwLock;
use tracing::info;

use hanshiro_core::{
    error::{Error, Result},
    metrics::Metrics,
    Event, EventId, HanshiroValue, KeyPrefix, CodeArtifact, IndexEntry,
};

use super::table::MemTable;
use super::types::{MemTableConfig, MemTableManagerStats};

pub struct MemTableManager {
    pub active: Arc<RwLock<Arc<MemTable>>>,
    pub immutable: Arc<RwLock<Vec<Arc<MemTable>>>>,
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

    /// Insert any HanshiroValue
    pub fn insert_value(&self, value: HanshiroValue) -> Result<u64> {
        for _ in 0..5 {
            let active = self.active.read();
            match active.insert_value(value.clone()) {
                Ok(seq) => return Ok(seq),
                Err(Error::MemTable { .. }) => {
                    drop(active);
                    self.rotate_memtable()?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::MemTable {
            message: "Insert failed after 5 rotation attempts".into(),
        })
    }

    /// Insert Event (backward compatible) - uses optimized fast path
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
        Err(Error::MemTable {
            message: "Insert failed after 5 rotation attempts".into(),
        })
    }

    /// Insert CodeArtifact
    pub fn insert_code(&self, artifact: CodeArtifact) -> Result<u64> {
        self.insert_value(HanshiroValue::Code(artifact))
    }

    /// Insert IndexEntry
    pub fn insert_index(&self, index: IndexEntry) -> Result<u64> {
        self.insert_value(HanshiroValue::Index(index))
    }

    /// Get value by ID
    pub fn get_value(&self, id: &EventId) -> Option<HanshiroValue> {
        if let Some(value) = self.active.read().get_value(id) {
            return Some(value);
        }

        for table in self.immutable.read().iter() {
            if let Some(value) = table.get_value(id) {
                return Some(value);
            }
        }

        None
    }

    /// Get Event by ID (backward compatible)
    pub fn get(&self, event_id: &EventId) -> Option<Event> {
        self.get_value(event_id)?.as_log().cloned()
    }

    /// Get CodeArtifact by ID
    pub fn get_code(&self, id: &EventId) -> Option<CodeArtifact> {
        self.get_value(id)?.as_code().cloned()
    }

    /// Scan by prefix across all memtables
    pub fn scan_prefix(&self, prefix: KeyPrefix) -> Vec<HanshiroValue> {
        let mut results = self.active.read().scan_prefix(prefix);
        for table in self.immutable.read().iter() {
            results.extend(table.scan_prefix(prefix));
        }
        results
    }

    /// Scan all logs
    pub fn scan_logs(&self) -> Vec<Event> {
        self.scan_prefix(KeyPrefix::Log)
            .into_iter()
            .filter_map(|v| v.as_log().cloned())
            .collect()
    }

    /// Scan all code artifacts
    pub fn scan_code(&self) -> Vec<CodeArtifact> {
        self.scan_prefix(KeyPrefix::Code)
            .into_iter()
            .filter_map(|v| v.as_code().cloned())
            .collect()
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
        let immutable_stats: Vec<_> = self
            .immutable
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