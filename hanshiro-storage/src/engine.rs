//! # Storage Engine
//!
//! The main storage engine that coordinates WAL, MemTable, and SSTable components.
//!
//! ## Storage Engine Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Storage Engine                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │  Write Path:                                                 │
//! │  ┌─────────┐    ┌─────────┐    ┌──────────┐               │
//! │  │  Event  │───>│   WAL   │───>│ MemTable │               │
//! │  └─────────┘    └─────────┘    └────┬─────┘               │
//! │                                      │ Flush                │
//! │                                      ▼                      │
//! │                                ┌──────────┐                 │
//! │                                │ SSTable  │                 │
//! │                                └──────────┘                 │
//! │                                                              │
//! │  Read Path:                                                  │
//! │  ┌─────────┐    ┌──────────┐    ┌──────────┐              │
//! │  │  Query  │───>│ MemTable │───>│ SSTables │              │
//! │  └─────────┘    └──────────┘    └──────────┘              │
//! │       │                                                      │
//! │       └──────────> Bloom Filter Check First                 │
//! │                                                              │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use hanshiro_core::{
    error::{Error, Result},
    metrics::Metrics,
    traits::*,
    Event, EventId,
};

use crate::{
    wal::{WriteAheadLog, WalConfig},
    memtable::{MemTableManager, MemTableConfig},
    sstable::{SSTableWriter, SSTableReader, SSTableConfig, SSTableInfo},
};

/// Storage engine configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub wal_config: WalConfig,
    pub memtable_config: MemTableConfig,
    pub sstable_config: SSTableConfig,
    pub flush_interval: Duration,
    pub compaction_interval: Duration,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_config: WalConfig::default(),
            memtable_config: MemTableConfig::default(),
            sstable_config: SSTableConfig::default(),
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(300),
        }
    }
}

/// Main storage engine implementation
pub struct StorageEngine {
    config: StorageConfig,
    wal: Arc<WriteAheadLog>,
    memtable_manager: Arc<MemTableManager>,
    sstables: Arc<RwLock<Vec<SSTableInfo>>>,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl StorageEngine {
    /// Create new storage engine
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Create directories
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let wal_dir = config.data_dir.join("wal");
        let sstable_dir = config.data_dir.join("sstables");
        tokio::fs::create_dir_all(&wal_dir).await?;
        tokio::fs::create_dir_all(&sstable_dir).await?;
        
        // Initialize components
        let wal = Arc::new(WriteAheadLog::new(wal_dir, config.wal_config.clone()).await?);
        let metrics = Arc::new(Metrics::new());
        let memtable_manager = Arc::new(MemTableManager::new(
            config.memtable_config.clone(),
            metrics.clone(),
        ));
        
        // Load existing SSTables
        let sstables = Self::load_sstables(&sstable_dir).await?;
        
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        
        let engine = Self {
            config,
            wal,
            memtable_manager,
            sstables: Arc::new(RwLock::new(sstables)),
            metrics,
            shutdown: shutdown_tx,
        };
        
        // Start background tasks
        engine.start_background_tasks();
        
        Ok(engine)
    }
    
    /// Load existing SSTables from disk
    async fn load_sstables(sstable_dir: &PathBuf) -> Result<Vec<SSTableInfo>> {
        let mut sstables = Vec::new();
        let mut entries = tokio::fs::read_dir(sstable_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension() == Some(std::ffi::OsStr::new("sst")) {
                // In a real implementation, we'd parse SSTable metadata
                let info = SSTableInfo {
                    path: entry.path(),
                    file_size: entry.metadata().await?.len(),
                    entry_count: 0, // Would be stored in SSTable
                    min_key: bytes::Bytes::new(),
                    max_key: bytes::Bytes::new(),
                    creation_time: 0,
                    level: 0,
                };
                sstables.push(info);
            }
        }
        
        // Sort by creation time (oldest first)
        sstables.sort_by_key(|info| info.creation_time);
        
        info!("Loaded {} SSTables", sstables.len());
        Ok(sstables)
    }
    
    /// Start background tasks
    fn start_background_tasks(&self) {
        // Flush task
        let memtable_manager = Arc::clone(&self.memtable_manager);
        let sstables = Arc::clone(&self.sstables);
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        
        tokio::spawn(async move {
            let mut flush_interval = interval(config.flush_interval);
            flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            
            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        if let Err(e) = Self::flush_memtables(&memtable_manager, &sstables, &config).await {
                            error!("Flush error: {:?}", e);
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Shutting down flush task");
                        break;
                    }
                }
            }
        });
        
        // Compaction task
        let sstables = Arc::clone(&self.sstables);
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        
        tokio::spawn(async move {
            let mut compaction_interval = interval(config.compaction_interval);
            compaction_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            
            loop {
                tokio::select! {
                    _ = compaction_interval.tick() => {
                        if let Err(e) = Self::compact_sstables(&sstables, &config).await {
                            error!("Compaction error: {:?}", e);
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Shutting down compaction task");
                        break;
                    }
                }
            }
        });
    }
    
    /// Flush MemTables to SSTables
    async fn flush_memtables(
        memtable_manager: &Arc<MemTableManager>,
        sstables: &Arc<RwLock<Vec<SSTableInfo>>>,
        config: &StorageConfig,
    ) -> Result<()> {
        while let Some(memtable) = memtable_manager.get_immutable_for_flush() {
            info!("Flushing MemTable to SSTable");
            
            let entries = memtable.get_all_entries();
            if entries.is_empty() {
                continue;
            }
            
            // Create new SSTable
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let sstable_path = config.data_dir
                .join("sstables")
                .join(format!("{}.sst", timestamp));
            
            let mut writer = SSTableWriter::new(&sstable_path, config.sstable_config.clone())?;
            
            // Write all entries
            for (key, entry) in entries {
                let key_bytes = bincode::serialize(&key).unwrap();
                let value_bytes = bincode::serialize(&entry.event).unwrap();
                writer.add(&key_bytes, &value_bytes)?;
            }
            
            let info = writer.finish()?;
            
            // Add to SSTable list
            sstables.write().await.push(info);
            
            // Clear the flushed MemTable
            memtable.clear();
        }
        
        Ok(())
    }
    
    /// Compact SSTables
    async fn compact_sstables(
        sstables: &Arc<RwLock<Vec<SSTableInfo>>>,
        config: &StorageConfig,
    ) -> Result<()> {
        let tables = sstables.read().await;
        
        // Simple compaction: merge small SSTables
        if tables.len() < 4 {
            return Ok(());
        }
        
        info!("Starting compaction of {} SSTables", tables.len());
        
        // TODO: Implement proper compaction logic
        // For now, just log
        
        Ok(())
    }
    
    /// Force flush all MemTables
    pub async fn force_flush(&self) -> Result<()> {
        Self::flush_memtables(&self.memtable_manager, &self.sstables, &self.config).await
    }
    
    /// Verify WAL integrity
    pub async fn verify_wal_integrity(&self) -> Result<()> {
        self.wal.verify_integrity().await
    }
}

#[async_trait]
impl hanshiro_core::traits::StorageEngine for StorageEngine {
    async fn write(&self, event: Event) -> Result<EventId> {
        let event_id = event.id;
        
        // Write to WAL first
        self.wal.append(&event).await?;
        
        // Then write to MemTable
        self.memtable_manager.insert(event)?;
        
        // Update metrics
        self.metrics.record_ingestion(1, 0);
        
        Ok(event_id)
    }
    
    async fn read(&self, id: EventId) -> Result<Option<Event>> {
        // Check MemTable first
        if let Some(event) = self.memtable_manager.get(&id) {
            return Ok(Some(event));
        }
        
        // Then check SSTables
        let sstables = self.sstables.read().await;
        for info in sstables.iter().rev() {  // Check newest first
            let reader = SSTableReader::open(&info.path)?;
            let key = bincode::serialize(&id).unwrap();
            
            if let Some(value) = reader.get(&key)? {
                let event: Event = bincode::deserialize(&value)?;
                return Ok(Some(event));
            }
        }
        
        Ok(None)
    }
    
    async fn write_batch(&self, events: Vec<Event>) -> Result<Vec<EventId>> {
        let mut ids = Vec::with_capacity(events.len());
        
        for event in events {
            let id = self.write(event).await?;
            ids.push(id);
        }
        
        Ok(ids)
    }
    
    async fn scan(&self, start: u64, end: u64) -> Result<Vec<Event>> {
        let mut all_events = Vec::new();
        
        // Scan MemTable
        let memtable_events = self.memtable_manager.active.read().scan(start, end);
        all_events.extend(memtable_events);
        
        // Scan immutable MemTables
        for memtable in self.memtable_manager.immutable.read().iter() {
            let events = memtable.scan(start, end);
            all_events.extend(events);
        }
        
        // Scan SSTables
        let sstables = self.sstables.read().await;
        for info in &*sstables {
            let reader = SSTableReader::open(&info.path)?;
            
            // In a real implementation, we'd use the index to efficiently scan
            for result in reader.iter() {
                let (_, value) = result?;
                let event: Event = bincode::deserialize(&value)?;
                
                // Check if event is in time range
                let event_ts = event.timestamp.timestamp() as u64;
                if event_ts >= start && event_ts <= end {
                    all_events.push(event);
                }
            }
        }
        
        // Sort by timestamp
        all_events.sort_by_key(|e| e.timestamp);
        
        Ok(all_events)
    }
    
    async fn stats(&self) -> Result<StorageStats> {
        let memtable_stats = self.memtable_manager.stats();
        let sstables = self.sstables.read().await;
        
        let total_bytes: u64 = sstables.iter()
            .map(|info| info.file_size)
            .sum();
        
        let total_events: u64 = sstables.iter()
            .map(|info| info.entry_count)
            .sum::<u64>() + memtable_stats.active.entry_count as u64;
        
        Ok(StorageStats {
            total_events,
            total_bytes,
            wal_size: 0, // TODO: Get from WAL
            sstable_count: sstables.len() as u32,
            memtable_size: memtable_stats.active.size_bytes as u64,
            compaction_pending: sstables.len() > 10,
        })
    }
    
    async fn flush(&self) -> Result<()> {
        self.wal.flush().await?;
        self.force_flush().await?;
        Ok(())
    }
    
    async fn compact(&self) -> Result<CompactionResult> {
        let start = std::time::Instant::now();
        
        Self::compact_sstables(&self.sstables, &self.config).await?;
        
        Ok(CompactionResult {
            files_compacted: 0,
            bytes_reclaimed: 0,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // Signal shutdown to background tasks
        let _ = self.shutdown.send(true);
    }
}