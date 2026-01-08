//! Storage Engine
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Storage Engine                           │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  Write Path:                                                │
//! │  ┌─────────┐    ┌─────────┐    ┌──────────┐                 │
//! │  │  Event  │───>│   WAL   │───>│ MemTable │                 │
//! │  └─────────┘    └─────────┘    └────┬─────┘                 │
//! │                                     │ Flush                 │
//! │                                     ▼                       │
//! │                                ┌──────────┐                 │
//! │                                │ SSTable  │                 │
//! │                                └──────────┘                 │
//! │                                                             │
//! │  Read Path:                                                 │
//! │  ┌─────────┐    ┌──────────┐    ┌──────────┐                │
//! │  │  Query  │───>│ MemTable │───>│ SSTables │                │
//! │  └─────────┘    └──────────┘    └──────────┘                │
//! │                                                             │
//! │  Recovery Path:                                             │
//! │  ┌──────────┐    ┌─────────────┐    ┌──────────┐            │
//! │  │ Manifest │───>│ WAL Replay  │───>│ MemTable │            │
//! │  │checkpoint│    │ from seq N  │    │ rebuilt  │            │
//! │  └──────────┘    └─────────────┘    └──────────┘            │
//! │                                                             │
//! └─────────────────────────────────────────────────────────────┘

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
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
    compaction::{Compactor, CompactionConfig},
    fd::{FdConfig, FdMonitor, SSTablePool},
    manifest::{Manifest, SSTableManifestEntry},
    memtable::{MemTableConfig, MemTableManager},
    sstable::{SSTableConfig, SSTableInfo, SSTableReader, SSTableWriter},
    wal::{WalConfig, WriteAheadLog},
};

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub wal_config: WalConfig,
    pub memtable_config: MemTableConfig,
    pub sstable_config: SSTableConfig,
    pub compaction_config: CompactionConfig,
    pub fd_config: FdConfig,
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
            compaction_config: CompactionConfig::default(),
            fd_config: FdConfig::default(),
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(30), // More aggressive than usual for security logs
        }
    }
}

pub struct StorageEngine {
    config: StorageConfig,
    wal: Arc<WriteAheadLog>,
    memtable_manager: Arc<MemTableManager>,
    sstables: Arc<RwLock<Vec<SSTableInfo>>>,
    manifest: Arc<Mutex<Manifest>>,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Sender<bool>,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    sstable_pool: Arc<SSTablePool>,
    fd_monitor: Arc<FdMonitor>,
    compactor: Arc<Compactor>,
}

impl StorageEngine {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Initialize cached timestamp early
        crate::cached_time::init();
        
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let wal_dir = config.data_dir.join("wal");
        let sstable_dir = config.data_dir.join("sstables");
        tokio::fs::create_dir_all(&wal_dir).await?;
        tokio::fs::create_dir_all(&sstable_dir).await?;

        // Pre-create level directories to avoid mkdir latency during flushes
        for level in 0..config.compaction_config.max_levels {
            let level_dir = sstable_dir.join(format!("L{}", level));
            tokio::fs::create_dir_all(&level_dir).await?;
        }

        let manifest = Manifest::load_or_create(&config.data_dir)?;
        let wal_checkpoint = manifest.wal_checkpoint;
        
        let next_sstable_id = manifest
            .sstables
            .iter()
            .map(|s| s.id)
            .max()
            .unwrap_or(0)
            + 1;

        info!(
            "Opening database: wal_checkpoint={}, sstables={}",
            wal_checkpoint,
            manifest.sstables.len()
        );

        let wal = Arc::new(WriteAheadLog::new(&wal_dir, config.wal_config.clone()).await?);

        let metrics = Arc::new(Metrics::new());
        let memtable_manager = Arc::new(MemTableManager::new(
            config.memtable_config.clone(),
            metrics.clone(),
        ));

        let sstables: Vec<SSTableInfo> = manifest
            .sstables
            .iter()
            .map(|entry| SSTableInfo {
                path: entry.path.clone(),
                file_size: entry.size,
                entry_count: entry.entry_count,
                min_key: entry.min_key.clone(),
                max_key: entry.max_key.clone(),
                creation_time: entry.creation_time,
                level: entry.level,
            })
            .collect();

        // CRASH RECOVERY: Replay WAL entries not yet flushed to SSTable
        let replayed = Self::replay_wal(&wal, &memtable_manager, wal_checkpoint).await?;
        if replayed > 0 {
            info!(
                "Crash recovery: replayed {} WAL entries from sequence {}",
                replayed, wal_checkpoint
            );
        }

        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        let next_sstable_id = Arc::new(std::sync::atomic::AtomicU64::new(next_sstable_id));

        let sstable_pool = Arc::new(SSTablePool::new(config.fd_config.clone()));
        let fd_monitor = Arc::new(FdMonitor::new(config.fd_config.soft_limit_ratio));

        let compactor = Arc::new(Compactor::new(
            config.compaction_config.clone(),
            config.sstable_config.clone(),
            config.data_dir.clone(),
            Arc::clone(&next_sstable_id),
        ));

        let engine = Self {
            config,
            wal,
            memtable_manager,
            sstables: Arc::new(RwLock::new(sstables)),
            manifest: Arc::new(Mutex::new(manifest)),
            metrics,
            shutdown: shutdown_tx,
            next_sstable_id,
            sstable_pool,
            fd_monitor,
            compactor,
        };

        // Start background tasks
        engine.start_background_tasks();

        Ok(engine)
    }

    /// Replay WAL entries after checkpoint into MemTable
    async fn replay_wal(
        wal: &WriteAheadLog,
        memtable_manager: &MemTableManager,
        checkpoint: u64,
    ) -> Result<usize> {
        // Read all WAL entries after the checkpoint
        let entries = wal.read_from(checkpoint).await?;
        
        let mut replayed = 0;
        for entry in entries {
            // Skip entries at or before checkpoint (already flushed)
            if entry.sequence <= checkpoint && checkpoint > 0 {
                continue;
            }

            // Deserialize and insert into MemTable
            match rmp_serde::from_slice::<Event>(&entry.data) {
                Ok(event) => {
                    if let Err(e) = memtable_manager.insert(event) {
                        warn!("Failed to replay WAL entry {}: {:?}", entry.sequence, e);
                    } else {
                        replayed += 1;
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to deserialize WAL entry {}: {:?}",
                        entry.sequence, e
                    );
                }
            }
        }

        Ok(replayed)
    }

    /// Start background tasks
    fn start_background_tasks(&self) {
        // Flush task
        let memtable_manager = Arc::clone(&self.memtable_manager);
        let sstables = Arc::clone(&self.sstables);
        let manifest = Arc::clone(&self.manifest);
        let wal = Arc::clone(&self.wal);
        let config = self.config.clone();
        let next_id = Arc::clone(&self.next_sstable_id);
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut flush_interval = interval(config.flush_interval);
            flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        if let Err(e) = Self::flush_memtables(
                            &memtable_manager,
                            &sstables,
                            &manifest,
                            &wal,
                            &config,
                            &next_id,
                        ).await {
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
        let manifest = Arc::clone(&self.manifest);
        let compactor = Arc::clone(&self.compactor);
        let sstable_pool = Arc::clone(&self.sstable_pool);
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut compaction_interval = interval(config.compaction_interval);
            compaction_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = compaction_interval.tick() => {
                        if let Err(e) = Self::run_compaction(
                            &compactor,
                            &sstables,
                            &manifest,
                            &sstable_pool,
                            &config,
                        ).await {
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

    /// Flush MemTables to SSTables and update manifest
    async fn flush_memtables(
        memtable_manager: &Arc<MemTableManager>,
        sstables: &Arc<RwLock<Vec<SSTableInfo>>>,
        manifest: &Arc<Mutex<Manifest>>,
        wal: &Arc<WriteAheadLog>,
        config: &StorageConfig,
        next_id: &Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<()> {
        while let Some(memtable) = memtable_manager.get_immutable_for_flush() {
            let entries = memtable.get_all_entries();
            if entries.is_empty() {
                continue;
            }

            info!("Flushing MemTable with {} entries to SSTable", entries.len());

            // Track sequence range for this flush
            let mut min_sequence = u64::MAX;
            let mut max_sequence = 0u64;

            // Generate SSTable ID and path
            let sstable_id = next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let sstable_path = config
                .data_dir
                .join("sstables")
                .join(format!("{}_{}.sst", sstable_id, timestamp));

            let mut writer = SSTableWriter::new(&sstable_path, config.sstable_config.clone())?;

            // Write all entries
            for (key, entry) in &entries {
                let key_bytes = rmp_serde::to_vec(&key)
                    .map_err(|e| Error::Internal { message: e.to_string() })?;
                let value_bytes = rmp_serde::to_vec(&entry.event)
                    .map_err(|e| Error::Internal { message: e.to_string() })?;
                writer.add(&key_bytes, &value_bytes)?;

                min_sequence = min_sequence.min(entry.sequence);
                max_sequence = max_sequence.max(entry.sequence);
            }

            let info = writer.finish()?;

            // Create manifest entry
            let manifest_entry = SSTableManifestEntry {
                id: sstable_id,
                level: 0,
                path: sstable_path,
                size: info.file_size,
                entry_count: info.entry_count,
                min_key: info.min_key.to_vec(),
                max_key: info.max_key.to_vec(),
                min_sequence,
                max_sequence,
                creation_time: timestamp,
            };

            // Update manifest with new SSTable and checkpoint
            {
                let checkpoint_hash = wal.get_hash_at_sequence(max_sequence).await?;
                let mut m = manifest.lock();
                m.add_sstable(manifest_entry);
                m.update_checkpoint(max_sequence, checkpoint_hash);
                m.save(&config.data_dir)?;
            }

            // Add to in-memory SSTable list
            sstables.write().await.push(info);

            // Clear the flushed MemTable
            memtable.clear();

            info!(
                "Flushed MemTable: sequences {}..{}, checkpoint updated",
                min_sequence, max_sequence
            );

            // Optionally truncate old WAL files
            // wal.truncate(max_sequence).await?;
        }

        Ok(())
    }

    /// Run compaction if needed
    async fn run_compaction(
        compactor: &Arc<Compactor>,
        sstables: &Arc<RwLock<Vec<SSTableInfo>>>,
        manifest: &Arc<Mutex<Manifest>>,
        sstable_pool: &Arc<SSTablePool>,
        config: &StorageConfig,
    ) -> Result<()> {
        // Get current SSTable manifest entries
        let manifest_entries = manifest.lock().sstables.clone();

        // Check if compaction is needed
        let job = match compactor.pick_compaction(&manifest_entries) {
            Some(job) => job,
            None => return Ok(()),
        };

        // Execute compaction (blocking, could be moved to spawn_blocking for large files)
        let result = compactor.execute(job.clone())?;

        // Update manifest: remove old, add new
        {
            let mut m = manifest.lock();
            m.remove_sstables(&result.input_ids);
            if let Some(ref output) = result.output_sstable {
                m.add_sstable(output.clone());
            }
            m.save(&config.data_dir)?;
        }

        // Update in-memory SSTable list
        {
            let mut tables = sstables.write().await;
            tables.retain(|t| !result.input_ids.iter().any(|id| {
                // Match by path since SSTableInfo doesn't have id
                job.input_sstables.iter().any(|input| input.path == t.path)
            }));
            if let Some(ref output) = result.output_sstable {
                tables.push(SSTableInfo {
                    path: output.path.clone(),
                    file_size: output.size,
                    entry_count: output.entry_count,
                    min_key: output.min_key.clone(),
                    max_key: output.max_key.clone(),
                    creation_time: output.creation_time,
                    level: output.level,
                });
            }
        }

        // Remove compacted files from FD pool and disk
        for input in &job.input_sstables {
            sstable_pool.remove(&input.path);
        }
        let paths: Vec<_> = job.input_sstables.iter().map(|s| s.path.clone()).collect();
        compactor.cleanup_inputs(&paths)?;

        info!(
            "Compaction complete: {} files merged, {} bytes reclaimed",
            result.input_ids.len(),
            result.bytes_read.saturating_sub(result.bytes_written)
        );

        Ok(())
    }

    /// Force flush all MemTables
    pub async fn force_flush(&self) -> Result<()> {
        Self::flush_memtables(
            &self.memtable_manager,
            &self.sstables,
            &self.manifest,
            &self.wal,
            &self.config,
            &self.next_sstable_id,
        )
        .await
    }

    /// Verify WAL integrity
    pub async fn verify_wal_integrity(&self) -> Result<()> {
        self.wal.verify_integrity().await
    }

    /// Flush WAL to disk (for testing crash scenarios)
    pub async fn flush_wal(&self) -> Result<()> {
        self.wal.flush().await
    }

    /// Get current WAL checkpoint
    pub fn wal_checkpoint(&self) -> u64 {
        self.manifest.lock().wal_checkpoint
    }

    /// Get FD statistics
    pub fn fd_stats(&self) -> crate::fd::FdStats {
        self.sstable_pool.stats()
    }

    /// Check FD health
    pub fn fd_healthy(&self) -> bool {
        self.fd_monitor.check().healthy
    }
}

#[async_trait]
impl hanshiro_core::traits::StorageEngine for StorageEngine {
    async fn write(&self, event: Event) -> Result<EventId> {
        let event_id = event.id;

        // Write to WAL first (durability)
        self.wal.append(&event).await?;

        // Then write to MemTable (queryable)
        self.memtable_manager.insert(event)?;

        self.metrics.record_ingestion(1, 0);

        Ok(event_id)
    }

    async fn read(&self, id: EventId) -> Result<Option<Event>> {
        // Check MemTable first (hot data)
        if let Some(event) = self.memtable_manager.get(&id) {
            return Ok(Some(event));
        }

        // Then check SSTables (newest first) using pooled readers
        let sstables = self.sstables.read().await;
        for info in sstables.iter().rev() {
            let reader = self.sstable_pool.get(&info.path)?;
            let key = rmp_serde::to_vec(&id)
                .map_err(|e| Error::Internal { message: e.to_string() })?;

            if let Some(value) = reader.get(&key)? {
                let event: Event = rmp_serde::from_slice(&value)
                    .map_err(|e| Error::Internal { message: e.to_string() })?;
                return Ok(Some(event));
            }
        }

        Ok(None)
    }

    async fn write_batch(&self, events: Vec<Event>) -> Result<Vec<EventId>> {
        let ids: Vec<EventId> = events.iter().map(|e| e.id).collect();

        // Batch write to WAL
        self.wal.append_batch(&events).await?;

        // Insert into MemTable
        for event in events {
            self.memtable_manager.insert(event)?;
        }

        self.metrics.record_ingestion(ids.len() as u64, 0);

        Ok(ids)
    }

    async fn scan(&self, start: u64, end: u64) -> Result<Vec<Event>> {
        let mut all_events = Vec::new();

        // Convert seconds to nanoseconds for MemTable scan
        let start_ns = start.saturating_mul(1_000_000_000);
        let end_ns = end.saturating_mul(1_000_000_000);

        // Scan MemTable
        let memtable_events = self.memtable_manager.active.read().scan(start_ns, end_ns);
        all_events.extend(memtable_events);

        // Scan immutable MemTables
        for memtable in self.memtable_manager.immutable.read().iter() {
            let events = memtable.scan(start_ns, end_ns);
            all_events.extend(events);
        }

        // Scan SSTables
        let sstables = self.sstables.read().await;
        for info in &*sstables {
            let reader = SSTableReader::open(&info.path)?;

            for result in reader.iter() {
                let (_, value) = result?;
                let event: Event = rmp_serde::from_slice(&value)
                    .map_err(|e| Error::Internal { message: e.to_string() })?;

                let event_ts = event.timestamp.timestamp() as u64;
                if event_ts >= start && event_ts <= end {
                    all_events.push(event);
                }
            }
        }

        all_events.sort_by_key(|e| e.timestamp);

        Ok(all_events)
    }

    async fn stats(&self) -> Result<StorageStats> {
        let memtable_stats = self.memtable_manager.stats();
        let sstables = self.sstables.read().await;

        let total_bytes: u64 = sstables.iter().map(|info| info.file_size).sum();

        let total_events: u64 = sstables.iter().map(|info| info.entry_count).sum::<u64>()
            + memtable_stats.active.entry_count as u64;

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

        Self::run_compaction(
            &self.compactor,
            &self.sstables,
            &self.manifest,
            &self.sstable_pool,
            &self.config,
        ).await?;

        Ok(CompactionResult {
            files_compacted: 0,
            bytes_reclaimed: 0,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}
