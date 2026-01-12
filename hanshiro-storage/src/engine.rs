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

use std::collections::HashSet;
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
    Event, EventId, HanshiroValue, KeyPrefix, CodeArtifact, IndexEntry,
    serialization::serialize_value,
};
use hanshiro_index::{VectorCompactionConfig, VectorIndexCompactor, vidx_path_for_sstable};

use crate::{
    compaction::{Compactor, CompactionConfig, CompactionJob},
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
    pub vector_compaction_config: VectorCompactionConfig,
    pub fd_config: FdConfig,
    pub flush_interval: Duration,
    pub compaction_interval: Duration,
    /// Block cache size in bytes (0 = disabled, default 64MB)
    pub block_cache_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_config: WalConfig::default(),
            memtable_config: MemTableConfig::default(),
            sstable_config: SSTableConfig::default(),
            compaction_config: CompactionConfig::default(),
            vector_compaction_config: VectorCompactionConfig::default(),
            fd_config: FdConfig::default(),
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(30),
            block_cache_size: 64 * 1024 * 1024, // 64MB default
        }
    }
}

use crate::correlation::{CorrelationEngine, CorrelationConfig, Correlation};

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
    correlation_engine: Arc<CorrelationEngine>,
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

        // Create block cache if enabled
        let block_cache = if config.block_cache_size > 0 {
            Some(Arc::new(crate::cache::BlockCache::new(config.block_cache_size)))
        } else {
            None
        };

        let sstable_pool = Arc::new(SSTablePool::with_cache(config.fd_config.clone(), block_cache));
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
            correlation_engine: Arc::new(CorrelationEngine::new(CorrelationConfig::default())),
        };

        engine.start_background_tasks();

        Ok(engine)
    }

    async fn replay_wal(
        wal: &WriteAheadLog,
        memtable_manager: &MemTableManager,
        checkpoint: u64,
    ) -> Result<usize> {
        let entries = wal.read_from(checkpoint).await?;
        
        let mut replayed = 0;
        for entry in entries {
            // Skip entries at or before checkpoint (already flushed)
            if entry.sequence <= checkpoint && checkpoint > 0 {
                continue;
            }

            // Deserialize and insert into MemTable
            match hanshiro_core::deserialize_event(&entry.data) {
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

            // Write all entries (now using HanshiroValue)
            for (key, entry) in &entries {
                let key_bytes = rmp_serde::to_vec(&key)
                    .map_err(|e| Error::Internal { message: e.to_string() })?;
                let value_bytes = serialize_value(&entry.value)
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

        // Execute SSTable compaction
        let result = compactor.execute(job.clone())?;

        // Run vector index compaction (CPU-heavy, use spawn_blocking)
        let vector_result = Self::run_vector_compaction(&job, &result, config).await;
        if let Err(e) = &vector_result {
            warn!("Vector compaction failed (non-fatal): {:?}", e);
        }

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

        // Cleanup old .vidx files
        let vidx_paths: Vec<_> = paths.iter().map(|p| vidx_path_for_sstable(p)).collect();
        if let Err(e) = hanshiro_index::compaction::cleanup_vidx_files(&vidx_paths) {
            warn!("Failed to cleanup .vidx files: {:?}", e);
        }

        info!(
            "Compaction complete: {} files merged, {} bytes reclaimed",
            result.input_ids.len(),
            result.bytes_read.saturating_sub(result.bytes_written)
        );

        Ok(())
    }

    /// Run vector index compaction after SSTable compaction
    async fn run_vector_compaction(
        job: &CompactionJob,
        result: &crate::compaction::CompactionResult,
        config: &StorageConfig,
    ) -> Result<()> {
        let output_sstable = match &result.output_sstable {
            Some(s) => s,
            None => return Ok(()),
        };

        // Derive .vidx paths
        let input_vidx_paths: Vec<PathBuf> = job
            .input_sstables
            .iter()
            .map(|s| vidx_path_for_sstable(&s.path))
            .collect();

        // Check if any .vidx files exist
        let has_vidx = input_vidx_paths.iter().any(|p| p.exists());
        if !has_vidx {
            debug!("No .vidx files to compact");
            return Ok(());
        }

        let output_vidx_path = vidx_path_for_sstable(&output_sstable.path);

        // Build live_ids set from live_keys
        // Keys are MessagePack-serialized EventIds
        let live_ids: HashSet<u64> = result
            .live_keys
            .iter()
            .filter_map(|key_bytes| {
                // Deserialize EventId from key bytes
                rmp_serde::from_slice::<EventId>(key_bytes)
                    .ok()
                    .map(|id| id.hi ^ id.lo) // Use XOR of hi/lo as u64 identifier
            })
            .collect();

        let vector_config = config.vector_compaction_config.clone();

        // Run in spawn_blocking since graph building is CPU-intensive
        tokio::task::spawn_blocking(move || {
            let compactor = VectorIndexCompactor::new(vector_config);
            compactor.compact(&input_vidx_paths, &output_vidx_path, &live_ids)
        })
        .await
        .map_err(|e| Error::Internal {
            message: format!("Vector compaction task failed: {}", e),
        })??;

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

    // ========== HanshiroValue API ==========

    /// Write any HanshiroValue (Log, Code, or Index)
    /// Auto-correlation is done lazily, not on the write path
    pub async fn write_value(&self, value: HanshiroValue) -> Result<EventId> {
        let id = match &value {
            HanshiroValue::Log(e) => e.id,
            HanshiroValue::Code(c) => c.id,
            HanshiroValue::Index(_) => EventId::new(),
        };

        // For logs, write to WAL for durability
        if let HanshiroValue::Log(event) = &value {
            self.wal.append(event).await?;
        }

        // Write to MemTable
        self.memtable_manager.insert_value(value)?;
        self.metrics.record_ingestion(1, 0);

        Ok(id)
    }

    /// Write a CodeArtifact
    pub async fn write_code(&self, artifact: CodeArtifact) -> Result<EventId> {
        let id = artifact.id;
        self.memtable_manager.insert_code(artifact)?;
        self.metrics.record_ingestion(1, 0);
        Ok(id)
    }

    /// Write an IndexEntry (for graph relationships)
    pub async fn write_index(&self, index: IndexEntry) -> Result<EventId> {
        let id = EventId::new();
        self.memtable_manager.insert_index(index)?;
        Ok(id)
    }

    /// Run auto-correlation between all logs and code artifacts in memory.
    /// Call this after batch inserts, not on every write (for performance).
    pub async fn correlate_all(&self) -> Result<Vec<Correlation>> {
        let logs = self.memtable_manager.scan_logs();
        let code_artifacts = self.memtable_manager.scan_code();
        
        if logs.is_empty() || code_artifacts.is_empty() {
            return Ok(vec![]);
        }

        let mut all_correlations = Vec::new();
        
        for log in &logs {
            let correlations = self.correlation_engine.correlate_log_to_code(log, &code_artifacts);
            all_correlations.extend(correlations);
        }

        // Store correlations as index entries
        self.store_correlations(&all_correlations).await?;
        
        Ok(all_correlations)
    }

    /// Correlate a specific log against all code artifacts
    pub async fn correlate_log(&self, log: &Event) -> Result<Vec<Correlation>> {
        let code_artifacts = self.memtable_manager.scan_code();
        if code_artifacts.is_empty() {
            return Ok(vec![]);
        }
        
        let correlations = self.correlation_engine.correlate_log_to_code(log, &code_artifacts);
        self.store_correlations(&correlations).await?;
        Ok(correlations)
    }

    /// Correlate a specific code artifact against all logs
    pub async fn correlate_code(&self, artifact: &CodeArtifact) -> Result<Vec<Correlation>> {
        let logs = self.memtable_manager.scan_logs();
        if logs.is_empty() {
            return Ok(vec![]);
        }
        
        let correlations = self.correlation_engine.correlate_code_to_logs(artifact, &logs);
        self.store_correlations(&correlations).await?;
        Ok(correlations)
    }

    /// Store correlations as index entries
    async fn store_correlations(&self, correlations: &[Correlation]) -> Result<()> {
        for corr in correlations {
            let mut index = IndexEntry::new(
                format!("{:?}", corr.correlation_type),
                format!("{}", corr.target_id),
            );
            index.add_ref(corr.source_id);
            self.memtable_manager.insert_index(index)?;
        }
        Ok(())
    }

    /// Find entities correlated to the given ID
    pub async fn find_correlated(&self, id: EventId) -> Result<Vec<EventId>> {
        // Scan index entries for correlations
        let indexes = self.memtable_manager.scan_prefix(KeyPrefix::Index);
        let mut correlated = Vec::new();
        
        for value in indexes {
            if let Some(idx) = value.as_index() {
                // Check if this index references our ID
                if idx.refs.contains(&id) {
                    // The index value is the correlated entity ID
                    if let Ok(target_id) = idx.value.parse::<u64>() {
                        correlated.push(EventId { hi: target_id, lo: 0 });
                    }
                }
                // Or if the index is about our ID
                if idx.value.contains(&format!("{}", id)) {
                    correlated.extend(idx.refs.iter().cloned());
                }
            }
        }
        
        Ok(correlated)
    }

    /// Find similar entities by vector similarity
    pub async fn find_similar(&self, query_vector: &[f32], top_k: usize) -> Result<Vec<(EventId, f32)>> {
        // Collect all vectors from logs and code
        let mut candidates = Vec::new();
        
        for log in self.memtable_manager.scan_logs() {
            if let Some(vec) = &log.vector {
                candidates.push((log.id, vec.data.clone()));
            }
        }
        
        for code in self.memtable_manager.scan_code() {
            if let Some(vec) = &code.vector {
                candidates.push((code.id, vec.data.clone()));
            }
        }
        
        let results = self.correlation_engine.find_similar_by_vector(
            query_vector,
            &candidates,
            top_k,
        );
        
        Ok(results)
    }

    /// Read any value by ID
    pub async fn read_value(&self, id: EventId) -> Result<Option<HanshiroValue>> {
        // Check MemTable first
        if let Some(value) = self.memtable_manager.get_value(&id) {
            return Ok(Some(value));
        }

        // Check SSTables
        let sstables = self.sstables.read().await;
        for info in sstables.iter().rev() {
            let reader = self.sstable_pool.get(&info.path)?;
            let key = rmp_serde::to_vec(&id)
                .map_err(|e| Error::Internal { message: e.to_string() })?;

            if let Some(value_bytes) = reader.get(&key)? {
                let value = hanshiro_core::serialization::deserialize_value(&value_bytes)?;
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Read a CodeArtifact by ID
    pub async fn read_code(&self, id: EventId) -> Result<Option<CodeArtifact>> {
        Ok(self.read_value(id).await?.and_then(|v| v.as_code().cloned()))
    }

    /// Scan all values by prefix (virtual table)
    pub async fn scan_prefix(&self, prefix: KeyPrefix) -> Result<Vec<HanshiroValue>> {
        let mut results = self.memtable_manager.scan_prefix(prefix);

        // Also scan SSTables (would need prefix-aware iteration)
        // For now, just return memtable results
        // TODO: Add SSTable prefix scanning

        Ok(results)
    }

    /// Scan all logs
    pub async fn scan_logs(&self) -> Result<Vec<Event>> {
        Ok(self.memtable_manager.scan_logs())
    }

    /// Scan all code artifacts
    pub async fn scan_code(&self) -> Result<Vec<CodeArtifact>> {
        Ok(self.memtable_manager.scan_code())
    }

    /// Create a secondary index linking a field/value to entity IDs
    pub async fn create_index(&self, field: &str, value: &str, refs: Vec<EventId>) -> Result<()> {
        let mut index = IndexEntry::new(field, value);
        for id in refs {
            index.add_ref(id);
        }
        self.write_index(index).await?;
        Ok(())
    }

    /// Link a code artifact to related logs
    pub async fn link_code_to_logs(&self, code_id: EventId, log_ids: Vec<EventId>) -> Result<()> {
        // Get the code artifact
        if let Some(mut artifact) = self.read_code(code_id).await? {
            // Get hash before modifying
            let hash_hex = artifact.hash_hex();

            // Add log references
            for log_id in &log_ids {
                if !artifact.related_logs.contains(log_id) {
                    artifact.related_logs.push(*log_id);
                }
            }
            // Re-write the artifact
            self.write_code(artifact).await?;

            // Also create a reverse index: hash -> log_ids
            self.create_index("hash", &hash_hex, log_ids).await?;
        }
        Ok(())
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
                let event: Event = hanshiro_core::deserialize_event(&value)
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
                let event: Event = hanshiro_core::deserialize_event(&value)
                    .map_err(|e| Error::Internal { message: e.to_string() })?;

                if event.timestamp_ms >= start && event.timestamp_ms <= end {
                    all_events.push(event);
                }
            }
        }

        all_events.sort_by_key(|e| e.timestamp_ms);

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
