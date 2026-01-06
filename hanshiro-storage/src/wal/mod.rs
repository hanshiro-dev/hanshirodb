//! Write-Ahead Log (WAL) with Merkle chain integrity and group commit.
//!
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Write Path (Group Commit)                    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Writer 1 ──┐                                                   │
//! │  Writer 2 ──┼──► Channel ──► Background Task ──► Batch fsync    │
//! │  Writer 3 ──┘                                                   │
//! │                                                                 │
//! │  append_batch() ──────────► Direct Write (bypasses group commit)│
//! └─────────────────────────────────────────────────────────────────┘
//!
//! File Format
//! - Header: 64 bytes (magic, version, timestamps, sequence range)
//! - Entries: Header (32B) + Merkle (96B) + Payload (variable)
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    WAL File Layout                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Header (64 bytes)                                          │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │ Magic Number (8 bytes): "HANSHIRO"                  │    │
//! │  │ Version (4 bytes)                                   │    │
//! │  │ Creation Time (8 bytes)                             │    │
//! │  │ First Sequence (8 bytes)                            │    │
//! │  │ Last Sequence (8 bytes)                             │    │
//! │  │ Entry Count (8 bytes)                               │    │
//! │  │ File Checksum (4 bytes)                             │    │
//! │  │ Reserved (16 bytes)                                 │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Entry 1                                                    │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │ Entry Header (32 bytes)                             │    │
//! │  │   - Length (4 bytes)                                │    │
//! │  │   - Sequence (8 bytes)                              │    │
//! │  │   - Timestamp (8 bytes)                             │    │
//! │  │   - Entry Type (1 byte)                             │    │
//! │  │   - Flags (1 byte)                                  │    │
//! │  │   - CRC32 (4 bytes)                                 │    │
//! │  │   - Reserved (6 bytes)                              │    │
//! │  ├─────────────────────────────────────────────────────┤    │
//! │  │ Merkle Info (96 bytes) - BLAKE3 hashes              │    │
//! │  │   - Previous Hash (32 bytes) ─┐                     │    │
//! │  │   - Current Hash (32 bytes)   ├─► Tamper-proof chain│    │
//! │  │   - Data Hash (32 bytes) ─────┘                     │    │
//! │  ├─────────────────────────────────────────────────────┤    │
//! │  │ Payload (Variable length)                           │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Entry 2...N (chained via Merkle hashes)                    │
//! └─────────────────────────────────────────────────────────────┘

mod file;
mod iterator;
mod types;

pub use iterator::WalEntryIterator;
pub use types::{EntryType, WalConfig, WalEntry};

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use hanshiro_core::{
    crypto::MerkleChain,
    error::{Error, Result},
    metrics::Metrics,
    Event,
};

use file::{create_file, entry_size, finalize_header, read_entry, recover_file, write_entry, WalFile, read_header_last_sequence};
use types::{WAL_HEADER_SIZE};


struct WriteRequest {
    entry: WalEntry,
    response: oneshot::Sender<Result<()>>,
}

pub struct WriteAheadLog {
    wal_dir: PathBuf,
    config: WalConfig,
    current_file: Arc<RwLock<WalFile>>,
    sequence: Arc<AtomicU64>,
    merkle_chain: Arc<RwLock<MerkleChain>>,
    metrics: Arc<Metrics>,
    write_tx: mpsc::Sender<WriteRequest>,
}

impl WriteAheadLog {
    /// Create or recover a WAL in the given directory.
    pub async fn new(wal_dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&wal_dir).await.map_err(|e| Error::Io {
            message: format!("Failed to create WAL directory: {:?}", wal_dir),
            source: e,
        })?;

        let (wal_file, sequence, merkle_chain) = Self::open_or_create(&wal_dir, &config).await?;
        let current_file = Arc::new(RwLock::new(wal_file));
        let metrics = Arc::new(Metrics::new());
        let (write_tx, write_rx) = mpsc::channel::<WriteRequest>(config.max_batch_size * 2);

        let bg_file = Arc::clone(&current_file);
        let bg_config = config.clone();
        let bg_metrics = Arc::clone(&metrics);
        let bg_dir = wal_dir.clone();
        tokio::spawn(async move {
            Self::group_commit_loop(write_rx, bg_file, bg_config, bg_metrics, bg_dir).await;
        });

        Ok(Self {
            wal_dir,
            config,
            current_file,
            sequence: Arc::new(AtomicU64::new(sequence)),
            merkle_chain: Arc::new(RwLock::new(merkle_chain)),
            metrics,
            write_tx,
        })
    }

    /// Append a single event (goes through group commit).
    pub async fn append(&self, event: &Event) -> Result<u64> {
        let entry = self.create_entry(event)?;
        let sequence = entry.sequence;

        let (tx, rx) = oneshot::channel();
        self.write_tx
            .send(WriteRequest { entry, response: tx })
            .await
            .map_err(|_| Error::WriteAheadLog {
                message: "WAL channel closed".to_string(),
                source: None,
            })?;

        rx.await.map_err(|_| Error::WriteAheadLog {
            message: "WAL response channel closed".to_string(),
            source: None,
        })??;

        Ok(sequence)
    }

    /// Append multiple events in a single batch (NOTE: bypasses group commit).
    pub async fn append_batch(&self, events: &[Event]) -> Result<Vec<u64>> {
        if events.is_empty() {
            return Ok(vec![]);
        }

        let entries: Vec<WalEntry> = events
            .iter()
            .map(|e| self.create_entry(e))
            .collect::<Result<_>>()?;
        let sequences: Vec<u64> = entries.iter().map(|e| e.sequence).collect();

        self.write_batch(&entries).await?;

        let total_bytes: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
        self.metrics.record_wal_write(total_bytes);

        Ok(sequences)
    }

    pub async fn flush(&self) -> Result<()> {
        let mut file = self.current_file.write();
        file.file.flush()?;
        file.file.get_ref().sync_all()?;
        Ok(())
    }

    pub async fn read_from(&self, start_sequence: u64) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();

        self.flush().await?;

        let current_path = self.current_file.read().path.clone();
        let mut wal_files = self.list_wal_files().await?;
        wal_files.sort_by_key(|f| f.0);

        for (_, path) in &wal_files {
            if *path == current_path {
                continue;
            }
            self.read_entries_from_file(path, start_sequence, &mut entries, &mut seen)?;
        }

        self.read_entries_from_file(&current_path, start_sequence, &mut entries, &mut seen)?;
        entries.sort_by_key(|e| e.sequence);

        Ok(entries)
    }

    pub async fn iter_entries(&self) -> Result<WalEntryIterator> {
        self.iter_entries_from(0).await
    }

    pub async fn iter_entries_from(&self, start_sequence: u64) -> Result<WalEntryIterator> {
        self.flush().await?;

        let current_path = self.current_file.read().path.clone();
        let mut wal_files = self.list_wal_files().await?;
        wal_files.sort_by_key(|f| f.0);

        // Filter files that might contain entries > start_sequence
        let paths: Vec<PathBuf> = wal_files
            .into_iter()
            .filter(|(_, path)| {
                if start_sequence == 0 || *path == current_path {
                    return true;
                }
                read_header_last_sequence(path)
                    .map(|last| last >= start_sequence)
                    .unwrap_or(true)
            })
            .map(|(_, p)| p)
            .collect();

        WalEntryIterator::new(paths, start_sequence)
    }

    pub async fn get_hash_at_sequence(&self, sequence: u64) -> Result<Option<String>> {
        for entry in self.iter_entries().await? {
            let entry = entry?;
            if entry.sequence == sequence {
                return Ok(Some(entry.merkle.hash));
            }
            if entry.sequence > sequence {
                break;
            }
        }
        Ok(None)
    }

    pub async fn verify_integrity(&self) -> Result<()> {
        self.verify_integrity_from(0, None).await
    }

    /// Verify integrity from a checkpoint (O(unflushed) instead of O(all)).
    pub async fn verify_integrity_from(
        &self,
        checkpoint_sequence: u64,
        checkpoint_hash: Option<String>,
    ) -> Result<()> {
        info!("Verifying WAL integrity from sequence {}", checkpoint_sequence);

        let mut count = 0u64;
        let mut prev_hash = checkpoint_hash;

        for entry in self.iter_entries_from(checkpoint_sequence).await? {
            let entry = entry?;
            if entry.merkle.prev_hash != prev_hash {
                return Err(Error::MerkleValidation {
                    expected: prev_hash.unwrap_or_default(),
                    actual: entry.merkle.prev_hash.unwrap_or_default(),
                });
            }
            prev_hash = Some(entry.merkle.hash);
            count += 1;
        }

        info!("WAL integrity verified: {} entries", count);
        Ok(())
    }

    /// Delete WAL files with sequences below `up_to_sequence`.
    pub async fn truncate(&self, up_to_sequence: u64) -> Result<()> {
        info!("Truncating WAL up to sequence {}", up_to_sequence);

        let current_path = self.current_file.read().path.clone();
        
        for (seq, path) in self.list_wal_files().await? {
            // IMPORTANT: Never delete the current active file
            if path == current_path {
                continue;
            }
            if seq < up_to_sequence {
                info!("Deleting WAL file: {:?}", path);
                tokio::fs::remove_file(path).await?;
            }
        }
        Ok(())
    }

    fn create_entry(&self, event: &Event) -> Result<WalEntry> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let data = rmp_serde::to_vec(event).map_err(|e| Error::WriteAheadLog {
            message: "Serialization failed".to_string(),
            source: Some(Box::new(e)),
        })?;

        let merkle = self.merkle_chain.write().add(&data);

        Ok(WalEntry {
            sequence,
            timestamp,
            entry_type: EntryType::Data,
            data: Bytes::from(data),
            merkle,
        })
    }

    async fn write_batch(&self, entries: &[WalEntry]) -> Result<()> {
        for entry in entries {
            let needs_rotation = {
                let f = self.current_file.read();
                f.size + entry_size(entry) as u64 > self.config.max_file_size
            };
            if needs_rotation {
                self.rotate().await?;
            }

            let mut f = self.current_file.write();
            write_entry(&mut f.file, entry)?;
            f.size += entry_size(entry) as u64;
            f.entry_count += 1;
            f.last_sequence = entry.sequence;
        }

        if self.config.sync_on_write {
            let mut f = self.current_file.write();
            f.file.flush()?;
            f.file.get_ref().sync_all()?;
        }
        Ok(())
    }

    async fn rotate(&self) -> Result<()> {
        rotate_sync(&self.current_file, &self.wal_dir, &self.config)
    }

    async fn group_commit_loop(
        mut rx: mpsc::Receiver<WriteRequest>,
        current_file: Arc<RwLock<WalFile>>,
        config: WalConfig,
        metrics: Arc<Metrics>,
        wal_dir: PathBuf,
    ) {
        let delay = std::time::Duration::from_micros(config.group_commit_delay_us);

        loop {
            let first = match rx.recv().await {
                Some(req) => req,
                None => break,
            };

            let mut batch = vec![first];
            let deadline = tokio::time::Instant::now() + delay;

            while batch.len() < config.max_batch_size {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(req)) => batch.push(req),
                    _ => break,
                }
            }

            let result = write_batch_sync(&current_file, &batch, &config, &wal_dir);
            let total_bytes: u64 = batch.iter().map(|r| r.entry.data.len() as u64).sum();
            let ok = result.is_ok();

            for req in batch {
                let _ = req.response.send(if ok {
                    Ok(())
                } else {
                    Err(Error::WriteAheadLog {
                        message: "Batch write failed".to_string(),
                        source: None,
                    })
                });
            }

            if ok {
                metrics.record_wal_write(total_bytes);
            }
        }
    }

    async fn open_or_create(wal_dir: &Path, config: &WalConfig) -> Result<(WalFile, u64, MerkleChain)> {
        let mut entries = tokio::fs::read_dir(wal_dir).await?;
        let mut wal_files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                wal_files.push(path);
            }
        }
        wal_files.sort();

        if let Some(latest) = wal_files.last() {
            recover_file(latest, config)
        } else {
            Ok((create_file(wal_dir, 0, config)?, 0, MerkleChain::new()))
        }
    }

    async fn list_wal_files(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut files = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.wal_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                if let Some(name) = path.file_stem() {
                    if let Ok(seq) = name.to_string_lossy().parse::<u64>() {
                        files.push((seq, path));
                    }
                }
            }
        }
        Ok(files)
    }

    fn read_entries_from_file(
        &self,
        path: &Path,
        start_sequence: u64,
        entries: &mut Vec<WalEntry>,
        seen: &mut HashSet<u64>,
    ) -> Result<()> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        while let Ok(entry) = read_entry(&mut reader) {
            if entry.sequence >= start_sequence && !seen.contains(&entry.sequence) {
                seen.insert(entry.sequence);
                entries.push(entry);
            }
        }
        Ok(())
    }
}


//!===============================
//! Synchronous helper routines
//!===============================

fn write_batch_sync(
    current_file: &Arc<RwLock<WalFile>>,
    batch: &[WriteRequest],
    config: &WalConfig,
    wal_dir: &Path,
) -> Result<()> {
    for req in batch {
        let needs_rotation = {
            let f = current_file.read();
            f.size + entry_size(&req.entry) as u64 > config.max_file_size
        };
        if needs_rotation {
            rotate_sync(current_file, wal_dir, config)?;
        }

        let mut f = current_file.write();
        write_entry(&mut f.file, &req.entry)?;
        f.size += entry_size(&req.entry) as u64;
        f.entry_count += 1;
        f.last_sequence = req.entry.sequence;
    }

    if config.sync_on_write {
        let mut f = current_file.write();
        f.file.flush()?;
        f.file.get_ref().sync_all()?;
    }
    Ok(())
}

fn rotate_sync(
    current_file: &Arc<RwLock<WalFile>>,
    wal_dir: &Path,
    config: &WalConfig,
) -> Result<()> {
    let mut current = current_file.write();
    finalize_header(&mut current)?;

    let new_seq = current.last_sequence + 1;
    *current = create_file(wal_dir, new_seq, config)?;

    info!("Rotated WAL file, new sequence: {}", new_seq);
    Ok(())
}
