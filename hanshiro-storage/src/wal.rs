//! Write-Ahead Log (WAL) with Group Commit
//!
//! High-performance WAL with Merkle chain integrity and group commit optimization.
//! Achieves ~292K ops/sec (batch) with full cryptographic chaining.
//!
//! ## Write Path Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         Write Path (Group Commit)                       │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │   Writer 1 ──┐                                                          │
//! │   Writer 2 ──┼──► Channel ──► Group Commit Task ──► Batch Write ──► Disk│
//! │   Writer 3 ──┘      │              │                    │               │
//! │                     │              │                    │               │
//! │              ┌──────▼──────┐ ┌─────▼─────┐      ┌──────▼──────┐        │
//! │              │ WriteRequest│ │  Collect  │      │ Single fsync│        │
//! │              │ + oneshot   │ │  within   │      │ for batch   │        │
//! │              │   channel   │ │  delay_us │      │             │        │
//! │              └─────────────┘ └───────────┘      └─────────────┘        │
//! │                                                                         │
//! │   append_batch() ──────────────────────────► Direct Write ──► Disk     │
//! │   (bypasses group commit for explicit batches)                         │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## WAL File Structure
//!
//! ```text
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
//! ```
//!
//! ## Performance (Release build, 512-byte events)
//!
//! | Mode                      | Throughput      | Latency    |
//! |---------------------------|-----------------|------------|
//! | Single + sync             | 361 ops/sec     | 2.77 ms    |
//! | Group commit + sync       | 1,223 ops/sec   | 817 μs     |
//! | Batch API + sync          | 22,834 ops/sec  | 44 μs      |
//! | Batch API + no sync       | 292,311 ops/sec | 3.4 μs     |
//! 

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use hanshiro_core::{
    crypto::{blake3_hash, crc32_checksum, MerkleChain, MerkleNode},
    error::{Error, Result},
    metrics::Metrics,
    Event,
};

const WAL_MAGIC: &[u8; 8] = b"HANSHIRO";
const WAL_VERSION: u32 = 1;
const WAL_HEADER_SIZE: usize = 64;
const ENTRY_HEADER_SIZE: usize = 32;
const MERKLE_INFO_SIZE: usize = 96;

// WAL entry types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    Data = 1,

    // This is a "control" entry marking a safe point in history.
    // It tells the system "everything before this point has been
    // successfully flushed to the main database files."
    Checkpoint = 2,

    // This entry signals that the log was deliberately cut off
    // or reset at this point.
    Truncate = 3,
}

impl TryFrom<u8> for EntryType {
    type Error = Error;
    
    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(EntryType::Data),
            2 => Ok(EntryType::Checkpoint),
            3 => Ok(EntryType::Truncate),
            _ => Err(Error::WriteAheadLog {
                message: format!("Invalid entry type: {}", value),
                source: None,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub sequence: u64,
    pub timestamp: u64,
    pub entry_type: EntryType,
    pub data: Bytes,
    pub merkle: MerkleNode,
}

/// Request sent to the group commit background task
struct WriteRequest {
    entry: WalEntry,
    response: oneshot::Sender<Result<()>>,
}

pub struct WriteAheadLog {
    current_file: Arc<RwLock<WalFile>>,
    wal_dir: PathBuf,
    sequence: Arc<AtomicU64>,
    merkle_chain: Arc<RwLock<MerkleChain>>,
    metrics: Arc<Metrics>,
    config: WalConfig,
    /// Channel for group commit - sends writes to background flusher
    write_tx: mpsc::Sender<WriteRequest>,
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub max_file_size: u64,
    pub sync_on_write: bool,
    pub compression: bool,
    pub buffer_size: usize,
    /// Max time to wait for batch to fill before flushing (microseconds)
    pub group_commit_delay_us: u64,
    /// Max entries per batch before forcing flush
    pub max_batch_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1024 * 1024 * 1024, // 1GB
            sync_on_write: true,
            compression: false,
            buffer_size: 64 * 1024, // 64KB
            group_commit_delay_us: 1000, // 1ms
            max_batch_size: 1000,
        }
    }
}

struct WalFile {
    path: PathBuf,
    file: BufWriter<File>,
    size: u64,
    entry_count: u64,
    first_sequence: u64,
    last_sequence: u64,
}

impl WriteAheadLog {

    pub async fn new(wal_dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        
        tokio::fs::create_dir_all(&wal_dir).await
            .map_err(|e| Error::Io { 
                message: format!("Failed to create WAL directory: {:?}", wal_dir),
                source: e,
            })?;
        
        let (current_file, sequence, merkle_chain) = Self::open_or_create_wal_file(&wal_dir, &config).await?;
        
        let current_file = Arc::new(RwLock::new(current_file));
        let metrics = Arc::new(Metrics::new());
        
        // Create channel for group commit
        let (write_tx, write_rx) = mpsc::channel::<WriteRequest>(config.max_batch_size * 2);
        
        // Spawn background group commit task
        let bg_file = Arc::clone(&current_file);
        let bg_config = config.clone();
        let bg_metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            Self::group_commit_loop(write_rx, bg_file, bg_config, bg_metrics).await;
        });
        
        Ok(Self {
            current_file,
            wal_dir,
            sequence: Arc::new(AtomicU64::new(sequence)),
            merkle_chain: Arc::new(RwLock::new(merkle_chain)),
            metrics,
            config,
            write_tx,
        })
    }
    
    /// Background task that batches writes and does group commit
    async fn group_commit_loop(
        mut rx: mpsc::Receiver<WriteRequest>,
        current_file: Arc<RwLock<WalFile>>,
        config: WalConfig,
        metrics: Arc<Metrics>,
    ) {
        let delay = std::time::Duration::from_micros(config.group_commit_delay_us);
        
        loop {
            // Wait for first write
            let first = match rx.recv().await {
                Some(req) => req,
                None => break, // Channel closed, shutdown
            };
            
            let mut batch = vec![first];
            
            // Collect more writes within delay window (non-blocking)
            let deadline = tokio::time::Instant::now() + delay;
            while batch.len() < config.max_batch_size {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(req)) => batch.push(req),
                    _ => break, // Timeout or channel closed
                }
            }
            
            // Write batch with single fsync
            let result = Self::write_batch_sync(&current_file, &batch, &config);
            let total_bytes: u64 = batch.iter().map(|r| r.entry.data.len() as u64).sum();
            
            // Notify all waiters
            let is_ok = result.is_ok();
            for req in batch {
                let _ = req.response.send(if is_ok {
                    Ok(())
                } else {
                    Err(Error::WriteAheadLog {
                        message: "Batch write failed".to_string(),
                        source: None,
                    })
                });
            }
            
            if is_ok {
                metrics.record_wal_write(total_bytes);
            }
        }
    }
    
    /// Synchronous batch write with single fsync
    fn write_batch_sync(
        current_file: &Arc<RwLock<WalFile>>,
        batch: &[WriteRequest],
        config: &WalConfig,
    ) -> Result<()> {
        let mut wal_file = current_file.write();
        
        for req in batch {
            Self::write_entry_to_file(&mut wal_file.file, &req.entry)?;
            wal_file.size += Self::entry_size(&req.entry) as u64;
            wal_file.entry_count += 1;
            wal_file.last_sequence = req.entry.sequence;
        }
        
        // Single sync for entire batch
        if config.sync_on_write {
            wal_file.file.flush()?;
            wal_file.file.get_ref().sync_all()?;
        }
        
        Ok(())
    }
    
    /// Append single event (uses group commit)
    pub async fn append(&self, event: &Event) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let data = bincode::serialize(event)
            .map_err(|e| Error::WriteAheadLog {
                message: "Failed to serialize event".to_string(),
                source: Some(Box::new(e)),
            })?;
        
        let merkle = {
            let mut chain = self.merkle_chain.write();
            chain.add(&data)
        };
        
        let entry = WalEntry {
            sequence,
            timestamp,
            entry_type: EntryType::Data,
            data: Bytes::from(data),
            merkle,
        };
        
        // Send to group commit task and wait for result
        let (tx, rx) = oneshot::channel();
        self.write_tx.send(WriteRequest { entry, response: tx }).await
            .map_err(|_| Error::WriteAheadLog {
                message: "WAL write channel closed".to_string(),
                source: None,
            })?;
        
        rx.await.map_err(|_| Error::WriteAheadLog {
            message: "WAL write response channel closed".to_string(),
            source: None,
        })??;
        
        Ok(sequence)
    }
    
    /// Batch append - bypasses group commit for explicit batches
    pub async fn append_batch(&self, events: &[Event]) -> Result<Vec<u64>> {
        if events.is_empty() {
            return Ok(vec![]);
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut entries = Vec::with_capacity(events.len());
        let mut sequences = Vec::with_capacity(events.len());

        for event in events {
            let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
            sequences.push(sequence);

            let data = bincode::serialize(event)
                .map_err(|e| Error::WriteAheadLog {
                    message: "Failed to serialize event".to_string(),
                    source: Some(Box::new(e)),
                })?;

            let merkle = {
                let mut chain = self.merkle_chain.write();
                chain.add(&data)
            };

            entries.push(WalEntry {
                sequence,
                timestamp,
                entry_type: EntryType::Data,
                data: Bytes::from(data),
                merkle,
            });
        }

        // Direct write for explicit batches
        self.write_entry_batch(&entries).await?;
        
        let total_bytes: u64 = entries.iter().map(|e| e.data.len() as u64).sum();
        self.metrics.record_wal_write(total_bytes);

        Ok(sequences)
    }

    /// Write batch directly (for explicit batch API)
    async fn write_entry_batch(&self, entries: &[WalEntry]) -> Result<()> {
        let mut wal_file = self.current_file.write();

        for entry in entries {
            Self::write_entry_to_file(&mut wal_file.file, entry)?;
            wal_file.size += Self::entry_size(entry) as u64;
            wal_file.entry_count += 1;
            wal_file.last_sequence = entry.sequence;
        }

        if self.config.sync_on_write {
            wal_file.file.flush()?;
            wal_file.file.get_ref().sync_all()?;
        }

        Ok(())
    }
    
    /// Calculate entry size
    fn entry_size(entry: &WalEntry) -> usize {
        ENTRY_HEADER_SIZE + MERKLE_INFO_SIZE + entry.data.len()
    }
    
    /// Write entry to file
    fn write_entry_to_file(writer: &mut impl Write, entry: &WalEntry) -> Result<()> {
        let mut header_buf = vec![0u8; ENTRY_HEADER_SIZE];
        let mut cursor = std::io::Cursor::new(&mut header_buf);
        
        // Write entry header
        cursor.write_u32::<LittleEndian>(entry.data.len() as u32)?;
        cursor.write_u64::<LittleEndian>(entry.sequence)?;
        cursor.write_u64::<LittleEndian>(entry.timestamp)?;
        cursor.write_u8(entry.entry_type as u8)?;
        cursor.write_u8(0)?; // Flags
        
        // Calculate and write CRC32
        let crc = crc32_checksum(&entry.data);
        cursor.write_u32::<LittleEndian>(crc)?;
        cursor.write_all(&[0u8; 6])?; // Reserved bytes
        
        // Write header
        writer.write_all(&header_buf)?;
        
        // Write Merkle info (hash strings are hex encoded, need to decode back to 32 bytes)
        let prev_hash_bytes = entry.merkle.prev_hash.as_ref()
            .map(|h| hex::decode(h).unwrap())
            .unwrap_or_else(|| vec![0u8; 32]);
        writer.write_all(&prev_hash_bytes)?;
        
        let hash_bytes = hex::decode(&entry.merkle.hash).unwrap();
        writer.write_all(&hash_bytes)?;
        
        let data_hash_bytes = hex::decode(&entry.merkle.data_hash).unwrap();
        writer.write_all(&data_hash_bytes)?;
        
        // Write payload
        writer.write_all(&entry.data)?;
        
        Ok(())
    }
    
    /// Rotate to a new WAL file
    async fn rotate_file(&self) -> Result<()> {
        info!("Rotating WAL file");
        
        let new_sequence = self.sequence.load(Ordering::SeqCst);
        let new_file = Self::create_wal_file(&self.wal_dir, new_sequence, &self.config)?;
        
        // Swap the file
        {
            let mut current = self.current_file.write();
            current.file.flush()?;
            *current = new_file;
        }
        
        Ok(())
    }
    
    /// Open existing or create new WAL file
    async fn open_or_create_wal_file(
        wal_dir: &Path,
        config: &WalConfig,
    ) -> Result<(WalFile, u64, MerkleChain)> {
        // List existing WAL files
        let mut entries = tokio::fs::read_dir(wal_dir).await?;
        let mut wal_files = Vec::new();
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                wal_files.push(path);
            }
        }
        
        // Sort by name (which includes sequence number)
        wal_files.sort();
        
        if let Some(latest) = wal_files.last() {
            // Open and recover from latest file
            Self::recover_from_file(latest, config)
        } else {
            // Create new file
            let wal_file = Self::create_wal_file(wal_dir, 0, config)?;
            Ok((wal_file, 0, MerkleChain::new()))
        }
    }
    
    /// Create new WAL file
    fn create_wal_file(wal_dir: &Path, sequence: u64, config: &WalConfig) -> Result<WalFile> {
        let filename = format!("{:020}.wal", sequence);
        let path = wal_dir.join(&filename);
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;
            
        let mut writer = BufWriter::with_capacity(config.buffer_size, file);
        
        // Write header
        writer.write_all(WAL_MAGIC)?;
        writer.write_u32::<LittleEndian>(WAL_VERSION)?;
        writer.write_u64::<LittleEndian>(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        )?;
        writer.write_u64::<LittleEndian>(sequence)?; // First sequence
        writer.write_u64::<LittleEndian>(sequence)?; // Last sequence
        writer.write_u64::<LittleEndian>(0)?; // Entry count
        writer.write_u32::<LittleEndian>(0)?; // File checksum
        writer.write_all(&[0u8; 16])?; // Reserved
        
        writer.flush()?;
        
        Ok(WalFile {
            path,
            file: writer,
            size: WAL_HEADER_SIZE as u64,
            entry_count: 0,
            first_sequence: sequence,
            last_sequence: sequence,
        })
    }
    
    /// Recover from existing WAL file
    fn recover_from_file(path: &Path, config: &WalConfig) -> Result<(WalFile, u64, MerkleChain)> {
        info!("Recovering from WAL file: {:?}", path);
        
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
            
        let mut reader = BufReader::new(file.try_clone()?);
        
        // Read and validate header
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(Error::WriteAheadLog {
                message: "Invalid WAL file magic number".to_string(),
                source: None,
            });
        }
        
        let version = reader.read_u32::<LittleEndian>()?;
        if version != WAL_VERSION {
            return Err(Error::WriteAheadLog {
                message: format!("Unsupported WAL version: {}", version),
                source: None,
            });
        }
        
        let _creation_time = reader.read_u64::<LittleEndian>()?;
        let first_sequence = reader.read_u64::<LittleEndian>()?;
        let mut last_sequence = reader.read_u64::<LittleEndian>()?;
        let mut entry_count = reader.read_u64::<LittleEndian>()?;
        let _checksum = reader.read_u32::<LittleEndian>()?;
        reader.read_exact(&mut [0u8; 16])?; // Reserved
        
        // Rebuild Merkle chain by reading all entries
        let mut merkle_chain = MerkleChain::new();
        let mut position = WAL_HEADER_SIZE as u64;
        
        loop {
            match Self::read_entry(&mut reader) {
                Ok(entry) => {
                    // Rebuild Merkle chain
                    merkle_chain.add(&entry.data);
                    last_sequence = entry.sequence;
                    position = reader.seek(SeekFrom::Current(0))?;
                }
                Err(_) => {
                    // End of valid entries
                    break;
                }
            }
        }
        
        // Seek to end for appending
        let file_size = reader.seek(SeekFrom::End(0))?;
        
        // Open for writing
        let writer = BufWriter::with_capacity(config.buffer_size, file);
        
        Ok((
            WalFile {
                path: path.to_path_buf(),
                file: writer,
                size: file_size,
                entry_count,
                first_sequence,
                last_sequence,
            },
            last_sequence + 1,
            merkle_chain,
        ))
    }
    
    /// Read entry from file
    fn read_entry(reader: &mut impl Read) -> Result<WalEntry> {
        // Read entry header
        let length = match reader.read_u32::<LittleEndian>() {
            Ok(len) => len as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::WriteAheadLog {
                    message: "EOF reached".to_string(),
                    source: Some(Box::new(e)),
                });
            }
            Err(e) => return Err(e.into()),
        };
        let sequence = reader.read_u64::<LittleEndian>()?;
        let timestamp = reader.read_u64::<LittleEndian>()?;
        let entry_type = EntryType::try_from(reader.read_u8()?)?;
        let _flags = reader.read_u8()?;
        let crc = reader.read_u32::<LittleEndian>()?;
        reader.read_exact(&mut [0u8; 6])?; // Reserved
        
        // Read Merkle info
        let mut prev_hash_bytes = [0u8; 32];
        let mut hash_bytes = [0u8; 32];
        let mut data_hash_bytes = [0u8; 32];
        reader.read_exact(&mut prev_hash_bytes)?;
        reader.read_exact(&mut hash_bytes)?;
        reader.read_exact(&mut data_hash_bytes)?;
        
        // Read payload
        let mut data = vec![0u8; length];
        reader.read_exact(&mut data)?;
        
        // Verify CRC
        if crc32_checksum(&data) != crc {
            return Err(Error::WriteAheadLog {
                message: "CRC mismatch in WAL entry".to_string(),
                source: None,
            });
        }
        
        let merkle = MerkleNode {
            hash: hex::encode(&hash_bytes),
            prev_hash: if prev_hash_bytes == [0u8; 32] {
                None
            } else {
                Some(hex::encode(&prev_hash_bytes))
            },
            data_hash: hex::encode(&data_hash_bytes),
            sequence,
        };
        
        Ok(WalEntry {
            sequence,
            timestamp,
            entry_type,
            data: Bytes::from(data),
            merkle,
        })
    }
    
    /// Read all entries from a specific sequence
    pub async fn read_from(&self, start_sequence: u64) -> Result<Vec<WalEntry>> {
        let mut all_entries = Vec::new();
        let mut seen_sequences = std::collections::HashSet::new();
        
        // First, get list of all WAL files sorted by sequence
        let mut wal_files = self.list_wal_files().await?;
        wal_files.sort_by_key(|f| f.0); // Sort by sequence number
        
        // Get current file path to avoid reading it twice
        let current_file_path = {
            let current = self.current_file.read();
            // Get the current file's sequence number from the path
            if let Some(name) = current.path.file_stem() {
                name.to_string_lossy().parse::<u64>().ok()
            } else {
                None
            }
        };
        
        // Read from each file that might contain entries >= start_sequence
        for (_file_seq, path) in wal_files {
            // Skip if this is the current file (we'll read it last with proper flushing)
            if let Some(current_seq) = current_file_path {
                if let Some(name) = path.file_stem() {
                    if let Ok(seq) = name.to_string_lossy().parse::<u64>() {
                        if seq == current_seq {
                            continue;
                        }
                    }
                }
            }
            
            // Open and read header to check sequence range
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            
            // Skip header validation for now - just position after header
            reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
            
            // Read all entries from this file
            while let Ok(entry) = Self::read_entry(&mut reader) {
                if entry.sequence >= start_sequence && !seen_sequences.contains(&entry.sequence) {
                    seen_sequences.insert(entry.sequence);
                    all_entries.push(entry);
                }
            }
        }
        
        // Finally, read from current file with proper flushing
        let (file_handle, _) = {
            let mut current_file = self.current_file.write(); // Use write lock to ensure we can flush
            current_file.file.flush()?;  // Flush the writer first
            current_file.file.get_ref().sync_all()?;
            (current_file.file.get_ref().try_clone()?, current_file.first_sequence)
        };
        
        let mut reader = BufReader::new(file_handle);
        reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
        
        while let Ok(entry) = Self::read_entry(&mut reader) {
            if entry.sequence >= start_sequence && !seen_sequences.contains(&entry.sequence) {
                seen_sequences.insert(entry.sequence);
                all_entries.push(entry);
            }
        }
        
        // Sort entries by sequence to ensure correct order
        all_entries.sort_by_key(|e| e.sequence);
        
        Ok(all_entries)
    }
    
    /// List all WAL files in the directory
    async fn list_wal_files(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut wal_files = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.wal_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                // Parse sequence number from filename (format: NNNNNNNNNNNNNNNNNNNN.wal)
                if let Some(name) = path.file_stem() {
                    if let Ok(seq) = name.to_string_lossy().parse::<u64>() {
                        wal_files.push((seq, path));
                    }
                }
            }
        }
        
        Ok(wal_files)
    }
    
    /// Verify Merkle chain integrity
    pub async fn verify_integrity(&self) -> Result<()> {
        info!("Verifying WAL integrity");
        
        let entries = self.read_from(0).await?;
        let merkle_nodes: Vec<MerkleNode> = entries.iter()
            .map(|e| e.merkle.clone())
            .collect();
            
        hanshiro_core::crypto::MerkleChain::verify_chain(&merkle_nodes)
            .map_err(|e| Error::MerkleValidation {
                expected: "Valid chain".to_string(),
                actual: e.to_string(),
            })?;
            
        info!("WAL integrity verified: {} entries", entries.len());
        Ok(())
    }
    
    /// Truncate WAL up to a specific sequence
    pub async fn truncate(&self, up_to_sequence: u64) -> Result<()> {
        info!("Truncating WAL up to sequence: {}", up_to_sequence);
        
        // List all WAL files
        let mut entries = tokio::fs::read_dir(&self.wal_dir).await?;
        let mut files_to_delete = Vec::new();
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                // Parse sequence from filename
                if let Some(name) = path.file_stem() {
                    if let Ok(seq) = name.to_string_lossy().parse::<u64>() {
                        if seq < up_to_sequence {
                            files_to_delete.push(path);
                        }
                    }
                }
            }
        }
        
        // Delete old files
        for path in files_to_delete {
            info!("Deleting old WAL file: {:?}", path);
            tokio::fs::remove_file(path).await?;
        }
        
        Ok(())
    }
    
    /// Flush current WAL file
    pub async fn flush(&self) -> Result<()> {
        let mut file = self.current_file.write();
        file.file.flush()?;
        file.file.get_ref().sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use hanshiro_core::types::*;
    
    #[tokio::test]
    async fn test_wal_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
            .await
            .unwrap();
        
        // Create test event
        let event = Event::new(
            EventType::NetworkConnection,
            EventSource {
                host: "test-host".to_string(),
                ip: None,
                collector: "test".to_string(),
                format: IngestionFormat::Raw,
            },
            b"test data".to_vec(),
        );
        
        // Append event
        let seq = wal.append(&event).await.unwrap();
        assert_eq!(seq, 0);
        
        // Read back
        let entries = wal.read_from(0).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 0);
    }
    
    #[tokio::test]
    async fn test_wal_merkle_chain() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
            .await
            .unwrap();
        
        // Append multiple events
        for i in 0..10 {
            let event = Event::new(
                EventType::NetworkConnection,
                EventSource {
                    host: format!("host-{}", i),
                    ip: None,
                    collector: "test".to_string(),
                    format: IngestionFormat::Raw,
                },
                format!("data-{}", i).into_bytes(),
            );
            wal.append(&event).await.unwrap();
        }
        
        // Verify integrity
        wal.verify_integrity().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create WAL and write data
        {
            let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
                .await
                .unwrap();
                
            for i in 0..5 {
                let event = Event::new(
                    EventType::NetworkConnection,
                    EventSource {
                        host: format!("host-{}", i),
                        ip: None,
                        collector: "test".to_string(),
                        format: IngestionFormat::Raw,
                    },
                    format!("data-{}", i).into_bytes(),
                );
                wal.append(&event).await.unwrap();
            }
            
            wal.flush().await.unwrap();
        }
        
        // Reopen WAL and verify data
        {
            let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
                .await
                .unwrap();
                
            let entries = wal.read_from(0).await.unwrap();
            assert_eq!(entries.len(), 5);
            
            // Verify integrity after recovery
            wal.verify_integrity().await.unwrap();
        }
    }
}