//! Write-Ahead Log (WAL)
//!
//! WAL Structure
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    WAL File Structure                       │
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
//! │  │ Merkle Info (96 bytes)                              │    │
//! │  │   - Previous Hash (32 bytes)                        │    │
//! │  │   - Current Hash (32 bytes)                         │    │
//! │  │   - Data Hash (32 bytes)                            │    │
//! │  ├─────────────────────────────────────────────────────┤    │
//! │  │ Payload (Variable length)                           │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Entry 2                                                    │
//! │  ...                                                        │
//! └─────────────────────────────────────────────────────────────┘
//! 

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use parking_lot::RwLock;
use tokio::sync::mpsc;
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

pub struct WriteAheadLog {
    current_file: Arc<RwLock<WalFile>>,
    wal_dir: PathBuf,
    sequence: Arc<AtomicU64>,
    merkle_chain: Arc<RwLock<MerkleChain>>,
    metrics: Arc<Metrics>,
    config: WalConfig,
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub max_file_size: u64,
    pub sync_on_write: bool,
    pub compression: bool,
    pub buffer_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1024 * 1024 * 1024, // 1GB
            sync_on_write: true,
            compression: false,
            buffer_size: 64 * 1024, // 64KB
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
    /// Create new WAL instance
    pub async fn new(wal_dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        
        // Create WAL directory if it doesn't exist
        tokio::fs::create_dir_all(&wal_dir).await
            .map_err(|e| Error::Io { 
                message: format!("Failed to create WAL directory: {:?}", wal_dir),
                source: e,
            })?;
        
        // Find the latest WAL file or create a new one
        let (current_file, sequence, merkle_chain) = Self::open_or_create_wal_file(&wal_dir, &config).await?;
        
        Ok(Self {
            current_file: Arc::new(RwLock::new(current_file)),
            wal_dir,
            sequence: Arc::new(AtomicU64::new(sequence)),
            merkle_chain: Arc::new(RwLock::new(merkle_chain)),
            metrics: Arc::new(Metrics::new()),
            config,
        })
    }
    
    /// Append event to WAL
    pub async fn append(&self, event: &Event) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // Serialize event
        let data = bincode::serialize(event)
            .map_err(|e| Error::WriteAheadLog {
                message: "Failed to serialize event".to_string(),
                source: Some(Box::new(e)),
            })?;
        
        // Create Merkle node
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
        
        // Write to file
        self.write_entry(&entry).await?;
        
        // Record metrics
        self.metrics.record_wal_write(entry.data.len() as u64);
        
        Ok(sequence)
    }
    
    /// Write entry to WAL file
    async fn write_entry(&self, entry: &WalEntry) -> Result<()> {
        let mut should_rotate = false;
        
        // Write to current file
        {
            let mut wal_file = self.current_file.write();
            
            // Check if we need to rotate
            if wal_file.size >= self.config.max_file_size {
                should_rotate = true;
            } else {
                // Write entry
                Self::write_entry_to_file(&mut wal_file.file, entry)?;
                
                // Update file metadata
                wal_file.size += Self::entry_size(entry) as u64;
                wal_file.entry_count += 1;
                wal_file.last_sequence = entry.sequence;
                
                // Sync if configured
                if self.config.sync_on_write {
                    wal_file.file.flush()?;
                    wal_file.file.get_ref().sync_all()?;
                }
            }
        }
        
        // Rotate file if needed
        if should_rotate {
            self.rotate_file().await?;
            // Retry write to new file
            Box::pin(self.write_entry(entry)).await?;
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
        
        // Write header
        writer.write_all(&header_buf)?;
        
        // Write Merkle info
        writer.write_all(entry.merkle.prev_hash.as_ref()
            .map(|h| h.as_bytes())
            .unwrap_or(&[0u8; 32]))?;
        writer.write_all(entry.merkle.hash.as_bytes())?;
        writer.write_all(entry.merkle.data_hash.as_bytes())?;
        
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
        let length = reader.read_u32::<LittleEndian>()? as usize;
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
        let mut entries = Vec::new();
        
        // Read from current file
        let current_file = self.current_file.read();
        if current_file.first_sequence <= start_sequence {
            // Read entries from current file
            let file = current_file.file.get_ref().try_clone()?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
            
            while let Ok(entry) = Self::read_entry(&mut reader) {
                if entry.sequence >= start_sequence {
                    entries.push(entry);
                }
            }
        }
        
        Ok(entries)
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