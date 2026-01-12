use bytes::Bytes;
use hanshiro_core::{crypto::MerkleNode, error::{Error, Result}};

pub const WAL_MAGIC: &[u8; 8] = b"HANSHIRO";
pub const WAL_VERSION: u32 = 1;
pub const WAL_HEADER_SIZE: usize = 64;
pub const ENTRY_HEADER_SIZE: usize = 32;
pub const MERKLE_INFO_SIZE: usize = 96;

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

impl AsRef<WalEntry> for WalEntry {
    fn as_ref(&self) -> &WalEntry {
        self
    }
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub max_file_size: u64,
    pub sync_on_write: bool,
    pub compression: bool,
    pub buffer_size: usize,
    pub group_commit_delay_us: u64,
    pub max_batch_size: usize,
    /// Enable Merkle chain integrity (tamper-proof). Disable for higher throughput.
    pub merkle_enabled: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1024 * 1024 * 1024, // 1GB
            sync_on_write: true,
            compression: false,
            buffer_size: 64 * 1024, // 64KB
            group_commit_delay_us: 2000,
            max_batch_size: 512,
            merkle_enabled: true, // Enabled by default for tamper-proof storage
        }
    }
}
