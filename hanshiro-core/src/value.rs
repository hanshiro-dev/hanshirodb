//! # Polymorphic Value Types
//!
//! HanshiroDB stores multiple data types in a single KV store using
//! key prefixes for virtual tables and a polymorphic value enum.
//!
//! ## Key Structure
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Key Layout                              │
//! ├─────────────────────────────────────────────────────────────┤
//! │  [prefix: u8] [timestamp: u64] [id: u128]                   │
//! │       │              │              │                        │
//! │       │              │              └─ Unique identifier     │
//! │       │              └─ Nanoseconds since epoch              │
//! │       └─ Data type (0x01=Log, 0x02=Code, 0x03=Index)        │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Virtual Tables
//!
//! | Prefix | Type | Description |
//! |--------|------|-------------|
//! | 0x01 | Log | Security events (OCSF, Zeek, etc.) |
//! | 0x02 | Code | Binary/script analysis artifacts |
//! | 0x03 | Index | Secondary indexes for graph links |

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::types::{Event, EventId, Vector};

/// Key prefix for virtual table separation.
/// Using single byte for minimal overhead and fast comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[repr(u8)]
pub enum KeyPrefix {
    /// Security log entries (OCSF, Zeek, Suricata, etc.)
    Log = 0x01,
    /// Static code/binary analysis artifacts
    Code = 0x02,
    /// Secondary indexes for graph relationships
    Index = 0x03,
}

impl KeyPrefix {
    pub fn as_byte(self) -> u8 {
        self as u8
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Log),
            0x02 => Some(Self::Code),
            0x03 => Some(Self::Index),
            _ => None,
        }
    }
}

/// Composite key with prefix for virtual table separation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct StorageKey {
    pub prefix: KeyPrefix,
    pub timestamp_ns: u64,
    pub id: EventId,
}

impl StorageKey {
    pub fn new_log(id: EventId) -> Self {
        Self {
            prefix: KeyPrefix::Log,
            timestamp_ns: Self::now_ns(),
            id,
        }
    }

    pub fn new_code(id: EventId) -> Self {
        Self {
            prefix: KeyPrefix::Code,
            timestamp_ns: Self::now_ns(),
            id,
        }
    }

    pub fn new_index(id: EventId) -> Self {
        Self {
            prefix: KeyPrefix::Index,
            timestamp_ns: Self::now_ns(),
            id,
        }
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// Serialize to bytes for storage (25 bytes total)
    pub fn to_bytes(&self) -> [u8; 25] {
        let mut buf = [0u8; 25];
        buf[0] = self.prefix.as_byte();
        buf[1..9].copy_from_slice(&self.timestamp_ns.to_be_bytes());
        buf[9..17].copy_from_slice(&self.id.hi.to_be_bytes());
        buf[17..25].copy_from_slice(&self.id.lo.to_be_bytes());
        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 25 {
            return None;
        }
        let prefix = KeyPrefix::from_byte(bytes[0])?;
        let timestamp_ns = u64::from_be_bytes(bytes[1..9].try_into().ok()?);
        let hi = u64::from_be_bytes(bytes[9..17].try_into().ok()?);
        let lo = u64::from_be_bytes(bytes[17..25].try_into().ok()?);
        Some(Self {
            prefix,
            timestamp_ns,
            id: EventId { hi, lo },
        })
    }
}

/// File type for code artifacts
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum FileType {
    PE,         // Windows executable
    ELF,        // Linux executable
    MachO,      // macOS executable
    Script,     // PowerShell, Python, etc.
    Document,   // Office docs with macros
    Archive,    // ZIP, RAR with embedded content
    Unknown,
}

/// Capability flags for code artifacts (bitflags)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct Capabilities(pub u32);

impl Capabilities {
    pub const NETWORK: u32 = 1 << 0;
    pub const FILESYSTEM: u32 = 1 << 1;
    pub const REGISTRY: u32 = 1 << 2;
    pub const PROCESS: u32 = 1 << 3;
    pub const CRYPTO: u32 = 1 << 4;
    pub const KEYLOGGER: u32 = 1 << 5;
    pub const INJECTION: u32 = 1 << 6;
    pub const PERSISTENCE: u32 = 1 << 7;
    pub const EVASION: u32 = 1 << 8;
    pub const EXFILTRATION: u32 = 1 << 9;

    pub fn has(&self, cap: u32) -> bool {
        self.0 & cap != 0
    }

    pub fn set(&mut self, cap: u32) {
        self.0 |= cap;
    }
}

/// Static code/binary analysis artifact
#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct CodeArtifact {
    pub id: EventId,
    pub hash_sha256: [u8; 32],
    pub file_type: FileType,
    pub file_size: u64,
    pub capabilities: Capabilities,
    /// Imported functions/APIs (compressed)
    pub imports: Vec<String>,
    /// Extracted strings of interest
    pub strings: Vec<String>,
    /// Embedding vector for similarity search
    pub vector: Option<Vector>,
    /// Related log IDs (graph links)
    pub related_logs: Vec<EventId>,
    pub timestamp_ms: u64,
}

impl CodeArtifact {
    pub fn new(hash: [u8; 32], file_type: FileType, file_size: u64) -> Self {
        Self {
            id: EventId::new(),
            hash_sha256: hash,
            file_type,
            file_size,
            capabilities: Capabilities::default(),
            imports: Vec::new(),
            strings: Vec::new(),
            vector: None,
            related_logs: Vec::new(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    pub fn hash_hex(&self) -> String {
        hex::encode(self.hash_sha256)
    }
}

/// Secondary index entry for graph relationships
#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct IndexEntry {
    /// The indexed field (e.g., "hash", "ip", "user")
    pub field: String,
    /// The indexed value
    pub value: String,
    /// List of related entity IDs
    pub refs: Vec<EventId>,
}

impl IndexEntry {
    pub fn new(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            value: value.into(),
            refs: Vec::new(),
        }
    }

    pub fn add_ref(&mut self, id: EventId) {
        if !self.refs.contains(&id) {
            self.refs.push(id);
        }
    }
}

/// Polymorphic value enum - the "Value" in our KV store.
/// All data types stored in HanshiroDB are variants of this enum.
#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum HanshiroValue {
    /// Security log entry (OCSF, Zeek, etc.)
    Log(Event),
    /// Static code/binary analysis
    Code(CodeArtifact),
    /// Secondary index for graph links
    Index(IndexEntry),
}

impl HanshiroValue {
    pub fn as_log(&self) -> Option<&Event> {
        match self {
            Self::Log(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_code(&self) -> Option<&CodeArtifact> {
        match self {
            Self::Code(c) => Some(c),
            _ => None,
        }
    }

    pub fn as_index(&self) -> Option<&IndexEntry> {
        match self {
            Self::Index(i) => Some(i),
            _ => None,
        }
    }

    pub fn prefix(&self) -> KeyPrefix {
        match self {
            Self::Log(_) => KeyPrefix::Log,
            Self::Code(_) => KeyPrefix::Code,
            Self::Index(_) => KeyPrefix::Index,
        }
    }

    pub fn vector(&self) -> Option<&Vector> {
        match self {
            Self::Log(e) => e.vector.as_ref(),
            Self::Code(c) => c.vector.as_ref(),
            Self::Index(_) => None,
        }
    }
}

impl From<Event> for HanshiroValue {
    fn from(e: Event) -> Self {
        Self::Log(e)
    }
}

impl From<CodeArtifact> for HanshiroValue {
    fn from(c: CodeArtifact) -> Self {
        Self::Code(c)
    }
}

impl From<IndexEntry> for HanshiroValue {
    fn from(i: IndexEntry) -> Self {
        Self::Index(i)
    }
}
