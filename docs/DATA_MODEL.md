# Data Model

HanshiroDB uses a polymorphic key-value architecture that stores multiple data types in a single LSM-tree using key prefixes for virtual table separation.

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    HanshiroDB Data Model                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Physical Layer                         │   │
│  │                   (Single LSM-Tree)                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                     │
│         ┌──────────────────┼──────────────────┐                 │
│         ▼                  ▼                  ▼                  │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐             │
│  │  Logs      │    │   Code     │    │  Indexes   │             │
│  │  (0x01)    │    │  (0x02)    │    │  (0x03)    │             │
│  └────────────┘    └────────────┘    └────────────┘             │
│                                                                  │
│  Virtual Tables via Key Prefixes                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Key Structure

Keys are 25 bytes with the following layout:

```
┌─────────┬──────────────────┬──────────────────────────┐
│ Prefix  │    Timestamp     │          ID              │
│ (1 byte)│    (8 bytes)     │       (16 bytes)         │
├─────────┼──────────────────┼──────────────────────────┤
│  0x01   │  BE u64 nanos    │  UUID as (hi: u64, lo)   │
└─────────┴──────────────────┴──────────────────────────┘
```

### Key Prefixes

| Prefix | Name | Description |
|--------|------|-------------|
| `0x01` | Log | Security events (OCSF, Zeek, Suricata, etc.) |
| `0x02` | Code | Static binary/script analysis artifacts |
| `0x03` | Index | Secondary indexes for graph relationships |

### Why Byte Prefixes?

1. **Sorted Storage**: LSM-trees store keys in sorted order. Byte prefixes ensure all data of the same type is physically co-located.

2. **Efficient Scans**: Scanning `0x01*` retrieves only logs without touching code artifacts.

3. **Minimal Overhead**: Single byte vs string prefix saves space and comparison time.

## Value Types

All values are serialized using `rkyv` for zero-copy deserialization.

### HanshiroValue Enum

```rust
pub enum HanshiroValue {
    Log(Event),           // Security log entry
    Code(CodeArtifact),   // Binary/script analysis
    Index(IndexEntry),    // Secondary index
}
```

### Log Entry (Event)

Security events from various sources:

```rust
pub struct Event {
    pub id: EventId,
    pub timestamp_ms: u64,
    pub event_type: EventType,
    pub source: EventSource,
    pub raw_data: Vec<u8>,
    pub metadata_json: String,
    pub vector: Option<Vector>,  // Embedding for similarity search
    pub merkle_prev: Option<String>,
    pub merkle_hash: Option<String>,
}
```

Supported formats: OCSF, STIX, TAXII, Zeek, Suricata, CEF, LEEF, Raw.

### Code Artifact

Static analysis results for binaries and scripts:

```rust
pub struct CodeArtifact {
    pub id: EventId,
    pub hash_sha256: [u8; 32],
    pub file_type: FileType,      // PE, ELF, MachO, Script, etc.
    pub file_size: u64,
    pub capabilities: Capabilities,  // Bitflags
    pub imports: Vec<String>,     // API imports
    pub strings: Vec<String>,     // Interesting strings
    pub vector: Option<Vector>,   // CodeBERT embedding
    pub related_logs: Vec<EventId>,
    pub timestamp_ms: u64,
}
```

#### Capability Flags

```rust
pub struct Capabilities(pub u32);

impl Capabilities {
    pub const NETWORK: u32 = 1 << 0;      // Network operations
    pub const FILESYSTEM: u32 = 1 << 1;   // File I/O
    pub const REGISTRY: u32 = 1 << 2;     // Windows registry
    pub const PROCESS: u32 = 1 << 3;      // Process manipulation
    pub const CRYPTO: u32 = 1 << 4;       // Cryptographic APIs
    pub const KEYLOGGER: u32 = 1 << 5;    // Input capture
    pub const INJECTION: u32 = 1 << 6;    // Code injection
    pub const PERSISTENCE: u32 = 1 << 7;  // Persistence mechanisms
    pub const EVASION: u32 = 1 << 8;      // Anti-analysis
    pub const EXFILTRATION: u32 = 1 << 9; // Data exfiltration
}
```

### Index Entry

Secondary indexes for graph-like queries:

```rust
pub struct IndexEntry {
    pub field: String,      // e.g., "hash", "ip", "user"
    pub value: String,      // The indexed value
    pub refs: Vec<EventId>, // Related entity IDs
}
```

## Usage Patterns

### Storing a Log

```rust
use hanshiro_core::{Event, EventType, EventSource, IngestionFormat};

let event = Event::new(
    EventType::ProcessStart,
    EventSource {
        host: "server-01".into(),
        ip: Some("10.0.0.1".into()),
        collector: "sysmon".into(),
        format: IngestionFormat::Raw,
    },
    b"Process: bad.exe started".to_vec(),
);

// Using the engine
let id = engine.write(event).await?;
```

### Storing a Code Artifact

```rust
use hanshiro_core::{CodeArtifact, FileType, Capabilities};

let mut artifact = CodeArtifact::new(sha256_hash, FileType::PE, file_size);
artifact.capabilities.set(Capabilities::NETWORK);
artifact.capabilities.set(Capabilities::CRYPTO);
artifact.imports = vec!["CryptEncrypt".into(), "InternetOpenUrlA".into()];

let id = engine.write_code(artifact).await?;
```

### Creating a Secondary Index

Link a hash to related logs:

```rust
// Create index linking hash to log IDs
engine.create_index("hash", &hash_hex, vec![log_id_1, log_id_2]).await?;

// Or link code artifact to logs (creates both forward and reverse links)
engine.link_code_to_logs(code_id, vec![log_id_1, log_id_2]).await?;
```

### Reading Values

```rust
// Read any value by ID
let value = engine.read_value(id).await?;

// Read specific types
let event = engine.read(event_id).await?;           // Event only
let artifact = engine.read_code(code_id).await?;   // CodeArtifact only
```

### Scanning by Type

```rust
// Scan all logs in memory
let logs = engine.scan_logs().await?;

// Scan all code artifacts in memory
let artifacts = engine.scan_code().await?;

// Scan by prefix (virtual table)
let all_logs = engine.scan_prefix(KeyPrefix::Log).await?;
let all_code = engine.scan_prefix(KeyPrefix::Code).await?;
```

### Querying by Hash

```rust
// 1. Find the index entry
let index_key = format_index_key("hash", &hash_hex);
let index_entry = db.get(&index_key)?.as_index()?;

// 2. Fetch related logs
for log_id in &index_entry.refs {
    let log_key = StorageKey::new_log(*log_id);
    let log = db.get(&log_key.to_bytes())?.as_log()?;
    // Process log...
}
```

### Low-Level Key Operations

```rust
// Create keys directly
let log_key = StorageKey::new_log(event.id);
let code_key = StorageKey::new_code(artifact.id);
let index_key = StorageKey::new_index(EventId::new());

// Serialize to bytes (25 bytes)
let key_bytes = log_key.to_bytes();

// Deserialize
let recovered = StorageKey::from_bytes(&key_bytes)?;
```

## Graph Relationships

HanshiroDB supports graph-like queries without a separate graph database:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Graph Relationship Model                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Log Entry                    Code Artifact                      │
│  ┌─────────────┐              ┌─────────────┐                   │
│  │ id: abc123  │              │ hash: xyz   │                   │
│  │ process:    │──────────────│ imports:    │                   │
│  │  bad.exe    │   executed   │  CryptAPI   │                   │
│  └─────────────┘              └─────────────┘                   │
│         │                            │                           │
│         │                            │                           │
│         ▼                            ▼                           │
│  ┌─────────────────────────────────────────┐                    │
│  │         Secondary Index                  │                    │
│  │  field: "hash"                          │                    │
│  │  value: "xyz..."                        │                    │
│  │  refs: [abc123, def456, ...]            │                    │
│  └─────────────────────────────────────────┘                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Query: "Find logs related to this malware"

1. Upload `malware.exe`
2. Compute SHA256 → `xyz...`
3. Vector search in `Code` prefix → Find similar artifacts
4. Lookup `Index` for `hash:xyz...` → Get related log IDs
5. Fetch logs by ID

### Query: "Find similar behavior patterns"

1. Take a suspicious log's embedding vector
2. Vector search across `Log` prefix
3. Return top-K similar events

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Point lookup | O(log N) | Bloom filters skip irrelevant SSTables |
| Prefix scan | O(K) | K = number of matching keys |
| Vector search | O(log N) | Per-segment Vamana graphs |
| Index lookup | O(1) + O(M) | M = number of refs |

## Serialization

All values use `rkyv` for zero-copy deserialization:

```rust
// Serialize
let bytes = serialize_value(&value)?;

// Deserialize (with validation)
let value = deserialize_value(&bytes)?;

// Zero-copy access (fastest)
let archived = archived_value(&bytes)?;
let timestamp = archived.as_log()?.timestamp_ms;
```

Benefits:
- No parsing overhead on read
- Direct memory access to fields
- Schema validation on deserialize
- ~10x faster than JSON, ~3x faster than bincode
