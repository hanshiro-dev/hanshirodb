# HanshiroDB API Reference

High-level Rust API for SecOps vector database operations.

## Quick Start

```rust
use hanshiro_api::{HanshiroClient, EventBuilder, CodeArtifactBuilder};
use hanshiro_core::{EventType, IngestionFormat, value::FileType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database
    let db = HanshiroClient::open("./data").await?;

    // Ingest a security event
    let event = EventBuilder::new(EventType::ProcessStart)
        .source("endpoint-01", "crowdstrike", IngestionFormat::Raw)
        .source_ip("10.0.1.50")
        .raw_data(r#"{"process":"suspicious.exe","parent":"explorer.exe"}"#)
        .metadata("severity", "high")
        .vector(embedding_model.embed("suspicious process execution").await?)
        .build();

    let event_id = db.ingest_event(event).await?;

    // Store malware sample
    let artifact = CodeArtifactBuilder::new(sha256_hash, FileType::PE, 150000)
        .imports(vec!["kernel32.dll:CreateRemoteThread".into()])
        .strings(vec!["http://evil.com/beacon".into()])
        .vector(code_model.embed(&binary_features).await?)
        .build();

    db.store_code(artifact).await?;

    // Auto-correlate logs ↔ code
    let correlations = db.correlate_all().await?;

    Ok(())
}
```

---

## HanshiroClient

Main client for database operations.

### Opening a Database

```rust
// Default configuration
let db = HanshiroClient::open("./data").await?;

// Custom configuration
use hanshiro_storage::engine::StorageConfig;

let config = StorageConfig {
    data_dir: "./data".into(),
    block_cache_size: 128 * 1024 * 1024, // 128MB cache
    ..Default::default()
};
let db = HanshiroClient::open_with_config(config).await?;
```

### Event Operations

| Method | Description |
|--------|-------------|
| `ingest_event(event)` | Store a security event |
| `get_event(id)` | Retrieve event by ID |
| `scan_logs()` | Scan all log entries |

```rust
// Ingest
let id = db.ingest_event(event).await?;

// Retrieve
if let Some(event) = db.get_event(id).await? {
    println!("Found: {:?}", event.event_type);
}

// Scan all
for event in db.scan_logs().await? {
    println!("{}: {}", event.id, event.event_type);
}
```

### Code Artifact Operations

| Method | Description |
|--------|-------------|
| `store_code(artifact)` | Store binary/script analysis |
| `get_code(id)` | Retrieve artifact by ID |
| `scan_code()` | Scan all code artifacts |

```rust
let id = db.store_code(artifact).await?;
let artifacts = db.scan_code().await?;
```

### Correlation Operations

| Method | Description |
|--------|-------------|
| `correlate_all()` | Auto-correlate all logs ↔ code |
| `find_correlated(id)` | Find entities related to ID |
| `find_similar(vector, k)` | Vector similarity search |

```rust
// Run auto-correlation (hash matching, vector similarity)
let correlations = db.correlate_all().await?;
for c in correlations {
    println!("{:?} -> {:?} ({:?}, score: {:.2})", 
        c.source_id, c.target_id, c.correlation_type, c.score);
}

// Find related entities
let related = db.find_correlated(malware_id).await?;

// Vector similarity search
let similar = db.find_similar(&query_vector, 10).await?;
```

### Utility

```rust
// Force flush to disk
db.flush().await?;

// Access underlying engine for advanced ops
let engine = db.engine();
```

---

## EventBuilder

Fluent builder for security events.

```rust
let event = EventBuilder::new(EventType::NetworkConnection)
    .source("firewall-01", "paloalto", IngestionFormat::Raw)
    .source_ip("192.168.1.100")
    .raw_data(r#"{"src":"10.0.1.5","dst":"185.x.x.x","port":443}"#)
    .metadata("severity", "critical")
    .metadata("rule_id", 12345)
    .vector(vec![0.1, 0.2, ...])  // 128-768 dim embedding
    .build();
```

### Methods

| Method | Description |
|--------|-------------|
| `new(event_type)` | Create builder with event type |
| `source(host, collector, format)` | Set event source |
| `source_ip(ip)` | Set source IP address |
| `raw_data(data)` | Set raw event data |
| `metadata(key, value)` | Add metadata field |
| `vector(data)` | Set embedding vector |
| `build()` | Build the Event |

### Event Types

```rust
EventType::NetworkConnection  // Network conn established
EventType::NetworkTraffic     // Traffic flow data
EventType::DNSQuery           // DNS lookup
EventType::HTTPRequest        // HTTP request
EventType::Authentication     // Login attempt
EventType::Authorization      // Access control
EventType::FileCreate         // File created
EventType::FileModify         // File modified
EventType::FileDelete         // File deleted
EventType::ProcessStart       // Process spawned
EventType::ProcessStop        // Process terminated
EventType::MalwareDetected    // AV/EDR detection
EventType::AnomalyDetected    // Behavioral anomaly
EventType::PolicyViolation    // Policy breach
EventType::Custom(String)     // Custom event type
```

### Ingestion Formats

```rust
IngestionFormat::OCSF      // Open Cybersecurity Schema
IngestionFormat::STIX      // STIX 2.x
IngestionFormat::Zeek      // Zeek/Bro logs
IngestionFormat::Suricata  // Suricata alerts
IngestionFormat::CEF       // Common Event Format
IngestionFormat::Raw       // Raw/custom format
```

---

## CodeArtifactBuilder

Builder for malware/binary analysis artifacts.

```rust
let artifact = CodeArtifactBuilder::new(
    sha256_hash,      // [u8; 32]
    FileType::PE,     // File type
    file_size,        // u64
)
.imports(vec![
    "kernel32.dll:VirtualAllocEx".into(),
    "ntdll.dll:NtCreateThreadEx".into(),
])
.strings(vec![
    "http://c2.evil.com/beacon".into(),
    "YOUR FILES HAVE BEEN ENCRYPTED".into(),
])
.vector(code_embedding)
.build();
```

### File Types

```rust
FileType::PE        // Windows executable
FileType::ELF       // Linux executable
FileType::MachO     // macOS executable
FileType::Script    // PowerShell, Python, etc.
FileType::Document  // Office docs with macros
FileType::Archive   // ZIP/RAR with embedded content
FileType::Unknown
```

---

## Embedder Trait

Trait for integrating embedding models.

```rust
use hanshiro_api::Embedder;

#[async_trait]
impl Embedder for MyModel {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // Your embedding logic
    }
    
    fn dimension(&self) -> usize {
        384  // e.g., all-MiniLM-L6-v2
    }
}
```

### Production Options

| Model | Use Case | Dimensions |
|-------|----------|------------|
| all-MiniLM-L6-v2 | Log text | 384 |
| SBERT | Log text | 768 |
| CodeBERT | Code/binaries | 768 |
| Titan Embeddings | General (AWS) | 1536 |
| text-embedding-3-small | General (OpenAI) | 1536 |

### Example: AWS Bedrock

```rust
struct BedrockEmbedder {
    client: aws_sdk_bedrockruntime::Client,
}

#[async_trait]
impl Embedder for BedrockEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let resp = self.client
            .invoke_model()
            .model_id("amazon.titan-embed-text-v1")
            .body(Blob::new(serde_json::to_vec(&json!({"inputText": text}))?))
            .send()
            .await?;
        
        let result: TitanResponse = serde_json::from_slice(&resp.body)?;
        Ok(result.embedding)
    }
    
    fn dimension(&self) -> usize { 1536 }
}
```

### Example: Local ONNX

```rust
struct OnnxEmbedder {
    session: ort::Session,
    tokenizer: tokenizers::Tokenizer,
}

#[async_trait]
impl Embedder for OnnxEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let encoding = self.tokenizer.encode(text, true)?;
        let input_ids = encoding.get_ids();
        
        let outputs = self.session.run(ort::inputs![input_ids]?)?;
        Ok(outputs[0].try_extract()?.view().to_vec())
    }
    
    fn dimension(&self) -> usize { 384 }
}
```

---

## Correlation Types

Auto-correlation detects relationships via:

| Type | Description |
|------|-------------|
| `VectorSimilarity` | Embedding vectors are similar (cosine > 0.85) |
| `HashMatch` | Log mentions artifact's SHA256 hash |
| `FilenameMatch` | Log mentions artifact's filename |
| `IpMatch` | IP address correlation |
| `Manual` | User-defined relationship |

```rust
let correlations = db.correlate_all().await?;

for c in correlations {
    match c.correlation_type {
        CorrelationType::VectorSimilarity => {
            println!("Similar behavior: {} -> {} (score: {:.2})", 
                c.source_id, c.target_id, c.score);
        }
        CorrelationType::HashMatch => {
            println!("Hash match: {} references {}", 
                c.source_id, c.target_id);
        }
        _ => {}
    }
}
```

---

## Error Handling

All async methods return `Result<T>`:

```rust
use hanshiro_core::error::{Error, Result};

match db.ingest_event(event).await {
    Ok(id) => println!("Stored: {}", id),
    Err(Error::Io { message, .. }) => eprintln!("IO error: {}", message),
    Err(Error::Internal { message }) => eprintln!("Internal: {}", message),
    Err(e) => eprintln!("Error: {:?}", e),
}
```

---

## Performance Tips

1. **Batch ingestion**: Use `write_batch()` on engine for bulk inserts
2. **Lazy correlation**: Call `correlate_all()` after batch inserts, not per-event
3. **Vector dimensions**: Use 128-384 dims for speed, 768+ for accuracy
4. **Block cache**: Increase `block_cache_size` for read-heavy workloads

```rust
// Batch insert via engine
let events: Vec<Event> = ...;
db.engine().write_batch(events).await?;

// Then correlate once
db.correlate_all().await?;
```
