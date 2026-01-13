# HanshiroDB API Reference

High-level Rust API for SecOps vector database operations.

## Quick Start

```rust
use hanshiro_api::{HanshiroClient, EventBuilder, CodeArtifactBuilder, RemoteClient};
use hanshiro_core::{EventType, IngestionFormat, value::FileType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Local (embedded)
    let db = HanshiroClient::open("./data").await?;
    
    // OR Remote (server)
    let db = RemoteClient::connect("http://10.0.1.100:3000").await?;

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

## Deployment Modes

### Embedded (Local)

Direct file access, no network overhead:

```rust
let db = HanshiroClient::open("./data").await?;
```

### Server Mode

Start server:
```bash
cargo run --bin hanshiro-server -- --data-dir ./data --port 3000
```

Connect remotely:
```rust
let client = RemoteClient::connect("http://10.0.1.100:3000").await?;
```

---

## REST API Endpoints

### JSON Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/events` | Ingest event |
| GET | `/events/{hi}/{lo}` | Get event by ID |
| GET | `/events` | Scan all logs |
| POST | `/code` | Store code artifact |
| GET | `/code/{hi}/{lo}` | Get code by ID |
| POST | `/similar` | Vector similarity search |
| POST | `/correlate` | Run auto-correlation |

### Binary Endpoints (High-Performance)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/bin/events` | Ingest event (binary) |
| GET | `/bin/events/{hi}/{lo}` | Get event (binary) |
| POST | `/bin/code` | Store code (binary) |

Binary endpoints use rkyv serialization for zero-copy performance.

### Examples

```bash
# Health check
curl http://localhost:3000/health

# Ingest event (JSON)
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "ProcessStart",
    "source_host": "ws-01",
    "collector": "crowdstrike",
    "raw_data": "{\"process\":\"cmd.exe\"}",
    "metadata": {"severity": "high"}
  }'

# Get all logs
curl http://localhost:3000/events

# Vector similarity search
curl -X POST http://localhost:3000/similar \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.1, 0.2, ...], "top_k": 10}'

# Run correlation
curl -X POST http://localhost:3000/correlate
```

---

## HanshiroClient (Embedded)

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

### Methods

| Method | Description |
|--------|-------------|
| `ingest_event(event)` | Store a security event |
| `get_event(id)` | Retrieve event by ID |
| `scan_logs()` | Scan all log entries |
| `store_code(artifact)` | Store binary/script analysis |
| `get_code(id)` | Retrieve artifact by ID |
| `scan_code()` | Scan all code artifacts |
| `correlate_all()` | Auto-correlate all logs ↔ code |
| `find_correlated(id)` | Find entities related to ID |
| `find_similar(vector, k)` | Vector similarity search |
| `flush()` | Force flush to disk |

---

## RemoteClient

### Connecting

```rust
let client = RemoteClient::connect("http://10.0.1.100:3000").await?;
```

### JSON Methods

```rust
// Ingest (JSON - debuggable)
client.ingest_event(api_event).await?;
client.store_code(api_artifact).await?;

// Query
let event = client.get_event(id).await?;
let logs = client.scan_logs().await?;

// Search & correlate
let similar = client.find_similar(&vector, 10).await?;
let correlations = client.correlate_all().await?;
```

### Binary Methods (High-Performance)

```rust
// Ingest (binary - faster)
client.ingest_event_binary(&event).await?;
client.store_code_binary(&artifact).await?;

// Query (binary)
let event = client.get_event_binary(id).await?;
```

Use binary methods for bulk ingestion pipelines.

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

### Event Types

```rust
EventType::NetworkConnection
EventType::NetworkTraffic
EventType::DNSQuery
EventType::HTTPRequest
EventType::Authentication
EventType::FileCreate / FileModify / FileDelete
EventType::ProcessStart / ProcessStop
EventType::MalwareDetected
EventType::AnomalyDetected
EventType::PolicyViolation
EventType::Custom(String)
```

### Ingestion Formats

```rust
IngestionFormat::OCSF      // Open Cybersecurity Schema
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
```

---

## Embedder Trait

Integrate your ML model for generating embeddings.

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

### Recommended Models

| Model | Use Case | Dimensions |
|-------|----------|------------|
| all-MiniLM-L6-v2 | Log text | 384 |
| CodeBERT | Code/binaries | 768 |
| Titan Embeddings | General (AWS) | 1536 |

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

---

## Correlation Types

Auto-correlation detects relationships via:

| Type | Description |
|------|-------------|
| `VectorSimilarity` | Embedding cosine similarity > 0.85 |
| `HashMatch` | Log mentions artifact's SHA256 hash |
| `FilenameMatch` | Log mentions artifact's filename |

```rust
let correlations = db.correlate_all().await?;

for c in correlations {
    println!("{:?} -> {:?} ({:?}, score: {:.2})", 
        c.source_id, c.target_id, c.correlation_type, c.score);
}
```

---

## Performance Tips

1. **Use binary endpoints** for bulk ingestion (`/bin/events`)
2. **Batch correlation**: Call `correlate_all()` after batch inserts, not per-event
3. **Vector dimensions**: 128-384 for speed, 768+ for accuracy
4. **Block cache**: Increase `block_cache_size` for read-heavy workloads

```rust
// High-throughput ingestion
for event in events {
    client.ingest_event_binary(&event).await?;
}

// Then correlate once
client.correlate_all().await?;
```
