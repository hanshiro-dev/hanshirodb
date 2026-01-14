<div align="center">
  <img src="docs/logo.png" alt="HanshiroDB Logo" width="800"/>
  
  **High-Velocity, Tamper-Proof Vector Database for SecOps**
  
  [![Build Status](https://github.com/hanshiro-dev/hanshirodb/workflows/CI/badge.svg)](https://github.com/hanshiro-dev/hanshirodb/actions)
  [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
  [![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
</div>

---

HanshiroDB is a specialized vector database built from the ground up for security operations. Unlike traditional vector databases optimized for recommendation systems, HanshiroDB is engineered for:

- **Massive Write Throughput**: 980,000+ events per second
- **Tamper-Proof Storage**: Merkle chaining ensures data integrity
- **Auto-Correlation**: Automatically links logs ↔ malware samples
- **Vector Similarity**: Find similar threats using embeddings
- **Polymorphic Storage**: Logs, code artifacts, and indexes in one DB

## Quick Start

### Installation

```bash
git clone https://github.com/hanshirodb/hanshirodb
cd hanshirodb
cargo build --release
```

### Embedded Mode (Local)

```rust
use hanshiro_api::{HanshiroClient, EventBuilder};
use hanshiro_core::{EventType, IngestionFormat};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open local database
    let db = HanshiroClient::open("./data").await?;

    // Ingest a security event
    let event = EventBuilder::new(EventType::ProcessStart)
        .source("endpoint-01", "crowdstrike", IngestionFormat::Raw)
        .source_ip("10.0.1.50")
        .raw_data(r#"{"process":"suspicious.exe","parent":"explorer.exe"}"#)
        .metadata("severity", "critical")
        .metadata("technique", "T1059.001")
        .vector(embedding)  // From your ML model
        .build();

    let id = db.ingest_event(event).await?;

    // Find similar events by vector
    let similar = db.find_similar(&query_vector, 10).await?;

    // Auto-correlate logs with malware samples
    let correlations = db.correlate_all().await?;

    Ok(())
}
```

### Server Mode (Remote)

Start the server:
```bash
cargo run --bin hanshiro-server -- --data-dir ./data --port 3000
```

Connect from anywhere:
```rust
use hanshiro_api::RemoteClient;

let client = RemoteClient::connect("http://10.0.1.100:3000").await?;

// JSON API (debuggable)
client.ingest_event(event).await?;

// Binary API (high-throughput)
client.ingest_event_binary(&event).await?;
```

### REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/events` | Ingest event (JSON) |
| GET | `/events/{hi}/{lo}` | Get event by ID |
| GET | `/events` | Scan all logs |
| POST | `/code` | Store code artifact |
| POST | `/similar` | Vector similarity search |
| POST | `/correlate` | Run auto-correlation |
| POST | `/bin/events` | Ingest event (binary, fast) |
| POST | `/bin/code` | Store code (binary, fast) |

Example with curl:
```bash
# Ingest event
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"ProcessStart","source_host":"ws-01","collector":"edr","raw_data":"{}"}'

# Get all logs
curl http://localhost:3000/events
```

## Data Model

HanshiroDB stores three types of data with key prefixes:

| Prefix | Type | Description |
|--------|------|-------------|
| `0x01` | Log | Security events (OCSF, Zeek, Suricata) |
| `0x02` | Code | Malware/binary analysis artifacts |
| `0x03` | Index | Secondary indexes, graph relationships |

Auto-correlation links logs to code via:
- **Vector similarity**: Embedding cosine distance > 0.85
- **Hash matching**: Log mentions artifact's SHA256
- **Filename matching**: Log references artifact filename

## Documentation

- [API Reference](docs/API.md) - Client API, builders, embedder trait
- [Data Model](docs/DATA_MODEL.md) - Storage internals, key structure
- [Optimization Guide](docs/OPTIMIZATION_GUIDE.md) - Performance tuning

## Development

```bash
# Build
cargo build --release

# Test
cargo test

# Run server
cargo run --bin hanshiro-server -- --data-dir ./data --port 3000

# Benchmarks
cargo test --release -p hanshiro-storage --test storage_peak_performance
```

## Performance

| Metric | Value |
|--------|-------|
| Event write throughput | 980K events/sec |
| Vector insert throughput | 2.4M vectors/sec |
| Vector search (10K, top-10) | 0.88ms |
| Latency per event | ~1 µs |
| Vector dimensions | 128-768 |
| Serialization | rkyv (zero-copy) |

Vectors are stored separately in a memory-mapped file for SIMD-optimized similarity search.

## License

Apache 2.0

---

<div align="center">
  Built with ❤️ for the Security Community
</div>
