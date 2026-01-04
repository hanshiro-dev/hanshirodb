# HanshiroDB

<div align="center">
  <img src="docs/logo.svg" alt="HanshiroDB Logo" width="300"/>
  
  **High-Velocity, Tamper-Proof Vector Database for SecOps**
  
  [![Build Status](https://github.com/hanshirodb/hanshirodb/workflows/CI/badge.svg)](https://github.com/hanshirodb/hanshirodb/actions)
  [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
  [![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
</div>

---

HanshiroDB is a specialized vector database built from the ground up for security operations. Unlike traditional vector databases optimized for recommendation systems, HanshiroDB is engineered for:

- **Massive Write Throughput**: Handle 100,000+ security events per second
- **Tamper-Proof Storage**: Blockchain-like Merkle chaining ensures data integrity
- **Hybrid Search**: Combine vector similarity with metadata filtering in real-time
- **Time-Travel Queries**: Reconstruct the exact state at any point in history
- **Native SecOps Support**: Built-in parsers for OCSF, STIX/TAXII, Zeek, and more

## 🏗️ Architecture Overview

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   Ingestion     │     │    Query     │     │   Admin     │
│    Engine       │     │   Engine     │     │    API      │
└────────┬────────┘     └──────┬───────┘     └──────┬──────┘
         │                     │                      │
         └─────────────────────┴──────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │   Core Engine       │
                    ├─────────────────────┤
                    │ • WAL (Merkle)     │
                    │ • MemTable          │
                    │ • SSTable Manager   │
                    └─────────┬───────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
         ┌───────┴────────┐      ┌────────┴────────┐
         │  Vector Index   │      │ Metadata Index  │
         │   (DiskANN)     │      │ (Roaring Bitmap)│
         └─────────────────┘      └─────────────────┘
```

## Quick Start

### Installation

```bash
git clone https://github.com/hanshirodb/hanshirodb
cd hanshirodb
cargo build --release
```

### Basic Usage

```rust
// Connect to HanshiroDB
let client = HanshiroClient::connect("localhost:9090").await?;

// Ingest a security event
let event = SecurityEvent {
    timestamp: Utc::now(),
    source_ip: "10.0.0.1".parse()?,
    event_type: EventType::Authentication,
    raw_log: "Failed login attempt for user admin",
    metadata: json!({
        "severity": "high",
        "user": "admin",
        "service": "ssh"
    }),
};

client.ingest(event).await?;

// Vector similarity search with metadata filtering
let results = client
    .search()
    .vector_query("suspicious authentication pattern")
    .filter("severity", "high")
    .filter("service", "ssh")
    .time_range(Utc::now() - Duration::hours(24), Utc::now())
    .limit(100)
    .execute()
    .await?;
```

## Documentation

- [Architecture Guide](docs/architecture.md) - Deep dive into internals
- [API Reference](docs/api.md) - Complete API documentation
- [Operations Manual](docs/ops.md) - Deployment and monitoring
- [Security Model](docs/security.md) - Threat model and hardening

## Development

### Prerequisites

### Building

```bash
# Debug build
cargo build

# Release build with optimizations
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

### Key Areas for Contribution

- Additional security data format parsers
- Performance optimizations
- Query language enhancements
- Monitoring integrations
- Documentation improvements


## 🙏 Acknowledgments

HanshiroDB builds upon ideas from:

- **RocksDB** - LSM-tree implementation
- **DiskANN** - Disk-based vector indexing
- **Tantivy** - Full-text search in Rust
- **InfluxDB** - Time-series optimization

---

<div align="center">
  Built with ❤️ for the Security Community
</div>