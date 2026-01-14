//! # HanshiroDB Storage Engine
//!
//! LSM-tree based storage engine optimized for high write throughput.
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Write Path                              │
//! │                                                              │
//! │  Incoming Write ──> WAL (Merkle Chain) ──> MemTable         │
//! │                      │                       │               │
//! │                      ▼                       ▼               │
//! │                   Persist                 Flush to           │
//! │                   to Disk                 SSTable            │
//! └─────────────────────────────────────────────────────────────┘
//!
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Read Path                               │
//! │                                                              │
//! │  Query ──> Check MemTable ──> Check SSTables (newest first) │
//! │              │                    │                          │
//! │              ▼                    ▼                          │
//! │           Hot Data            Bloom Filters                  │
//! │           (Fast)              (Skip files)                   │
//! └─────────────────────────────────────────────────────────────┘
//! ```

pub mod wal;
pub mod memtable;
pub mod sstable;
pub mod compaction;
pub mod correlation;
pub mod partitioning;
pub mod engine;
pub mod manifest;
pub mod cache;
pub mod fd;
pub mod cached_time;
pub mod vector_store;

pub use compaction::{Compactor, CompactionConfig, CompactionResult};
pub use correlation::{CorrelationEngine, CorrelationConfig, Correlation, CorrelationType};
pub use fd::{FdConfig, FdMonitor, FdStats, SSTablePool};
pub use vector_store::VectorStore;

pub use engine::StorageEngine;
pub use wal::WriteAheadLog;
pub use memtable::{MemTable, MemTableManager};
pub use cache::{BlockCache, CacheKey, CacheStats};