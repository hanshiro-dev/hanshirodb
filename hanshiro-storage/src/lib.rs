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
pub mod partitioning;
pub mod engine;
pub mod manifest;
pub mod cache;

pub use engine::StorageEngine;
pub use wal::WriteAheadLog;
pub use memtable::MemTable;