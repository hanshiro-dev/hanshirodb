//! The MemTable is an in-memory data structure that holds recent writes
//! before they are flushed to disk as SSTables. It uses a concurrent
//! skip list for fast, lock-free operations.
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        MemTable                             │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  ┌─────────────┐    Insert    ┌───────────────────────────┐ │
//! │  │   Events    │─────────────>│     Skip List             │ │
//! │  └─────────────┘              │                           │ │
//! │                               │  Level 3: 8 ---------> 25 │ │
//! │                               │  Level 2: 3 -> 8 ----> 25 │ │
//! │                               │  Level 1: 3 -> 8 -> 19->25│ │
//! │                               │  Level 0: 3->5->8->19->25 │ │
//! │                               └───────────────────────────┘ │
//! │                                         │                   │
//! │                                         ▼                   │
//! │                               ┌─────────────────┐           │
//! │                               │   Size Limit    │           │
//! │                               │   Reached?      │           │
//! │                               └────────┬────────┘           │
//! │                                        │ Yes                │
//! │                                        ▼                    │
//! │                               ┌─────────────────┐           │
//! │                               │  Flush to       │           │
//! │                               │  SSTable        │           │
//! │                               └─────────────────┘           │
//! └─────────────────────────────────────────────────────────────┘

mod manager;
mod table;
mod types;

pub use manager::MemTableManager;
pub use table::MemTable;
pub use types::{
    MemTableConfig, MemTableEntry, MemTableKey, MemTableManagerStats, MemTableStats,
};