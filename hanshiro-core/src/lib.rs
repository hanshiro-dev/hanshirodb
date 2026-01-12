//! # HanshiroDB Core
//!
//! This crate provides the fundamental building blocks for HanshiroDB:
//! - Core data structures and traits
//! - Error types
//! - Common utilities
//! - Cryptographic primitives
//! - Fast serialization with rkyv
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │                  hanshiro-core                  │
//! ├─────────────────────────────────────────────────┤
//! │  • types         - Core data structures        │
//! │  • value         - Polymorphic KV values       │
//! │  • traits        - Database interfaces         │
//! │  • error         - Error handling              │
//! │  • crypto        - Merkle chains & checksums   │
//! │  • serialization - Fast rkyv serialization     │
//! │  • utils         - Common utilities            │
//! └─────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod crypto;
pub mod error;
pub mod metrics;
pub mod serialization;
pub mod traits;
pub mod types;
pub mod utils;
pub mod value;

// Re-export commonly used types
pub use error::{Error, Result};
pub use types::{
    Event, EventId, EventType, EventSource, IngestionFormat,
    Vector, VectorId,
};
pub use value::{
    HanshiroValue, KeyPrefix, StorageKey,
    CodeArtifact, FileType, Capabilities, IndexEntry,
};
pub use serialization::{serialize_event, deserialize_event, archived_event};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROTOCOL_VERSION: u32 = 1;