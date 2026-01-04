//! # HanshiroDB Core
//!
//! This crate provides the fundamental building blocks for HanshiroDB:
//! - Core data structures and traits
//! - Error types
//! - Common utilities
//! - Cryptographic primitives
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │                  hanshiro-core                  │
//! ├─────────────────────────────────────────────────┤
//! │  • types      - Core data structures           │
//! │  • traits     - Database interfaces            │
//! │  • error      - Error handling                 │
//! │  • crypto     - Merkle chains & checksums      │
//! │  • utils      - Common utilities               │
//! └─────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod crypto;
pub mod error;
pub mod metrics;
pub mod traits;
pub mod types;
pub mod utils;

// Re-export commonly used types
pub use error::{Error, Result};
pub use types::{
    Event, EventId, EventMetadata, EventType,
    Vector, VectorId, VectorDimension,
    Timestamp,
};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROTOCOL_VERSION: u32 = 1;