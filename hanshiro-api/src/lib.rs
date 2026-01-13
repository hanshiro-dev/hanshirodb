//! # HanshiroDB API
//!
//! High-level API for SecOps vector database operations.
//!
//! ## Local vs Remote
//!
//! ```ignore
//! // Local (embedded) - opens database files directly
//! let db = HanshiroClient::open("./data").await?;
//!
//! // Remote - connects to server via HTTP
//! let db = RemoteClient::connect("http://10.0.1.100:3000").await?;
//! ```

pub mod client;
pub mod embedder;
pub mod server;
pub mod remote;

pub use client::{HanshiroClient, EventBuilder, CodeArtifactBuilder};
pub use embedder::Embedder;
pub use server::{AppState, create_router, ApiEvent, ApiCodeArtifact};
pub use remote::RemoteClient;

