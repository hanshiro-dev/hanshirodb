//! # HanshiroDB API
//!
//! High-level API for SecOps vector database operations.
//!
//! ## Embeddings
//!
//! In production, embeddings come from ML models:
//! - **Logs**: Sentence transformers (all-MiniLM-L6-v2, SBERT)
//! - **Malware**: CodeBERT, function2vec, or custom models
//! - **Network**: Flow/packet embedding models
//!
//! Implement the `Embedder` trait to integrate your model:
//! ```ignore
//! impl Embedder for MyModel {
//!     async fn embed(&self, text: &str) -> Result<Vec<f32>> { ... }
//! }
//! ```

pub mod client;
pub mod embedder;

pub use client::{HanshiroClient, EventBuilder, CodeArtifactBuilder};
pub use embedder::Embedder;
