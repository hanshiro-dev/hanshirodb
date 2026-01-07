//! # HanshiroDB Vector Index
//!
//! High-performance vector indexing for security event embeddings.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Vector Index Layer                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
//! │  │    SIMD     │    │    Flat     │    │   Vamana    │     │
//! │  │   Math      │    │   Index     │    │   Graph     │     │
//! │  │  (AVX2)     │    │  (Exact)    │    │   (ANN)     │     │
//! │  └─────────────┘    └─────────────┘    └─────────────┘     │
//! │         │                  │                  │              │
//! │         └──────────────────┴──────────────────┘              │
//! │                           │                                  │
//! │                    VectorIndex Trait                         │
//! │                                                              │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Modules
//!
//! - `simd`: SIMD-accelerated distance calculations (AVX2/FMA)
//! - `traits`: Common VectorIndex trait
//! - `flat`: Brute-force exact search (ground truth)
//! - `vamana`: DiskANN graph index (coming soon)

pub mod simd;
pub mod traits;
pub mod flat;
pub mod vamana;
pub mod hybrid;

pub use simd::{
    cosine_similarity, cosine_distance, dot_product,
    l2_distance, l2_distance_squared, normalize, normalized,
    quantize_sq8, dequantize_sq8, dot_product_sq8,
    DistanceMetric,
};

pub use traits::{VectorIndex, SearchResult, IndexConfig};
pub use flat::FlatIndex;
pub use vamana::{VamanaIndex, VamanaConfig};
pub use hybrid::{HybridIndex, HybridConfig, VectorSearchResult};
