//! # Vector Index Traits
//!
//! Common interface for all vector index implementations.

use std::path::Path;
use hanshiro_core::error::Result;

/// Search result with ID and distance
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub id: u64,
    pub distance: f32,
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Normal order: smaller distance = smaller in ordering
        // BinaryHeap is max-heap, so largest (worst) distance is at top
        // When we pop, we remove the worst result
        self.distance.partial_cmp(&other.distance).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Vector index trait - implemented by Flat and Graph indices
pub trait VectorIndex: Send + Sync {
    /// Insert a vector with given ID
    fn insert(&self, id: u64, vector: &[f32]) -> Result<()>;
    
    /// Search for k nearest neighbors
    fn search(&self, query: &[f32], k: usize) -> Vec<SearchResult>;
    
    /// Get vector by ID (if stored)
    fn get(&self, id: u64) -> Option<Vec<f32>>;
    
    /// Number of vectors in index
    fn len(&self) -> usize;
    
    /// Check if empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Vector dimension
    fn dimension(&self) -> usize;
    
    /// Save index to disk
    fn save(&self, path: &Path) -> Result<()>;
    
    /// Load index from disk
    fn load(path: &Path) -> Result<Self> where Self: Sized;
}

/// Index configuration
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Vector dimension
    pub dimension: usize,
    /// Distance metric
    pub metric: crate::simd::DistanceMetric,
    /// Use quantization (SQ8)
    pub quantized: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            dimension: 768,
            metric: crate::simd::DistanceMetric::Cosine,
            quantized: false,
        }
    }
}
