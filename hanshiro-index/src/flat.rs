//! # Flat Index (Brute-Force Exact Search)
//!
//! The "dumb" index that searches everything. Used for:
//! 1. Ground truth comparison (100% recall)
//! 2. Small datasets where graph overhead isn't worth it
//! 3. Unit testing graph index accuracy
//!
//! ## Architecture
//! - Vectors stored in contiguous memory (cache-friendly)
//! - Search: O(n) distance calculations, keep top-K in heap
//! - Supports both f32 and quantized (SQ8) storage

use std::collections::BinaryHeap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use parking_lot::RwLock;

use hanshiro_core::error::{Error, Result};

use crate::simd::{
    cosine_distance, dot_product, l2_distance_squared, normalize,
    quantize_sq8, dot_product_sq8, DistanceMetric,
};
use crate::traits::{IndexConfig, SearchResult, VectorIndex};

const FLAT_INDEX_MAGIC: &[u8; 8] = b"HNSHFLAT";
const FLAT_INDEX_VERSION: u32 = 1;

/// Flat index for exact nearest neighbor search
pub struct FlatIndex {
    config: IndexConfig,
    /// Contiguous vector storage: [v0_d0, v0_d1, ..., v0_dn, v1_d0, ...]
    vectors: RwLock<Vec<f32>>,
    /// Quantized vectors (if enabled)
    vectors_sq8: RwLock<Vec<i8>>,
    /// Vector IDs in insertion order
    ids: RwLock<Vec<u64>>,
    /// Number of vectors
    count: AtomicUsize,
}

impl FlatIndex {
    /// Create new flat index
    pub fn new(config: IndexConfig) -> Self {
        Self {
            config,
            vectors: RwLock::new(Vec::new()),
            vectors_sq8: RwLock::new(Vec::new()),
            ids: RwLock::new(Vec::new()),
            count: AtomicUsize::new(0),
        }
    }
    
    /// Create with pre-allocated capacity
    pub fn with_capacity(config: IndexConfig, capacity: usize) -> Self {
        let dim = config.dimension;
        Self {
            config,
            vectors: RwLock::new(Vec::with_capacity(capacity * dim)),
            vectors_sq8: RwLock::new(Vec::with_capacity(capacity * dim)),
            ids: RwLock::new(Vec::with_capacity(capacity)),
            count: AtomicUsize::new(0),
        }
    }
    
    /// Compute distance based on metric
    #[inline]
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.config.metric {
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::L2 => l2_distance_squared(a, b), // Use squared for speed
            DistanceMetric::DotProduct => -dot_product(a, b), // Negate for min-heap
        }
    }
    
    /// Search using quantized vectors (faster, approximate)
    pub fn search_quantized(&self, query: &[f32], k: usize) -> Vec<SearchResult> {
        if !self.config.quantized {
            return self.search(query, k);
        }
        
        let query_q = quantize_sq8(query);
        let vectors = self.vectors_sq8.read();
        let ids = self.ids.read();
        let dim = self.config.dimension;
        let n = self.count.load(Ordering::Relaxed);
        
        let mut heap: BinaryHeap<SearchResult> = BinaryHeap::with_capacity(k + 1);
        
        for i in 0..n {
            let start = i * dim;
            let end = start + dim;
            let vec_q = &vectors[start..end];
            
            // Quantized dot product (approximate distance)
            let dot = dot_product_sq8(&query_q, vec_q);
            let distance = -(dot as f32); // Negate for similarity -> distance
            
            heap.push(SearchResult { id: ids[i], distance });
            if heap.len() > k {
                heap.pop();
            }
        }
        
        let mut results: Vec<_> = heap.into_vec();
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
}

impl VectorIndex for FlatIndex {
    fn insert(&self, id: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.config.dimension {
            return Err(Error::Internal {
                message: format!(
                    "Vector dimension mismatch: expected {}, got {}",
                    self.config.dimension,
                    vector.len()
                ),
            });
        }
        
        // Normalize for cosine similarity
        let normalized = if self.config.metric == DistanceMetric::Cosine {
            let mut v = vector.to_vec();
            normalize(&mut v);
            v
        } else {
            vector.to_vec()
        };
        
        // Store f32 vectors
        {
            let mut vectors = self.vectors.write();
            vectors.extend_from_slice(&normalized);
        }
        
        // Store quantized if enabled
        if self.config.quantized {
            let quantized = quantize_sq8(&normalized);
            let mut vectors_sq8 = self.vectors_sq8.write();
            vectors_sq8.extend_from_slice(&quantized);
        }
        
        // Store ID
        {
            let mut ids = self.ids.write();
            ids.push(id);
        }
        
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    
    fn search(&self, query: &[f32], k: usize) -> Vec<SearchResult> {
        let vectors = self.vectors.read();
        let ids = self.ids.read();
        let dim = self.config.dimension;
        let n = self.count.load(Ordering::Relaxed);
        
        if n == 0 || k == 0 {
            return Vec::new();
        }
        
        // Normalize query for cosine
        let query_normalized;
        let query_vec = if self.config.metric == DistanceMetric::Cosine {
            query_normalized = crate::simd::normalized(query);
            &query_normalized[..]
        } else {
            query
        };
        
        // Use max-heap to keep top-K smallest distances
        let mut heap: BinaryHeap<SearchResult> = BinaryHeap::with_capacity(k + 1);
        
        for i in 0..n {
            let start = i * dim;
            let end = start + dim;
            let vec = &vectors[start..end];
            
            let distance = self.distance(query_vec, vec);
            
            heap.push(SearchResult { id: ids[i], distance });
            if heap.len() > k {
                heap.pop();
            }
        }
        
        // Convert to sorted vec (smallest distance first)
        let mut results: Vec<_> = heap.into_vec();
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
    
    fn get(&self, id: u64) -> Option<Vec<f32>> {
        let ids = self.ids.read();
        let vectors = self.vectors.read();
        let dim = self.config.dimension;
        
        ids.iter().position(|&x| x == id).map(|idx| {
            let start = idx * dim;
            let end = start + dim;
            vectors[start..end].to_vec()
        })
    }
    
    fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
    
    fn dimension(&self) -> usize {
        self.config.dimension
    }
    
    fn save(&self, path: &Path) -> Result<()> {
        let mut file = std::fs::File::create(path)?;
        
        // Header
        file.write_all(FLAT_INDEX_MAGIC)?;
        file.write_u32::<LittleEndian>(FLAT_INDEX_VERSION)?;
        file.write_u32::<LittleEndian>(self.config.dimension as u32)?;
        file.write_u8(self.config.metric as u8)?;
        file.write_u8(self.config.quantized as u8)?;
        
        let n = self.count.load(Ordering::Relaxed);
        file.write_u64::<LittleEndian>(n as u64)?;
        
        // IDs
        let ids = self.ids.read();
        for &id in ids.iter() {
            file.write_u64::<LittleEndian>(id)?;
        }
        
        // Vectors
        let vectors = self.vectors.read();
        for &v in vectors.iter() {
            file.write_f32::<LittleEndian>(v)?;
        }
        
        // Quantized vectors (if enabled)
        if self.config.quantized {
            let vectors_sq8 = self.vectors_sq8.read();
            let bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(vectors_sq8.as_ptr() as *const u8, vectors_sq8.len())
            };
            file.write_all(bytes)?;
        }
        
        file.flush()?;
        Ok(())
    }
    
    fn load(path: &Path) -> Result<Self> {
        let mut file = std::fs::File::open(path)?;
        
        // Header
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != FLAT_INDEX_MAGIC {
            return Err(Error::Internal {
                message: "Invalid flat index magic".to_string(),
            });
        }
        
        let version = file.read_u32::<LittleEndian>()?;
        if version != FLAT_INDEX_VERSION {
            return Err(Error::Internal {
                message: format!("Unsupported flat index version: {}", version),
            });
        }
        
        let dimension = file.read_u32::<LittleEndian>()? as usize;
        let metric = match file.read_u8()? {
            0 => DistanceMetric::Cosine,
            1 => DistanceMetric::L2,
            2 => DistanceMetric::DotProduct,
            _ => return Err(Error::Internal { message: "Invalid metric".to_string() }),
        };
        let quantized = file.read_u8()? != 0;
        let n = file.read_u64::<LittleEndian>()? as usize;
        
        // IDs
        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            ids.push(file.read_u64::<LittleEndian>()?);
        }
        
        // Vectors
        let mut vectors = Vec::with_capacity(n * dimension);
        for _ in 0..(n * dimension) {
            vectors.push(file.read_f32::<LittleEndian>()?);
        }
        
        // Quantized vectors
        let mut vectors_sq8 = Vec::new();
        if quantized {
            vectors_sq8.resize(n * dimension, 0i8);
            file.read_exact(unsafe {
                std::slice::from_raw_parts_mut(vectors_sq8.as_mut_ptr() as *mut u8, n * dimension)
            })?;
        }
        
        let config = IndexConfig { dimension, metric, quantized };
        
        Ok(Self {
            config,
            vectors: RwLock::new(vectors),
            vectors_sq8: RwLock::new(vectors_sq8),
            ids: RwLock::new(ids),
            count: AtomicUsize::new(n),
        })
    }
}
