//! # Vamana Graph Index (DiskANN)
//!
//! Approximate nearest neighbor search using a navigable small-world graph.
//! Designed for disk-based operation with large datasets.
//!
//! ## Algorithm Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Vamana Graph Structure                    │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │     ┌───┐         ┌───┐         ┌───┐                       │
//! │     │ A │─────────│ B │─────────│ C │                       │
//! │     └─┬─┘         └─┬─┘         └─┬─┘                       │
//! │       │    ╲        │        ╱    │                         │
//! │       │      ╲      │      ╱      │                         │
//! │     ┌─┴─┐      ╲  ┌─┴─┐  ╱      ┌─┴─┐                       │
//! │     │ D │────────│ E │────────│ F │  ← Entry Point          │
//! │     └───┘        └───┘        └───┘                         │
//! │                                                              │
//! │  Key Properties:                                             │
//! │  • Each node has R neighbors (degree bound)                  │
//! │  • Edges are "diverse" - not all pointing same direction     │
//! │  • Greedy search converges to nearest neighbor               │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Build Process
//! 1. Initialize random graph
//! 2. For each node, run greedy search to find candidates
//! 3. Apply RobustPrune to select diverse neighbors
//!
//! ## Search Process
//! 1. Start from entry point (medoid)
//! 2. Greedy walk: always move to unvisited neighbor closest to query
//! 3. Return top-K from visited set

use std::collections::{BinaryHeap, HashSet};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use rayon::prelude::*;

use hanshiro_core::error::{Error, Result};

use crate::simd::{l2_distance_squared, DistanceMetric};
use crate::traits::{IndexConfig, SearchResult, VectorIndex};

const VAMANA_MAGIC: &[u8; 8] = b"HNSHVAMN";
const VAMANA_VERSION: u32 = 1;

/// Vamana index configuration
#[derive(Debug, Clone)]
pub struct VamanaConfig {
    /// Vector dimension
    pub dimension: usize,
    /// Max out-degree per node (R)
    pub max_degree: usize,
    /// Search list size during build (L)
    pub build_search_size: usize,
    /// Alpha parameter for RobustPrune (typically 1.2)
    pub alpha: f32,
    /// Distance metric
    pub metric: DistanceMetric,
}

impl Default for VamanaConfig {
    fn default() -> Self {
        Self {
            dimension: 768,
            max_degree: 64,
            build_search_size: 128,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        }
    }
}

/// A node in the graph
struct Node {
    /// Vector ID
    id: u64,
    /// Neighbor indices (not IDs)
    neighbors: RwLock<Vec<u32>>,
}

/// Vamana graph index
pub struct VamanaIndex {
    config: VamanaConfig,
    /// All vectors stored contiguously
    vectors: RwLock<Vec<f32>>,
    /// Graph nodes
    nodes: RwLock<Vec<Node>>,
    /// Entry point (medoid index)
    entry_point: AtomicU32,
    /// Number of vectors
    count: AtomicUsize,
}

impl VamanaIndex {
    /// Create new empty index
    pub fn new(config: VamanaConfig) -> Self {
        Self {
            config,
            vectors: RwLock::new(Vec::new()),
            nodes: RwLock::new(Vec::new()),
            entry_point: AtomicU32::new(0),
            count: AtomicUsize::new(0),
        }
    }

    /// Build index from a batch of vectors (more efficient than incremental)
    pub fn build(config: VamanaConfig, ids: &[u64], vectors: &[f32]) -> Result<Self> {
        let n = ids.len();
        let dim = config.dimension;
        
        if vectors.len() != n * dim {
            return Err(Error::Internal {
                message: format!("Vector count mismatch: {} ids, {} floats", n, vectors.len()),
            });
        }

        let index = Self {
            config: config.clone(),
            vectors: RwLock::new(vectors.to_vec()),
            nodes: RwLock::new(Vec::with_capacity(n)),
            entry_point: AtomicU32::new(0),
            count: AtomicUsize::new(n),
        };

        // Initialize nodes with random neighbors
        {
            let mut nodes = index.nodes.write();
            let mut rng = rand::thread_rng();
            
            for i in 0..n {
                let mut neighbors: Vec<u32> = (0..n as u32).filter(|&j| j != i as u32).collect();
                neighbors.shuffle(&mut rng);
                neighbors.truncate(config.max_degree);
                
                nodes.push(Node {
                    id: ids[i],
                    neighbors: RwLock::new(neighbors),
                });
            }
        }

        // Find medoid as entry point
        let medoid = index.find_medoid();
        index.entry_point.store(medoid, Ordering::Relaxed);

        // Build graph using Vamana algorithm
        index.vamana_build()?;

        Ok(index)
    }

    /// Get vector at index
    #[inline]
    fn get_vector(&self, idx: usize) -> Vec<f32> {
        let vectors = self.vectors.read();
        let dim = self.config.dimension;
        let start = idx * dim;
        vectors[start..start + dim].to_vec()
    }

    /// Get vector slice at index (for distance calculation)
    #[inline]
    fn vector_slice<'a>(&self, vectors: &'a [f32], idx: usize) -> &'a [f32] {
        let dim = self.config.dimension;
        let start = idx * dim;
        &vectors[start..start + dim]
    }

    /// Compute distance between query and indexed vector
    #[inline]
    fn distance(&self, query: &[f32], vectors: &[f32], idx: usize) -> f32 {
        let vec = self.vector_slice(vectors, idx);
        match self.config.metric {
            DistanceMetric::L2 => l2_distance_squared(query, vec),
            DistanceMetric::Cosine => crate::simd::cosine_distance(query, vec),
            DistanceMetric::DotProduct => -crate::simd::dot_product(query, vec),
        }
    }

    /// Find medoid (vector closest to centroid)
    fn find_medoid(&self) -> u32 {
        let vectors = self.vectors.read();
        let n = self.count.load(Ordering::Relaxed);
        let dim = self.config.dimension;

        if n == 0 {
            return 0;
        }

        // Compute centroid
        let mut centroid = vec![0.0f32; dim];
        for i in 0..n {
            let vec = self.vector_slice(&vectors, i);
            for (c, v) in centroid.iter_mut().zip(vec.iter()) {
                *c += v;
            }
        }
        for c in centroid.iter_mut() {
            *c /= n as f32;
        }

        // Find closest to centroid
        let mut best_idx = 0u32;
        let mut best_dist = f32::MAX;
        for i in 0..n {
            let dist = l2_distance_squared(&centroid, self.vector_slice(&vectors, i));
            if dist < best_dist {
                best_dist = dist;
                best_idx = i as u32;
            }
        }

        best_idx
    }

    /// Greedy search from entry point
    fn greedy_search(&self, query: &[f32], search_size: usize) -> Vec<(u32, f32)> {
        let vectors = self.vectors.read();
        let nodes = self.nodes.read();
        let n = self.count.load(Ordering::Relaxed);

        if n == 0 {
            return Vec::new();
        }

        let entry = self.entry_point.load(Ordering::Relaxed) as usize;
        
        // Candidate set (min-heap by distance)
        let mut candidates: BinaryHeap<std::cmp::Reverse<(OrderedFloat, u32)>> = BinaryHeap::new();
        // Result set (max-heap to track worst in top-L)
        let mut results: BinaryHeap<(OrderedFloat, u32)> = BinaryHeap::new();
        // Visited set
        let mut visited: HashSet<u32> = HashSet::new();

        let entry_dist = self.distance(query, &vectors, entry);
        candidates.push(std::cmp::Reverse((OrderedFloat(entry_dist), entry as u32)));
        results.push((OrderedFloat(entry_dist), entry as u32));
        visited.insert(entry as u32);

        while let Some(std::cmp::Reverse((OrderedFloat(dist), idx))) = candidates.pop() {
            // If this candidate is worse than our worst result, we're done
            if results.len() >= search_size {
                if let Some(&(OrderedFloat(worst), _)) = results.peek() {
                    if dist > worst {
                        break;
                    }
                }
            }

            // Explore neighbors
            let neighbors = nodes[idx as usize].neighbors.read();
            for &neighbor in neighbors.iter() {
                if visited.insert(neighbor) {
                    let neighbor_dist = self.distance(query, &vectors, neighbor as usize);
                    
                    candidates.push(std::cmp::Reverse((OrderedFloat(neighbor_dist), neighbor)));
                    results.push((OrderedFloat(neighbor_dist), neighbor));
                    
                    // Keep only top search_size
                    while results.len() > search_size {
                        results.pop();
                    }
                }
            }
        }

        // Convert to sorted vec
        let mut result_vec: Vec<_> = results.into_iter().map(|(OrderedFloat(d), i)| (i, d)).collect();
        result_vec.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        result_vec
    }

    /// RobustPrune: Select diverse neighbors
    fn robust_prune(&self, node_idx: u32, candidates: &[(u32, f32)]) -> Vec<u32> {
        let vectors = self.vectors.read();
        let alpha = self.config.alpha;
        let max_degree = self.config.max_degree;

        let mut result: Vec<u32> = Vec::with_capacity(max_degree);
        let mut remaining: Vec<(u32, f32)> = candidates.to_vec();
        
        // Sort by distance
        remaining.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        while !remaining.is_empty() && result.len() < max_degree {
            // Take closest
            let (best_idx, _best_dist) = remaining.remove(0);
            
            if best_idx == node_idx {
                continue;
            }
            
            result.push(best_idx);

            // Filter remaining: keep only if not "covered" by best
            let best_vec = self.vector_slice(&vectors, best_idx as usize);
            remaining.retain(|(idx, dist)| {
                if *idx == node_idx {
                    return false;
                }
                let vec = self.vector_slice(&vectors, *idx as usize);
                let dist_to_best = match self.config.metric {
                    DistanceMetric::L2 => l2_distance_squared(best_vec, vec),
                    DistanceMetric::Cosine => crate::simd::cosine_distance(best_vec, vec),
                    DistanceMetric::DotProduct => -crate::simd::dot_product(best_vec, vec),
                };
                // Keep if distance to query is significantly less than alpha * distance to best
                *dist < alpha * dist_to_best
            });
        }

        result
    }

    /// Main Vamana build algorithm (parallel)
    fn vamana_build(&self) -> Result<()> {
        let n = self.count.load(Ordering::Relaxed);
        let search_size = self.config.build_search_size;
        let max_degree = self.config.max_degree;

        // Process nodes in parallel batches
        // Each batch: search + prune (read-heavy), then update edges (write)
        let batch_size = 256.min(n);
        let mut order: Vec<usize> = (0..n).collect();
        order.shuffle(&mut rand::thread_rng());

        for batch_start in (0..n).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(n);
            let batch = &order[batch_start..batch_end];

            // Parallel: compute new neighbors for each node in batch
            let updates: Vec<(usize, Vec<u32>)> = batch
                .par_iter()
                .map(|&i| {
                    let query = self.get_vector(i);
                    let candidates = self.greedy_search(&query, search_size);
                    let new_neighbors = self.robust_prune(i as u32, &candidates);
                    (i, new_neighbors)
                })
                .collect();

            // Sequential: apply updates (to avoid write conflicts)
            for (i, new_neighbors) in updates {
                // Update this node's neighbors
                {
                    let nodes = self.nodes.read();
                    let mut neighbors = nodes[i].neighbors.write();
                    *neighbors = new_neighbors.clone();
                }

                // Add reverse edges
                for &neighbor in &new_neighbors {
                    let nodes = self.nodes.read();
                    let mut neighbor_neighbors = nodes[neighbor as usize].neighbors.write();
                    
                    if !neighbor_neighbors.contains(&(i as u32)) {
                        if neighbor_neighbors.len() < max_degree {
                            neighbor_neighbors.push(i as u32);
                        }
                        // Skip expensive re-pruning for reverse edges in batch mode
                    }
                }
            }
        }

        Ok(())
    }
}

impl VectorIndex for VamanaIndex {
    fn insert(&self, id: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.config.dimension {
            return Err(Error::Internal {
                message: format!(
                    "Dimension mismatch: expected {}, got {}",
                    self.config.dimension,
                    vector.len()
                ),
            });
        }

        let idx = self.count.fetch_add(1, Ordering::Relaxed);

        // Add vector
        {
            let mut vectors = self.vectors.write();
            vectors.extend_from_slice(vector);
        }

        // Add node with random initial neighbors
        {
            let mut nodes = self.nodes.write();
            let n = nodes.len();
            
            let mut rng = rand::thread_rng();
            let mut neighbors: Vec<u32> = (0..n as u32).collect();
            neighbors.shuffle(&mut rng);
            neighbors.truncate(self.config.max_degree.min(n));
            
            nodes.push(Node {
                id,
                neighbors: RwLock::new(neighbors),
            });
        }

        // Update entry point if this is first node
        if idx == 0 {
            self.entry_point.store(0, Ordering::Relaxed);
        }

        // Connect to graph via greedy search + prune
        if idx > 0 {
            let candidates = self.greedy_search(vector, self.config.build_search_size);
            let new_neighbors = self.robust_prune(idx as u32, &candidates);
            
            {
                let nodes = self.nodes.read();
                let mut neighbors = nodes[idx].neighbors.write();
                *neighbors = new_neighbors.clone();
            }

            // Add reverse edges
            for &neighbor in &new_neighbors {
                let nodes = self.nodes.read();
                let mut neighbor_neighbors = nodes[neighbor as usize].neighbors.write();
                if !neighbor_neighbors.contains(&(idx as u32)) 
                    && neighbor_neighbors.len() < self.config.max_degree 
                {
                    neighbor_neighbors.push(idx as u32);
                }
            }
        }

        Ok(())
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<SearchResult> {
        let search_size = k.max(self.config.build_search_size);
        let results = self.greedy_search(query, search_size);
        
        let nodes = self.nodes.read();
        results
            .into_iter()
            .take(k)
            .map(|(idx, dist)| SearchResult {
                id: nodes[idx as usize].id,
                distance: dist,
            })
            .collect()
    }

    fn get(&self, id: u64) -> Option<Vec<f32>> {
        let nodes = self.nodes.read();
        nodes.iter().position(|n| n.id == id).map(|idx| self.get_vector(idx))
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
        file.write_all(VAMANA_MAGIC)?;
        file.write_u32::<LittleEndian>(VAMANA_VERSION)?;
        file.write_u32::<LittleEndian>(self.config.dimension as u32)?;
        file.write_u32::<LittleEndian>(self.config.max_degree as u32)?;
        file.write_f32::<LittleEndian>(self.config.alpha)?;
        file.write_u8(self.config.metric as u8)?;
        
        let n = self.count.load(Ordering::Relaxed);
        file.write_u64::<LittleEndian>(n as u64)?;
        file.write_u32::<LittleEndian>(self.entry_point.load(Ordering::Relaxed))?;

        // Node IDs and neighbors
        let nodes = self.nodes.read();
        for node in nodes.iter() {
            file.write_u64::<LittleEndian>(node.id)?;
            let neighbors = node.neighbors.read();
            file.write_u32::<LittleEndian>(neighbors.len() as u32)?;
            for &neighbor in neighbors.iter() {
                file.write_u32::<LittleEndian>(neighbor)?;
            }
        }

        // Vectors
        let vectors = self.vectors.read();
        for &v in vectors.iter() {
            file.write_f32::<LittleEndian>(v)?;
        }

        file.flush()?;
        Ok(())
    }

    fn load(path: &Path) -> Result<Self> {
        let mut file = std::fs::File::open(path)?;

        // Header
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != VAMANA_MAGIC {
            return Err(Error::Internal { message: "Invalid Vamana magic".to_string() });
        }

        let version = file.read_u32::<LittleEndian>()?;
        if version != VAMANA_VERSION {
            return Err(Error::Internal { 
                message: format!("Unsupported Vamana version: {}", version) 
            });
        }

        let dimension = file.read_u32::<LittleEndian>()? as usize;
        let max_degree = file.read_u32::<LittleEndian>()? as usize;
        let alpha = file.read_f32::<LittleEndian>()?;
        let metric = match file.read_u8()? {
            0 => DistanceMetric::Cosine,
            1 => DistanceMetric::L2,
            2 => DistanceMetric::DotProduct,
            _ => return Err(Error::Internal { message: "Invalid metric".to_string() }),
        };

        let n = file.read_u64::<LittleEndian>()? as usize;
        let entry_point = file.read_u32::<LittleEndian>()?;

        // Nodes
        let mut nodes = Vec::with_capacity(n);
        for _ in 0..n {
            let id = file.read_u64::<LittleEndian>()?;
            let neighbor_count = file.read_u32::<LittleEndian>()? as usize;
            let mut neighbors = Vec::with_capacity(neighbor_count);
            for _ in 0..neighbor_count {
                neighbors.push(file.read_u32::<LittleEndian>()?);
            }
            nodes.push(Node {
                id,
                neighbors: RwLock::new(neighbors),
            });
        }

        // Vectors
        let mut vectors = Vec::with_capacity(n * dimension);
        for _ in 0..(n * dimension) {
            vectors.push(file.read_f32::<LittleEndian>()?);
        }

        let config = VamanaConfig {
            dimension,
            max_degree,
            build_search_size: 128,
            alpha,
            metric,
        };

        Ok(Self {
            config,
            vectors: RwLock::new(vectors),
            nodes: RwLock::new(nodes),
            entry_point: AtomicU32::new(entry_point),
            count: AtomicUsize::new(n),
        })
    }
}

/// Wrapper for f32 to implement Ord (for BinaryHeap)
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedFloat(f32);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}
