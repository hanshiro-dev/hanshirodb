//! # Hybrid Index
//!
//! Combines vector index with event storage for the LSM pipeline.
//! 
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Hybrid Index                              │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │  Write Path:                                                 │
//! │  Event ──► Extract Vector ──► Insert into Graph Index        │
//! │                │                                             │
//! │                └──► Store Event ID mapping                   │
//! │                                                              │
//! │  Search Path:                                                │
//! │  Query Vector ──► Graph Search ──► Return Event IDs          │
//! │                                                              │
//! │  Flush Path:                                                 │
//! │  MemTable Flush ──► Serialize Graph to .vidx sidecar         │
//! │                                                              │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use hanshiro_core::error::{Error, Result};
use hanshiro_core::types::EventId;

use crate::vamana::{VamanaConfig, VamanaIndex};
use crate::flat::FlatIndex;
use crate::traits::{IndexConfig, SearchResult, VectorIndex};
use crate::simd::DistanceMetric;

/// Hybrid index configuration
#[derive(Debug, Clone)]
pub struct HybridConfig {
    /// Vector dimension
    pub dimension: usize,
    /// Use graph index (true) or flat index (false)
    pub use_graph: bool,
    /// Vamana config (if use_graph)
    pub vamana_config: VamanaConfig,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Threshold for switching from flat to graph (number of vectors)
    pub graph_threshold: usize,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            dimension: 768,
            use_graph: true,
            vamana_config: VamanaConfig::default(),
            metric: DistanceMetric::Cosine,
            graph_threshold: 1000, // Build graph after 1000 vectors
        }
    }
}

/// Search result with event ID
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub event_id: EventId,
    pub distance: f32,
    pub score: f32, // 1.0 - distance for similarity
}

/// Hybrid index for MemTable integration
pub struct HybridIndex {
    config: HybridConfig,
    /// Flat index (always maintained for small datasets / ground truth)
    flat: Arc<FlatIndex>,
    /// Graph index (built when threshold reached)
    graph: RwLock<Option<Arc<VamanaIndex>>>,
    /// Event ID to internal index mapping
    id_map: RwLock<Vec<EventId>>,
}

impl HybridIndex {
    pub fn new(config: HybridConfig) -> Self {
        let flat_config = IndexConfig {
            dimension: config.dimension,
            metric: config.metric,
            quantized: false,
        };

        Self {
            config,
            flat: Arc::new(FlatIndex::new(flat_config)),
            graph: RwLock::new(None),
            id_map: RwLock::new(Vec::new()),
        }
    }

    /// Insert event vector
    pub fn insert(&self, event_id: EventId, vector: &[f32]) -> Result<()> {
        if vector.len() != self.config.dimension {
            return Err(Error::Internal {
                message: format!(
                    "Vector dimension mismatch: expected {}, got {}",
                    self.config.dimension,
                    vector.len()
                ),
            });
        }

        // Get internal index and insert into flat in one go
        let internal_id = {
            let mut id_map = self.id_map.write();
            let idx = id_map.len() as u64;
            id_map.push(event_id);
            idx
        };

        // Insert into flat index
        self.flat.insert(internal_id, vector)?;

        // Check if we should build/update graph (less frequent path)
        let n = self.flat.len();
        if self.config.use_graph && n >= self.config.graph_threshold {
            // Rebuild graph periodically (every 2x threshold)
            if n == self.config.graph_threshold || n % (self.config.graph_threshold * 2) == 0 {
                self.rebuild_graph()?;
            } else {
                // Incremental insert - only take read lock if graph exists
                let graph = self.graph.read();
                if let Some(ref g) = *graph {
                    g.insert(internal_id, vector)?;
                }
            }
        }

        Ok(())
    }

    /// Search for similar vectors
    pub fn search(&self, query: &[f32], k: usize) -> Vec<VectorSearchResult> {
        let results = if self.config.use_graph {
            if let Some(ref graph) = *self.graph.read() {
                graph.search(query, k)
            } else {
                self.flat.search(query, k)
            }
        } else {
            self.flat.search(query, k)
        };

        let id_map = self.id_map.read();
        results
            .into_iter()
            .filter_map(|r| {
                id_map.get(r.id as usize).map(|&event_id| VectorSearchResult {
                    event_id,
                    distance: r.distance,
                    score: 1.0 - r.distance.min(1.0),
                })
            })
            .collect()
    }

    /// Rebuild graph index from flat index
    fn rebuild_graph(&self) -> Result<()> {
        let n = self.flat.len();
        if n == 0 {
            return Ok(());
        }

        // Collect all vectors and IDs
        let ids: Vec<u64> = (0..n as u64).collect();
        let mut vectors = Vec::with_capacity(n * self.config.dimension);
        
        for i in 0..n {
            if let Some(vec) = self.flat.get(i as u64) {
                vectors.extend(vec);
            }
        }

        let mut vamana_config = self.config.vamana_config.clone();
        vamana_config.dimension = self.config.dimension;
        vamana_config.metric = self.config.metric;

        let graph = VamanaIndex::build(vamana_config, &ids, &vectors)?;
        *self.graph.write() = Some(Arc::new(graph));

        Ok(())
    }

    /// Get number of indexed vectors
    pub fn len(&self) -> usize {
        self.flat.len()
    }

    pub fn is_empty(&self) -> bool {
        self.flat.is_empty()
    }

    /// Save index to directory (creates .flat and .vidx files)
    pub fn save(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;

        // Save flat index
        self.flat.save(&dir.join("vectors.flat"))?;

        // Save graph if exists
        if let Some(ref graph) = *self.graph.read() {
            graph.save(&dir.join("vectors.vidx"))?;
        }

        // Save ID map
        let id_map = self.id_map.read();
        let id_bytes: Vec<u8> = id_map
            .iter()
            .flat_map(|id| id.to_uuid().as_bytes().to_vec())
            .collect();
        std::fs::write(dir.join("id_map.bin"), &id_bytes)?;

        Ok(())
    }

    /// Load index from directory
    pub fn load(dir: &Path, config: HybridConfig) -> Result<Self> {
        let flat_path = dir.join("vectors.flat");
        let graph_path = dir.join("vectors.vidx");
        let id_map_path = dir.join("id_map.bin");

        let flat = Arc::new(FlatIndex::load(&flat_path)?);

        let graph = if graph_path.exists() {
            Some(Arc::new(VamanaIndex::load(&graph_path)?))
        } else {
            None
        };

        // Load ID map
        let id_bytes = std::fs::read(&id_map_path)?;
        let mut id_map = Vec::new();
        for chunk in id_bytes.chunks(16) {
            if chunk.len() == 16 {
                let uuid = uuid::Uuid::from_slice(chunk)
                    .map_err(|e| Error::Internal { message: e.to_string() })?;
                let (hi, lo) = uuid.as_u64_pair();
                id_map.push(EventId { hi, lo });
            }
        }

        Ok(Self {
            config,
            flat,
            graph: RwLock::new(graph),
            id_map: RwLock::new(id_map),
        })
    }
}
