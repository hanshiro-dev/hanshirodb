//! # Vector Index Compaction
//!
//! Merges vector indices during SSTable compaction.
//!
//! ## Design
//!
//! Each SSTable has a sidecar `.vidx` file containing a Vamana graph.
//! When SSTables are compacted, we must also merge their vector indices:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  Vector Index Compaction                     │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │  Input:                                                      │
//! │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │
//! │  │ 1.vidx  │ │ 2.vidx  │ │ 3.vidx  │ │ 4.vidx  │            │
//! │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘            │
//! │       │           │           │           │                  │
//! │       └───────────┴─────┬─────┴───────────┘                  │
//! │                         │                                    │
//! │                         ▼                                    │
//! │              ┌─────────────────────┐                         │
//! │              │  Collect Vectors    │                         │
//! │              │  Filter by LiveSet  │                         │
//! │              └──────────┬──────────┘                         │
//! │                         │                                    │
//! │                         ▼                                    │
//! │              ┌─────────────────────┐                         │
//! │              │  Build Fresh Graph  │  (spawn_blocking)       │
//! │              └──────────┬──────────┘                         │
//! │                         │                                    │
//! │                         ▼                                    │
//! │                   ┌──────────┐                               │
//! │                   │ 5.vidx   │                               │
//! │                   └──────────┘                               │
//! │                                                              │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Invariants
//!
//! 1. Never modify existing `.vidx` files (append-only philosophy)
//! 2. Graph build is CPU-intensive - always use `spawn_blocking`
//! 3. If no vectors exist in merged output, skip `.vidx` creation

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

use hanshiro_core::error::{Error, Result};

use crate::vamana::{VamanaConfig, VamanaIndex};
use crate::traits::VectorIndex;
use crate::simd::DistanceMetric;

/// Configuration for vector index compaction
#[derive(Debug, Clone)]
pub struct VectorCompactionConfig {
    /// Vector dimension (must match all input indices)
    pub dimension: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Vamana graph parameters
    pub max_degree: usize,
    pub build_search_size: usize,
    pub alpha: f32,
    /// Minimum vectors to build graph (below this, skip .vidx)
    pub min_vectors_for_graph: usize,
}

impl Default for VectorCompactionConfig {
    fn default() -> Self {
        Self {
            dimension: 768,
            metric: DistanceMetric::Cosine,
            max_degree: 64,
            build_search_size: 128,
            alpha: 1.2,
            min_vectors_for_graph: 100,
        }
    }
}

/// Result of vector index compaction
#[derive(Debug)]
pub struct VectorCompactionResult {
    /// Path to output .vidx (None if no vectors)
    pub output_path: Option<PathBuf>,
    /// Number of vectors in merged index
    pub vector_count: usize,
    /// Input .vidx files that were merged
    pub input_paths: Vec<PathBuf>,
}

/// Compacts multiple vector indices into one
pub struct VectorIndexCompactor {
    config: VectorCompactionConfig,
}

impl VectorIndexCompactor {
    pub fn new(config: VectorCompactionConfig) -> Self {
        Self { config }
    }

    /// Compact multiple .vidx files into a single output.
    ///
    /// # Arguments
    /// * `input_paths` - Paths to input .vidx files (may not all exist)
    /// * `output_path` - Path for output .vidx file
    /// * `live_ids` - Set of vector IDs that survived SSTable compaction
    ///
    /// # Returns
    /// `VectorCompactionResult` with output path (None if no vectors)
    pub fn compact(
        &self,
        input_paths: &[PathBuf],
        output_path: &Path,
        live_ids: &HashSet<u64>,
    ) -> Result<VectorCompactionResult> {
        // Collect all vectors from input indices, filtering by live set
        let mut all_ids = Vec::new();
        let mut all_vectors = Vec::new();
        let mut processed_paths = Vec::new();

        for path in input_paths {
            if !path.exists() {
                debug!("Skipping non-existent .vidx: {:?}", path);
                continue;
            }

            match VamanaIndex::load(path) {
                Ok(index) => {
                    // Use iter_entries to get all (id, vector) pairs
                    for (id, vec) in index.iter_entries() {
                        if live_ids.contains(&id) {
                            all_ids.push(id);
                            all_vectors.extend(vec);
                        }
                    }
                    processed_paths.push(path.clone());
                    debug!("Loaded {} vectors from {:?}", index.len(), path);
                }
                Err(e) => {
                    warn!("Failed to load .vidx {:?}: {:?}", path, e);
                }
            }
        }

        let vector_count = all_ids.len();

        // Skip if no vectors or below threshold
        if vector_count < self.config.min_vectors_for_graph {
            info!(
                "Skipping .vidx creation: {} vectors below threshold {}",
                vector_count, self.config.min_vectors_for_graph
            );
            return Ok(VectorCompactionResult {
                output_path: None,
                vector_count,
                input_paths: processed_paths,
            });
        }

        // Build fresh Vamana graph
        let vamana_config = VamanaConfig {
            dimension: self.config.dimension,
            max_degree: self.config.max_degree,
            build_search_size: self.config.build_search_size,
            alpha: self.config.alpha,
            metric: self.config.metric,
        };

        info!(
            "Building compacted vector index: {} vectors -> {:?}",
            vector_count, output_path
        );

        let graph = VamanaIndex::build(vamana_config, &all_ids, &all_vectors)?;
        graph.save(output_path)?;

        info!(
            "Vector compaction complete: {} vectors from {} inputs",
            vector_count,
            processed_paths.len()
        );

        Ok(VectorCompactionResult {
            output_path: Some(output_path.to_path_buf()),
            vector_count,
            input_paths: processed_paths,
        })
    }

    /// Compact by extracting vectors directly from SSTable values.
    /// Use this when .vidx files don't exist or are corrupted.
    pub fn compact_from_vectors(
        &self,
        ids: &[u64],
        vectors: &[f32],
        output_path: &Path,
    ) -> Result<VectorCompactionResult> {
        let vector_count = ids.len();

        if vectors.len() != vector_count * self.config.dimension {
            return Err(Error::Internal {
                message: format!(
                    "Vector data mismatch: {} ids, {} floats, expected {}",
                    vector_count,
                    vectors.len(),
                    vector_count * self.config.dimension
                ),
            });
        }

        if vector_count < self.config.min_vectors_for_graph {
            return Ok(VectorCompactionResult {
                output_path: None,
                vector_count,
                input_paths: vec![],
            });
        }

        let vamana_config = VamanaConfig {
            dimension: self.config.dimension,
            max_degree: self.config.max_degree,
            build_search_size: self.config.build_search_size,
            alpha: self.config.alpha,
            metric: self.config.metric,
        };

        let graph = VamanaIndex::build(vamana_config, ids, vectors)?;
        graph.save(output_path)?;

        Ok(VectorCompactionResult {
            output_path: Some(output_path.to_path_buf()),
            vector_count,
            input_paths: vec![],
        })
    }
}

/// Delete old .vidx files after successful compaction
pub fn cleanup_vidx_files(paths: &[PathBuf]) -> Result<()> {
    for path in paths {
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| Error::Io {
                message: format!("Failed to delete .vidx: {:?}", path),
                source: e,
            })?;
            debug!("Deleted compacted .vidx: {:?}", path);
        }
    }
    Ok(())
}

/// Get .vidx path for an SSTable path
#[inline]
pub fn vidx_path_for_sstable(sstable_path: &Path) -> PathBuf {
    sstable_path.with_extension("vidx")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn random_vectors(n: usize, dim: usize) -> Vec<f32> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..n * dim).map(|_| rng.gen::<f32>()).collect()
    }

    #[test]
    fn test_compact_from_vectors() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("merged.vidx");

        let config = VectorCompactionConfig {
            dimension: 128,
            min_vectors_for_graph: 10,
            ..Default::default()
        };

        let compactor = VectorIndexCompactor::new(config);

        let ids: Vec<u64> = (0..100).collect();
        let vectors = random_vectors(100, 128);

        let result = compactor
            .compact_from_vectors(&ids, &vectors, &output_path)
            .unwrap();

        assert!(result.output_path.is_some());
        assert_eq!(result.vector_count, 100);
        assert!(output_path.exists());

        // Verify we can load it back
        let loaded = VamanaIndex::load(&output_path).unwrap();
        assert_eq!(loaded.len(), 100);
    }

    #[test]
    fn test_skip_below_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("merged.vidx");

        let config = VectorCompactionConfig {
            dimension: 128,
            min_vectors_for_graph: 100, // High threshold
            ..Default::default()
        };

        let compactor = VectorIndexCompactor::new(config);

        let ids: Vec<u64> = (0..10).collect(); // Only 10 vectors
        let vectors = random_vectors(10, 128);

        let result = compactor
            .compact_from_vectors(&ids, &vectors, &output_path)
            .unwrap();

        assert!(result.output_path.is_none());
        assert!(!output_path.exists());
    }

    #[test]
    fn test_vidx_path_for_sstable() {
        let sst_path = PathBuf::from("/data/sstables/123_456.sst");
        let vidx = vidx_path_for_sstable(&sst_path);
        assert_eq!(vidx, PathBuf::from("/data/sstables/123_456.vidx"));
    }
}
