//! # Auto-Correlation Engine
//!
//! Automatically establishes relationships between logs and code artifacts
//! based on vector similarity and metadata matching.
//!
//! ## Correlation Strategies
//!
//! 1. **Vector Similarity**: If embeddings are similar, entities are related
//! 2. **Hash Matching**: If a log mentions a hash that matches a code artifact
//! 3. **Filename Matching**: If a log mentions a filename from a code artifact

use std::collections::HashMap;

use hanshiro_core::{
    error::Result,
    Event, EventId, CodeArtifact, IndexEntry, HanshiroValue,
};

/// Correlation result between two entities
#[derive(Debug, Clone)]
pub struct Correlation {
    pub source_id: EventId,
    pub target_id: EventId,
    pub correlation_type: CorrelationType,
    pub score: f32,
}

/// Type of correlation detected
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorrelationType {
    /// Vector embeddings are similar
    VectorSimilarity,
    /// Hash in log matches code artifact hash
    HashMatch,
    /// Filename in log matches code artifact
    FilenameMatch,
    /// IP address correlation
    IpMatch,
    /// User-defined relationship
    Manual,
}

/// Configuration for auto-correlation
#[derive(Debug, Clone)]
pub struct CorrelationConfig {
    /// Minimum vector similarity score (0.0-1.0) to create correlation
    pub vector_similarity_threshold: f32,
    /// Enable hash-based correlation
    pub hash_matching: bool,
    /// Enable filename-based correlation
    pub filename_matching: bool,
    /// Maximum correlations per entity
    pub max_correlations_per_entity: usize,
}

impl Default for CorrelationConfig {
    fn default() -> Self {
        Self {
            vector_similarity_threshold: 0.85,
            hash_matching: true,
            filename_matching: true,
            max_correlations_per_entity: 100,
        }
    }
}

/// Auto-correlation engine
pub struct CorrelationEngine {
    config: CorrelationConfig,
}

impl CorrelationEngine {
    pub fn new(config: CorrelationConfig) -> Self {
        Self { config }
    }

    /// Find correlations for a new log entry against existing code artifacts
    pub fn correlate_log_to_code(
        &self,
        log: &Event,
        code_artifacts: &[CodeArtifact],
    ) -> Vec<Correlation> {
        let mut correlations = Vec::new();

        for artifact in code_artifacts {
            // 1. Vector similarity
            if let (Some(log_vec), Some(code_vec)) = (&log.vector, &artifact.vector) {
                let similarity = cosine_similarity(&log_vec.data, &code_vec.data);
                if similarity >= self.config.vector_similarity_threshold {
                    correlations.push(Correlation {
                        source_id: log.id,
                        target_id: artifact.id,
                        correlation_type: CorrelationType::VectorSimilarity,
                        score: similarity,
                    });
                }
            }

            // 2. Hash matching - check if log mentions the artifact's hash
            if self.config.hash_matching {
                let hash_hex = artifact.hash_hex();
                let hash_short = &hash_hex[..16.min(hash_hex.len())]; // First 16 chars

                // Check raw_data and metadata for hash
                let raw_str = String::from_utf8_lossy(&log.raw_data);
                if raw_str.contains(&hash_hex) || raw_str.contains(hash_short) {
                    correlations.push(Correlation {
                        source_id: log.id,
                        target_id: artifact.id,
                        correlation_type: CorrelationType::HashMatch,
                        score: 1.0,
                    });
                } else if log.metadata_json.contains(&hash_hex)
                    || log.metadata_json.contains(hash_short)
                {
                    correlations.push(Correlation {
                        source_id: log.id,
                        target_id: artifact.id,
                        correlation_type: CorrelationType::HashMatch,
                        score: 1.0,
                    });
                }
            }

            // 3. Filename matching - check strings in artifact against log
            if self.config.filename_matching {
                for filename in &artifact.strings {
                    // Only match reasonable filenames (not too short)
                    if filename.len() >= 5 {
                        let raw_str = String::from_utf8_lossy(&log.raw_data);
                        if raw_str.contains(filename) || log.metadata_json.contains(filename) {
                            correlations.push(Correlation {
                                source_id: log.id,
                                target_id: artifact.id,
                                correlation_type: CorrelationType::FilenameMatch,
                                score: 0.9,
                            });
                            break; // One filename match is enough
                        }
                    }
                }
            }
        }

        // Deduplicate and limit
        self.deduplicate_correlations(correlations)
    }

    /// Find correlations for a new code artifact against existing logs
    pub fn correlate_code_to_logs(
        &self,
        artifact: &CodeArtifact,
        logs: &[Event],
    ) -> Vec<Correlation> {
        let mut correlations = Vec::new();

        for log in logs {
            // Reuse the same logic but swap source/target
            let log_correlations = self.correlate_log_to_code(log, &[artifact.clone()]);
            for mut corr in log_correlations {
                // Swap direction: code -> log
                std::mem::swap(&mut corr.source_id, &mut corr.target_id);
                correlations.push(corr);
            }
        }

        self.deduplicate_correlations(correlations)
    }

    /// Find similar entities by vector
    pub fn find_similar_by_vector(
        &self,
        query_vector: &[f32],
        candidates: &[(EventId, Vec<f32>)],
        top_k: usize,
    ) -> Vec<(EventId, f32)> {
        let mut scores: Vec<(EventId, f32)> = candidates
            .iter()
            .map(|(id, vec)| (*id, cosine_similarity(query_vector, vec)))
            .filter(|(_, score)| *score >= self.config.vector_similarity_threshold)
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(top_k);
        scores
    }

    /// Build index entries from correlations
    pub fn build_index_entries(&self, correlations: &[Correlation]) -> Vec<IndexEntry> {
        // Group by target (code artifact)
        let mut by_target: HashMap<EventId, Vec<EventId>> = HashMap::new();

        for corr in correlations {
            by_target
                .entry(corr.target_id)
                .or_default()
                .push(corr.source_id);
        }

        by_target
            .into_iter()
            .map(|(target_id, source_ids)| {
                let mut entry = IndexEntry::new("correlated_to", format!("{}", target_id));
                for id in source_ids {
                    entry.add_ref(id);
                }
                entry
            })
            .collect()
    }

    fn deduplicate_correlations(&self, mut correlations: Vec<Correlation>) -> Vec<Correlation> {
        // Sort by score descending
        correlations.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Deduplicate by (source, target) pair, keeping highest score
        let mut seen = std::collections::HashSet::new();
        correlations.retain(|c| {
            let key = (c.source_id, c.target_id);
            seen.insert(key)
        });

        // Limit per entity
        correlations.truncate(self.config.max_correlations_per_entity);
        correlations
    }
}

/// Cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot / (norm_a * norm_b)
}
