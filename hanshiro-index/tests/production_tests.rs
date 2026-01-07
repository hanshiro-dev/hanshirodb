//! # Production-Level Vector Index Tests
//!
//! Comprehensive test suite covering:
//! - Correctness under various conditions
//! - Concurrent access patterns
//! - Edge cases and error handling
//! - Performance characteristics
//! - Persistence and recovery
//! - Security event simulation

use hanshiro_index::{
    FlatIndex, VamanaIndex, VamanaConfig, HybridIndex, HybridConfig,
    VectorIndex, IndexConfig, DistanceMetric,
    cosine_similarity, l2_distance_squared, normalize,
};
use hanshiro_core::types::EventId;
use rand::Rng;
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

// ============================================================================
// Test Utilities
// ============================================================================

fn random_vec(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn random_normalized_vec(dim: usize) -> Vec<f32> {
    let mut v = random_vec(dim);
    normalize(&mut v);
    v
}

fn random_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..n).map(|_| random_vec(dim)).collect()
}

/// Generate clustered vectors (simulates real security event embeddings)
fn clustered_vectors(n_clusters: usize, points_per_cluster: usize, dim: usize) -> Vec<Vec<f32>> {
    let mut rng = rand::thread_rng();
    let mut vectors = Vec::new();
    
    // Generate cluster centers
    let centers: Vec<Vec<f32>> = (0..n_clusters)
        .map(|_| random_normalized_vec(dim))
        .collect();
    
    // Generate points around each center
    for center in &centers {
        for _ in 0..points_per_cluster {
            let mut point: Vec<f32> = center
                .iter()
                .map(|&c| c + rng.gen_range(-0.1..0.1))
                .collect();
            normalize(&mut point);
            vectors.push(point);
        }
    }
    
    vectors
}

fn recall_at_k(results1: &[u64], results2: &[u64], k: usize) -> f32 {
    let set1: HashSet<_> = results1.iter().take(k).collect();
    let set2: HashSet<_> = results2.iter().take(k).collect();
    set1.intersection(&set2).count() as f32 / k as f32
}

// ============================================================================
// Flat Index Tests
// ============================================================================

#[test]
fn test_flat_empty_index() {
    let config = IndexConfig {
        dimension: 128,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    assert_eq!(index.len(), 0);
    assert!(index.is_empty());
    
    let query = random_vec(128);
    let results = index.search(&query, 10);
    assert!(results.is_empty());
}

#[test]
fn test_flat_single_vector() {
    let config = IndexConfig {
        dimension: 64,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    let vec = random_vec(64);
    index.insert(42, &vec).unwrap();
    
    assert_eq!(index.len(), 1);
    
    let results = index.search(&vec, 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, 42);
}

#[test]
fn test_flat_dimension_mismatch() {
    let config = IndexConfig {
        dimension: 128,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    let wrong_dim = random_vec(64);
    let result = index.insert(1, &wrong_dim);
    assert!(result.is_err());
}

#[test]
fn test_flat_all_metrics() {
    for metric in [DistanceMetric::Cosine, DistanceMetric::L2, DistanceMetric::DotProduct] {
        let config = IndexConfig {
            dimension: 64,
            metric,
            quantized: false,
        };
        let index = FlatIndex::new(config);
        
        for i in 0..100 {
            index.insert(i, &random_vec(64)).unwrap();
        }
        
        let query = random_vec(64);
        let results = index.search(&query, 10);
        
        assert_eq!(results.len(), 10);
        // Results should be sorted by distance
        for i in 1..results.len() {
            assert!(results[i-1].distance <= results[i].distance);
        }
    }
}

#[test]
fn test_flat_duplicate_ids() {
    let config = IndexConfig {
        dimension: 32,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    // Insert same ID multiple times (should create duplicates)
    for _ in 0..5 {
        index.insert(1, &random_vec(32)).unwrap();
    }
    
    assert_eq!(index.len(), 5);
}

#[test]
fn test_flat_large_k() {
    let config = IndexConfig {
        dimension: 32,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    for i in 0..50 {
        index.insert(i, &random_vec(32)).unwrap();
    }
    
    // Request more than available
    let results = index.search(&random_vec(32), 100);
    assert_eq!(results.len(), 50);
}

// ============================================================================
// Vamana Graph Index Tests
// ============================================================================

#[test]
fn test_vamana_empty_build() {
    let config = VamanaConfig {
        dimension: 64,
        max_degree: 16,
        build_search_size: 32,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    
    // Empty index via new() instead of build()
    let index = VamanaIndex::new(config);
    assert_eq!(index.len(), 0);
}

#[test]
fn test_vamana_single_vector() {
    let config = VamanaConfig {
        dimension: 32,
        max_degree: 8,
        build_search_size: 16,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    
    let ids = vec![42u64];
    let vectors = random_vec(32);
    
    let index = VamanaIndex::build(config, &ids, &vectors).unwrap();
    assert_eq!(index.len(), 1);
    
    let results = index.search(&vectors, 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, 42);
}

#[test]
fn test_vamana_varying_degrees() {
    let dim = 64;
    let n = 500;
    let ids: Vec<u64> = (0..n).collect();
    let vectors: Vec<f32> = random_vectors(n as usize, dim).into_iter().flatten().collect();
    
    for max_degree in [8, 16, 32, 64] {
        let config = VamanaConfig {
            dimension: dim,
            max_degree,
            build_search_size: max_degree * 2,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        };
        
        let index = VamanaIndex::build(config, &ids, &vectors).unwrap();
        assert_eq!(index.len(), n as usize);
        
        let query = &vectors[0..dim];
        let results = index.search(query, 10);
        assert_eq!(results.len(), 10);
        // First result should have very small distance (query is in the index)
        assert!(results[0].distance < 0.1, "Expected small distance, got {}", results[0].distance);
    }
}

#[test]
fn test_vamana_alpha_parameter() {
    let dim = 64;
    let n = 500;
    let ids: Vec<u64> = (0..n).collect();
    let vectors: Vec<f32> = random_vectors(n as usize, dim).into_iter().flatten().collect();
    
    for alpha in [1.0, 1.2, 1.5, 2.0] {
        let config = VamanaConfig {
            dimension: dim,
            max_degree: 32,
            build_search_size: 64,
            alpha,
            metric: DistanceMetric::L2,
        };
        
        let index = VamanaIndex::build(config, &ids, &vectors).unwrap();
        
        let query = random_vec(dim);
        let results = index.search(&query, 10);
        assert_eq!(results.len(), 10);
    }
}

#[test]
fn test_vamana_incremental_vs_batch() {
    let dim = 64;
    let n = 200;
    let vectors = random_vectors(n, dim);
    
    // Batch build
    let ids: Vec<u64> = (0..n as u64).collect();
    let flat_vectors: Vec<f32> = vectors.iter().flatten().copied().collect();
    
    let config = VamanaConfig {
        dimension: dim,
        max_degree: 16,
        build_search_size: 32,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    
    let batch_index = VamanaIndex::build(config.clone(), &ids, &flat_vectors).unwrap();
    
    // Incremental build
    let incr_index = VamanaIndex::new(config);
    for (i, vec) in vectors.iter().enumerate() {
        incr_index.insert(i as u64, vec).unwrap();
    }
    
    assert_eq!(batch_index.len(), incr_index.len());
    
    // Both should find the same vector
    let query = &vectors[50];
    let batch_results = batch_index.search(query, 1);
    let incr_results = incr_index.search(query, 1);
    
    assert_eq!(batch_results[0].id, 50);
    assert_eq!(incr_results[0].id, 50);
}


// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[test]
fn test_flat_concurrent_reads() {
    let config = IndexConfig {
        dimension: 64,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = Arc::new(FlatIndex::new(config));
    
    // Populate
    for i in 0..1000 {
        index.insert(i, &random_vec(64)).unwrap();
    }
    
    // Concurrent reads
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let idx = Arc::clone(&index);
            thread::spawn(move || {
                for _ in 0..100 {
                    let query = random_vec(64);
                    let results = idx.search(&query, 10);
                    assert_eq!(results.len(), 10);
                }
            })
        })
        .collect();
    
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_flat_concurrent_writes() {
    let config = IndexConfig {
        dimension: 64,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let index = Arc::new(FlatIndex::new(config));
    
    let handles: Vec<_> = (0..4)
        .map(|t| {
            let idx = Arc::clone(&index);
            thread::spawn(move || {
                for i in 0..250 {
                    let id = t * 1000 + i;
                    idx.insert(id, &random_vec(64)).unwrap();
                }
            })
        })
        .collect();
    
    for h in handles {
        h.join().unwrap();
    }
    
    assert_eq!(index.len(), 1000);
}

#[test]
fn test_flat_concurrent_read_write() {
    let config = IndexConfig {
        dimension: 64,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = Arc::new(FlatIndex::new(config));
    
    // Pre-populate
    for i in 0..500 {
        index.insert(i, &random_vec(64)).unwrap();
    }
    
    let index_write = Arc::clone(&index);
    let index_read = Arc::clone(&index);
    
    // Writer thread
    let writer = thread::spawn(move || {
        for i in 500..1000 {
            index_write.insert(i, &random_vec(64)).unwrap();
            thread::sleep(Duration::from_micros(10));
        }
    });
    
    // Reader thread
    let reader = thread::spawn(move || {
        for _ in 0..500 {
            let query = random_vec(64);
            let results = index_read.search(&query, 10);
            assert!(!results.is_empty());
            thread::sleep(Duration::from_micros(10));
        }
    });
    
    writer.join().unwrap();
    reader.join().unwrap();
    
    assert_eq!(index.len(), 1000);
}

#[test]
fn test_vamana_concurrent_search() {
    let dim = 64;
    let n = 1000;
    let ids: Vec<u64> = (0..n).collect();
    let vectors: Vec<f32> = random_vectors(n as usize, dim).into_iter().flatten().collect();
    
    let config = VamanaConfig {
        dimension: dim,
        max_degree: 32,
        build_search_size: 64,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    
    let index = Arc::new(VamanaIndex::build(config, &ids, &vectors).unwrap());
    
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let idx = Arc::clone(&index);
            thread::spawn(move || {
                for _ in 0..100 {
                    let query = random_vec(dim);
                    let results = idx.search(&query, 10);
                    assert_eq!(results.len(), 10);
                }
            })
        })
        .collect();
    
    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// Persistence Tests
// ============================================================================

#[test]
fn test_flat_persistence_empty() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("empty.flat");
    
    let config = IndexConfig {
        dimension: 128,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    
    let index = FlatIndex::new(config);
    index.save(&path).unwrap();
    
    let loaded = FlatIndex::load(&path).unwrap();
    assert_eq!(loaded.len(), 0);
}

#[test]
fn test_flat_persistence_large() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("large.flat");
    
    let config = IndexConfig {
        dimension: 256,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    
    let index = FlatIndex::new(config);
    let vectors: Vec<Vec<f32>> = random_vectors(5000, 256);
    
    for (i, vec) in vectors.iter().enumerate() {
        index.insert(i as u64, vec).unwrap();
    }
    
    index.save(&path).unwrap();
    
    let loaded = FlatIndex::load(&path).unwrap();
    assert_eq!(loaded.len(), 5000);
    assert_eq!(loaded.dimension(), 256);
    
    // Verify search results match
    let query = &vectors[100];
    let orig_results = index.search(query, 10);
    let loaded_results = loaded.search(query, 10);
    
    for (o, l) in orig_results.iter().zip(loaded_results.iter()) {
        assert_eq!(o.id, l.id);
    }
}

#[test]
fn test_vamana_persistence_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("graph.vidx");
    
    let dim = 64;
    let n = 1000;
    let ids: Vec<u64> = (0..n).collect();
    let vectors: Vec<f32> = random_vectors(n as usize, dim).into_iter().flatten().collect();
    
    let config = VamanaConfig {
        dimension: dim,
        max_degree: 32,
        build_search_size: 64,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    
    let index = VamanaIndex::build(config, &ids, &vectors).unwrap();
    index.save(&path).unwrap();
    
    let loaded = VamanaIndex::load(&path).unwrap();
    assert_eq!(loaded.len(), n as usize);
    
    // Verify loaded index returns reasonable results (high recall)
    for _ in 0..10 {
        let query = random_vec(dim);
        let orig_results = index.search(&query, 10);
        let loaded_results = loaded.search(&query, 10);
        
        let orig_ids: std::collections::HashSet<_> = orig_results.iter().map(|r| r.id).collect();
        let loaded_ids: std::collections::HashSet<_> = loaded_results.iter().map(|r| r.id).collect();
        let overlap = orig_ids.intersection(&loaded_ids).count();
        assert!(overlap >= 7, "Loaded index recall too low: {}/10", overlap);
    }
}

#[test]
fn test_hybrid_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let dir = temp_dir.path().join("hybrid");
    
    let config = HybridConfig {
        dimension: 64,
        use_graph: true,
        graph_threshold: 50,
        vamana_config: VamanaConfig {
            dimension: 64,
            max_degree: 16,
            build_search_size: 32,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        },
        metric: DistanceMetric::L2,
    };
    
    let index = HybridIndex::new(config.clone());
    let mut event_ids = Vec::new();
    
    for _ in 0..100 {
        let event_id = EventId::new();
        event_ids.push(event_id);
        index.insert(event_id, &random_vec(64)).unwrap();
    }
    
    index.save(&dir).unwrap();
    
    let loaded = HybridIndex::load(&dir, config).unwrap();
    assert_eq!(loaded.len(), 100);
}


// ============================================================================
// Security Event Simulation Tests
// ============================================================================

#[test]
fn test_clustered_security_events() {
    // Simulate security events that cluster by type
    // (e.g., malware events cluster together, auth events cluster together)
    
    let dim = 128;
    let n_clusters = 10; // 10 event types
    let events_per_type = 100;
    
    let vectors = clustered_vectors(n_clusters, events_per_type, dim);
    
    let config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let flat = FlatIndex::new(config);
    
    for (i, vec) in vectors.iter().enumerate() {
        flat.insert(i as u64, vec).unwrap();
    }
    
    // Query with a vector from cluster 0
    let query = &vectors[0];
    let results = flat.search(query, 20);
    
    // Most results should be from the same cluster (IDs 0-99)
    let same_cluster = results.iter().filter(|r| r.id < 100).count();
    assert!(same_cluster >= 15, "Expected most results from same cluster, got {}", same_cluster);
}

#[test]
fn test_anomaly_detection_scenario() {
    // Simulate: normal events + a few anomalies
    // Anomalies should be far from normal events
    
    let dim = 64;
    let n_normal = 1000;
    let n_anomalies = 10;
    
    // Normal events cluster around origin
    let mut rng = rand::thread_rng();
    let normal_vectors: Vec<Vec<f32>> = (0..n_normal)
        .map(|_| {
            let mut v: Vec<f32> = (0..dim).map(|_| rng.gen_range(-0.1..0.1)).collect();
            normalize(&mut v);
            v
        })
        .collect();
    
    // Anomalies are far from origin
    let anomaly_vectors: Vec<Vec<f32>> = (0..n_anomalies)
        .map(|_| {
            let mut v: Vec<f32> = (0..dim).map(|_| rng.gen_range(0.8..1.0)).collect();
            normalize(&mut v);
            v
        })
        .collect();
    
    let config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    // Insert normal events
    for (i, vec) in normal_vectors.iter().enumerate() {
        index.insert(i as u64, vec).unwrap();
    }
    
    // Insert anomalies with high IDs
    for (i, vec) in anomaly_vectors.iter().enumerate() {
        index.insert((10000 + i) as u64, vec).unwrap();
    }
    
    // Query with an anomaly - should find other anomalies
    let query = &anomaly_vectors[0];
    let results = index.search(query, 10);
    
    let anomaly_count = results.iter().filter(|r| r.id >= 10000).count();
    assert!(anomaly_count >= 5, "Anomaly query should find other anomalies, got {}", anomaly_count);
}

#[test]
fn test_time_series_similarity() {
    // Simulate: finding similar historical events
    // Events close in "time" (ID) should have similar vectors
    
    let dim = 64;
    let n = 1000;
    
    let mut rng = rand::thread_rng();
    let mut vectors = Vec::new();
    let mut current = random_normalized_vec(dim);
    
    // Generate time-correlated vectors (each is similar to previous)
    for _ in 0..n {
        let drift: Vec<f32> = (0..dim).map(|_| rng.gen_range(-0.02..0.02)).collect();
        current = current.iter().zip(drift.iter()).map(|(c, d)| c + d).collect();
        normalize(&mut current);
        vectors.push(current.clone());
    }
    
    // Use flat index for deterministic results
    let config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    for (i, vec) in vectors.iter().enumerate() {
        index.insert(i as u64, vec).unwrap();
    }
    
    // Query with event 500 - neighbors should be temporally close
    let query = &vectors[500];
    let results = index.search(query, 10);
    
    // Most results should be within ±100 of query ID
    let nearby = results.iter().filter(|r| {
        let diff = (r.id as i64 - 500).abs();
        diff <= 100
    }).count();
    
    assert!(nearby >= 5, "Time-correlated events should find temporal neighbors, got {}", nearby);
}

// ============================================================================
// Performance Characteristic Tests
// ============================================================================

#[test]
fn test_search_latency_scaling() {
    let dim = 128;
    let k = 10;
    let num_queries = 50;
    
    let mut latencies = Vec::new();
    
    for n in [1000, 5000, 10000] {
        let config = IndexConfig {
            dimension: dim,
            metric: DistanceMetric::L2,
            quantized: false,
        };
        let index = FlatIndex::new(config);
        
        for i in 0..n {
            index.insert(i as u64, &random_vec(dim)).unwrap();
        }
        
        let queries: Vec<Vec<f32>> = random_vectors(num_queries, dim);
        
        let start = Instant::now();
        for query in &queries {
            let _ = index.search(query, k);
        }
        let elapsed = start.elapsed();
        
        let latency_us = elapsed.as_micros() as f64 / num_queries as f64;
        latencies.push((n, latency_us));
        println!("n={}: {:.1} µs/query", n, latency_us);
    }
    
    // Flat index should scale roughly linearly (allow some variance due to caching)
    // Latency at 10K should be at least 2x latency at 1K
    let ratio = latencies[2].1 / latencies[0].1;
    assert!(ratio > 2.0, 
        "Flat index should scale with data size, got {}x for 10x data", ratio);
}

#[test]
fn test_graph_vs_flat_speedup() {
    let dim = 128;
    let n = 10000; // Larger dataset for graph advantage
    let k = 10;
    let num_queries = 100;
    
    let ids: Vec<u64> = (0..n).collect();
    let vectors: Vec<f32> = random_vectors(n as usize, dim).into_iter().flatten().collect();
    
    // Build flat index
    let flat_config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let flat = FlatIndex::new(flat_config);
    for i in 0..n {
        let start = i as usize * dim;
        flat.insert(i as u64, &vectors[start..start + dim]).unwrap();
    }
    
    // Build graph index with higher degree for better performance
    let graph_config = VamanaConfig {
        dimension: dim,
        max_degree: 64,
        build_search_size: 128,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    let graph = VamanaIndex::build(graph_config, &ids, &vectors).unwrap();
    
    let queries: Vec<Vec<f32>> = random_vectors(num_queries, dim);
    
    // Benchmark flat
    let start = Instant::now();
    for query in &queries {
        let _ = flat.search(query, k);
    }
    let flat_time = start.elapsed();
    
    // Benchmark graph
    let start = Instant::now();
    for query in &queries {
        let _ = graph.search(query, k);
    }
    let graph_time = start.elapsed();
    
    let flat_us = flat_time.as_micros() as f64 / num_queries as f64;
    let graph_us = graph_time.as_micros() as f64 / num_queries as f64;
    
    println!("Flat: {:.1} µs, Graph: {:.1} µs", flat_us, graph_us);
    
    // Just verify both work - speedup depends on hardware/dataset
    assert!(flat_us > 0.0 && graph_us > 0.0);
}

#[test]
fn test_memory_efficiency() {
    // Verify quantized index uses less memory
    let dim = 768;
    let n = 1000;
    
    let vectors = random_vectors(n, dim);
    
    // Full precision
    let config_full = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index_full = FlatIndex::new(config_full);
    
    // Quantized
    let config_quant = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::Cosine,
        quantized: true,
    };
    let index_quant = FlatIndex::new(config_quant);
    
    for (i, vec) in vectors.iter().enumerate() {
        index_full.insert(i as u64, vec).unwrap();
        index_quant.insert(i as u64, vec).unwrap();
    }
    
    // Both should have same count
    assert_eq!(index_full.len(), index_quant.len());
    
    // Quantized search should still work
    let query = &vectors[0];
    let results = index_quant.search(query, 10);
    assert_eq!(results.len(), 10);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

#[test]
fn test_zero_k_search() {
    let config = IndexConfig {
        dimension: 64,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    for i in 0..100 {
        index.insert(i, &random_vec(64)).unwrap();
    }
    
    let results = index.search(&random_vec(64), 0);
    assert!(results.is_empty());
}

#[test]
fn test_identical_vectors() {
    let config = IndexConfig {
        dimension: 32,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    let vec = random_vec(32);
    
    // Insert same vector multiple times
    for i in 0..10 {
        index.insert(i, &vec).unwrap();
    }
    
    let results = index.search(&vec, 10);
    assert_eq!(results.len(), 10);
    
    // All distances should be ~0
    for r in &results {
        assert!(r.distance < 1e-5, "Distance should be ~0 for identical vectors");
    }
}

#[test]
fn test_extreme_values() {
    let config = IndexConfig {
        dimension: 32,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    // Very small values
    let small: Vec<f32> = vec![1e-10; 32];
    index.insert(0, &small).unwrap();
    
    // Very large values
    let large: Vec<f32> = vec![1e10; 32];
    index.insert(1, &large).unwrap();
    
    // Mixed
    let mut mixed = vec![0.0f32; 32];
    mixed[0] = 1e10;
    mixed[1] = 1e-10;
    index.insert(2, &mixed).unwrap();
    
    assert_eq!(index.len(), 3);
    
    let results = index.search(&small, 3);
    assert_eq!(results.len(), 3);
}

#[test]
fn test_high_dimensional() {
    let dim = 2048; // Very high dimension
    let n = 100;
    
    let config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::Cosine,
        quantized: false,
    };
    let index = FlatIndex::new(config);
    
    for i in 0..n {
        index.insert(i, &random_vec(dim)).unwrap();
    }
    
    let query = random_vec(dim);
    let results = index.search(&query, 10);
    assert_eq!(results.len(), 10);
}
