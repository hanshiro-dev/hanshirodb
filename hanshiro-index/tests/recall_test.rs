//! # Recall Tests
//!
//! Compare Graph Index (Vamana) against Flat Index (ground truth).
//! Metric: Recall@K = |Graph results ∩ Flat results| / K

use hanshiro_index::{
    FlatIndex, VamanaIndex, VamanaConfig, VectorIndex,
    IndexConfig, DistanceMetric,
};
use rand::Rng;
use std::collections::HashSet;

fn random_vectors(n: usize, dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..n * dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn recall_at_k(graph_results: &[u64], flat_results: &[u64], k: usize) -> f32 {
    let graph_set: HashSet<_> = graph_results.iter().take(k).collect();
    let flat_set: HashSet<_> = flat_results.iter().take(k).collect();
    let intersection = graph_set.intersection(&flat_set).count();
    intersection as f32 / k as f32
}

#[test]
fn test_recall_10k_vectors() {
    let n = 10_000;
    let dim = 128;
    let k = 10;
    let num_queries = 100;

    println!("\n=== Recall Test: {} vectors, dim={}, k={} ===\n", n, dim, k);

    // Generate data
    let ids: Vec<u64> = (0..n as u64).collect();
    let vectors = random_vectors(n, dim);

    // Build Flat Index (ground truth)
    let flat_config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let flat_index = FlatIndex::new(flat_config);
    for i in 0..n {
        let start = i * dim;
        flat_index.insert(ids[i], &vectors[start..start + dim]).unwrap();
    }
    println!("Flat index built: {} vectors", flat_index.len());

    // Build Vamana Index
    let vamana_config = VamanaConfig {
        dimension: dim,
        max_degree: 64,        // Increased from 32
        build_search_size: 128, // Increased from 64
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    let vamana_index = VamanaIndex::build(vamana_config, &ids, &vectors).unwrap();
    println!("Vamana index built: {} vectors", vamana_index.len());

    // Run queries
    let mut total_recall = 0.0f32;
    let query_vectors = random_vectors(num_queries, dim);

    for q in 0..num_queries {
        let query = &query_vectors[q * dim..(q + 1) * dim];

        let flat_results: Vec<u64> = flat_index.search(query, k).iter().map(|r| r.id).collect();
        let vamana_results: Vec<u64> = vamana_index.search(query, k).iter().map(|r| r.id).collect();

        let recall = recall_at_k(&vamana_results, &flat_results, k);
        total_recall += recall;
    }

    let avg_recall = total_recall / num_queries as f32;
    println!("\nRecall@{}: {:.1}%", k, avg_recall * 100.0);
    println!("({} queries)", num_queries);

    // Recall should be at least 70% for a reasonable graph
    assert!(
        avg_recall >= 0.7,
        "Recall too low: {:.1}% (expected >= 70%)",
        avg_recall * 100.0
    );

    println!("\n=== RECALL TEST PASSED ===\n");
}

#[test]
fn test_recall_with_exact_match() {
    let n = 1000;
    let dim = 64;
    let k = 10;

    let ids: Vec<u64> = (0..n as u64).collect();
    let vectors = random_vectors(n, dim);

    // Build indices
    let flat_config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let flat_index = FlatIndex::new(flat_config);
    for i in 0..n {
        let start = i * dim;
        flat_index.insert(ids[i], &vectors[start..start + dim]).unwrap();
    }

    let vamana_config = VamanaConfig {
        dimension: dim,
        max_degree: 32,
        build_search_size: 64,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    let vamana_index = VamanaIndex::build(vamana_config, &ids, &vectors).unwrap();

    // Query with an existing vector - should find exact match
    let query_idx = 42;
    let query = &vectors[query_idx * dim..(query_idx + 1) * dim];

    let flat_results = flat_index.search(query, k);
    let vamana_results = vamana_index.search(query, k);

    // Both should return the query vector as first result
    assert_eq!(flat_results[0].id, query_idx as u64, "Flat didn't find exact match");
    assert_eq!(vamana_results[0].id, query_idx as u64, "Vamana didn't find exact match");
    
    // Distance should be ~0
    assert!(flat_results[0].distance < 1e-5, "Flat distance not zero");
    assert!(vamana_results[0].distance < 1e-5, "Vamana distance not zero");
}

#[test]
fn test_search_latency() {
    let n = 10_000;
    let dim = 128;
    let k = 10;
    let num_queries = 100;

    let ids: Vec<u64> = (0..n as u64).collect();
    let vectors = random_vectors(n, dim);

    // Build indices
    let flat_config = IndexConfig {
        dimension: dim,
        metric: DistanceMetric::L2,
        quantized: false,
    };
    let flat_index = FlatIndex::new(flat_config);
    for i in 0..n {
        let start = i * dim;
        flat_index.insert(ids[i], &vectors[start..start + dim]).unwrap();
    }

    let vamana_config = VamanaConfig {
        dimension: dim,
        max_degree: 32,
        build_search_size: 64,
        alpha: 1.2,
        metric: DistanceMetric::L2,
    };
    let vamana_index = VamanaIndex::build(vamana_config, &ids, &vectors).unwrap();

    let query_vectors = random_vectors(num_queries, dim);

    // Benchmark Flat
    let start = std::time::Instant::now();
    for q in 0..num_queries {
        let query = &query_vectors[q * dim..(q + 1) * dim];
        let _ = flat_index.search(query, k);
    }
    let flat_time = start.elapsed();

    // Benchmark Vamana
    let start = std::time::Instant::now();
    for q in 0..num_queries {
        let query = &query_vectors[q * dim..(q + 1) * dim];
        let _ = vamana_index.search(query, k);
    }
    let vamana_time = start.elapsed();

    let flat_latency_us = flat_time.as_micros() as f64 / num_queries as f64;
    let vamana_latency_us = vamana_time.as_micros() as f64 / num_queries as f64;
    let speedup = flat_latency_us / vamana_latency_us;

    println!("\n=== Search Latency ({} vectors, {} queries) ===", n, num_queries);
    println!("Flat:   {:.1} µs/query", flat_latency_us);
    println!("Vamana: {:.1} µs/query", vamana_latency_us);
    println!("Speedup: {:.1}x", speedup);

    // Vamana should be faster than flat for 10K vectors
    assert!(
        speedup > 1.0,
        "Vamana should be faster than Flat for {} vectors",
        n
    );
}
