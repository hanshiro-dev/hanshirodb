//! Unit tests extracted from lib modules + deadlock detection

use hanshiro_index::{
    cosine_similarity, dot_product, dot_product_sq8, l2_distance, normalize, quantize_sq8,
    dequantize_sq8, DistanceMetric, FlatIndex, HybridIndex, VamanaConfig, VamanaIndex,
    IndexConfig, VectorIndex,
};
use hanshiro_index::hybrid::HybridConfig;
use hanshiro_core::types::EventId;
use rand::Rng;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn random_vec(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

// ============================================================================
// SIMD tests
// ============================================================================

#[test]
fn test_dot_product_correctness() {
    let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
    let b = vec![8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];
    let expected: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let result = dot_product(&a, &b);
    assert!((result - expected).abs() < 1e-5);
}

#[test]
fn test_dot_product_768_dim() {
    let a = random_vec(768);
    let b = random_vec(768);
    let expected: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let result = dot_product(&a, &b);
    assert!((result - expected).abs() < 1e-3);
}

#[test]
fn test_l2_distance_correctness() {
    let a = vec![1.0, 2.0, 3.0];
    let b = vec![4.0, 5.0, 6.0];
    let expected = ((3.0f32).powi(2) * 3.0).sqrt();
    let result = l2_distance(&a, &b);
    assert!((result - expected).abs() < 1e-5);
}

#[test]
fn test_cosine_similarity_normalized() {
    let mut a = random_vec(768);
    let mut b = random_vec(768);
    normalize(&mut a);
    normalize(&mut b);
    let cos_sim = cosine_similarity(&a, &b);
    let dot = dot_product(&a, &b);
    assert!((cos_sim - dot).abs() < 1e-5);
}

#[test]
fn test_cosine_similarity_range() {
    let a = random_vec(768);
    let b = random_vec(768);
    let sim = cosine_similarity(&a, &b);
    assert!((-1.0..=1.0).contains(&sim));
}

#[test]
fn test_quantization_roundtrip() {
    let original = vec![0.5, -0.5, 1.0, -1.0, 0.0];
    let quantized = quantize_sq8(&original);
    let restored = dequantize_sq8(&quantized);
    for (o, r) in original.iter().zip(restored.iter()) {
        assert!((o - r).abs() < 0.01);
    }
}

#[test]
fn test_quantized_dot_product() {
    let a = random_vec(768);
    let b = random_vec(768);
    let qa = quantize_sq8(&a);
    let qb = quantize_sq8(&b);
    let exact = dot_product(&a, &b);
    let quantized = dot_product_sq8(&qa, &qb) as f32 / (127.0 * 127.0);
    let error = (exact - quantized).abs() / exact.abs().max(1.0);
    assert!(error < 0.1);
}

#[test]
fn test_normalize() {
    let mut v = vec![3.0, 4.0];
    normalize(&mut v);
    assert!((norm(&v) - 1.0).abs() < 1e-6);
}

// ============================================================================
// Flat index tests
// ============================================================================

#[test]
fn test_flat_index_insert_search() {
    let config = IndexConfig { dimension: 128, metric: DistanceMetric::Cosine, quantized: false };
    let index = FlatIndex::new(config);
    for i in 0..100 {
        index.insert(i, &random_vec(128)).unwrap();
    }
    assert_eq!(index.len(), 100);
    let results = index.search(&random_vec(128), 10);
    assert_eq!(results.len(), 10);
    for i in 1..results.len() {
        assert!(results[i - 1].distance <= results[i].distance);
    }
}

#[test]
fn test_flat_index_exact_match() {
    let config = IndexConfig { dimension: 64, metric: DistanceMetric::Cosine, quantized: false };
    let index = FlatIndex::new(config);
    let target: Vec<f32> = vec![1.0; 64];
    for i in 0..99 {
        index.insert(i, &random_vec(64)).unwrap();
    }
    index.insert(999, &target).unwrap();
    let results = index.search(&target, 10);
    assert_eq!(results[0].id, 999);
    assert!(results[0].distance < 1e-5);
}

#[test]
fn test_flat_index_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.flat");
    let config = IndexConfig { dimension: 64, metric: DistanceMetric::Cosine, quantized: false };
    let index = FlatIndex::new(config);
    let vecs: Vec<Vec<f32>> = (0..50).map(|_| random_vec(64)).collect();
    for (i, v) in vecs.iter().enumerate() {
        index.insert(i as u64, v).unwrap();
    }
    index.save(&path).unwrap();
    let loaded = FlatIndex::load(&path).unwrap();
    assert_eq!(loaded.len(), 50);
    let query = random_vec(64);
    let r1 = index.search(&query, 5);
    let r2 = loaded.search(&query, 5);
    assert_eq!(r1.iter().map(|r| r.id).collect::<Vec<_>>(), r2.iter().map(|r| r.id).collect::<Vec<_>>());
}

#[test]
fn test_flat_index_quantized() {
    let config = IndexConfig { dimension: 128, metric: DistanceMetric::DotProduct, quantized: true };
    let index = FlatIndex::new(config);
    for i in 0..100 {
        index.insert(i, &random_vec(128)).unwrap();
    }
    let query = random_vec(128);
    let exact = index.search(&query, 10);
    let quantized = index.search_quantized(&query, 10);
    let exact_ids: std::collections::HashSet<_> = exact.iter().map(|r| r.id).collect();
    let quantized_ids: std::collections::HashSet<_> = quantized.iter().map(|r| r.id).collect();
    let overlap = exact_ids.intersection(&quantized_ids).count();
    assert!(overlap >= 7);
}

// ============================================================================
// Vamana tests
// ============================================================================

fn random_vectors(n: usize, dim: usize) -> (Vec<u64>, Vec<f32>) {
    let mut rng = rand::thread_rng();
    let ids: Vec<u64> = (0..n as u64).collect();
    let vectors: Vec<f32> = (0..n * dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
    (ids, vectors)
}

#[test]
fn test_vamana_build_small() {
    let config = VamanaConfig { dimension: 32, max_degree: 8, build_search_size: 16, alpha: 1.2, metric: DistanceMetric::L2 };
    let (ids, vectors) = random_vectors(100, 32);
    let index = VamanaIndex::build(config, &ids, &vectors).unwrap();
    assert_eq!(index.len(), 100);
}

#[test]
fn test_vamana_search() {
    let config = VamanaConfig { dimension: 32, max_degree: 16, build_search_size: 32, alpha: 1.2, metric: DistanceMetric::L2 };
    let (ids, vectors) = random_vectors(500, 32);
    let index = VamanaIndex::build(config, &ids, &vectors).unwrap();
    let query = &vectors[0..32];
    let results = index.search(query, 10);
    assert_eq!(results.len(), 10);
    // First result should be very close to query (distance ~0)
    assert!(results[0].distance < 1e-3);
}

#[test]
fn test_vamana_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.vamana");
    let config = VamanaConfig { dimension: 32, max_degree: 8, build_search_size: 16, alpha: 1.2, metric: DistanceMetric::L2 };
    let (ids, vectors) = random_vectors(50, 32);
    let index = VamanaIndex::build(config, &ids, &vectors).unwrap();
    index.save(&path).unwrap();
    let loaded = VamanaIndex::load(&path).unwrap();
    assert_eq!(loaded.len(), 50);
    let query = &vectors[0..32];
    let r1 = index.search(query, 5);
    let r2 = loaded.search(query, 5);
    assert_eq!(r1.iter().map(|r| r.id).collect::<Vec<_>>(), r2.iter().map(|r| r.id).collect::<Vec<_>>());
}

#[test]
fn test_vamana_incremental_insert() {
    let config = VamanaConfig { dimension: 32, max_degree: 8, build_search_size: 16, alpha: 1.2, metric: DistanceMetric::L2 };
    let index = VamanaIndex::new(config);
    let mut rng = rand::thread_rng();
    for i in 0..50 {
        let vec: Vec<f32> = (0..32).map(|_| rng.gen_range(-1.0..1.0)).collect();
        index.insert(i, &vec).unwrap();
    }
    assert_eq!(index.len(), 50);
    let query: Vec<f32> = (0..32).map(|_| rng.gen_range(-1.0..1.0)).collect();
    let results = index.search(&query, 5);
    assert_eq!(results.len(), 5);
}

// ============================================================================
// Hybrid index tests
// ============================================================================

#[test]
fn test_hybrid_index_basic() {
    let config = HybridConfig { dimension: 64, use_graph: false, graph_threshold: 100, ..Default::default() };
    let index = HybridIndex::new(config);
    for _ in 0..50 {
        index.insert(EventId::new(), &random_vec(64)).unwrap();
    }
    assert_eq!(index.len(), 50);
    let results = index.search(&random_vec(64), 5);
    assert_eq!(results.len(), 5);
}

#[test]
fn test_hybrid_index_with_graph() {
    let config = HybridConfig {
        dimension: 64,
        use_graph: true,
        graph_threshold: 50,
        vamana_config: VamanaConfig { dimension: 64, max_degree: 16, build_search_size: 32, alpha: 1.2, metric: DistanceMetric::L2 },
        metric: DistanceMetric::L2,
    };
    let index = HybridIndex::new(config);
    for _ in 0..100 {
        index.insert(EventId::new(), &random_vec(64)).unwrap();
    }
    assert_eq!(index.len(), 100);
    let results = index.search(&random_vec(64), 10);
    assert_eq!(results.len(), 10);
}

// ============================================================================
// Deadlock detection test
// ============================================================================

#[test]
fn test_concurrent_no_deadlock() {
    let config = HybridConfig {
        dimension: 32,
        use_graph: true,
        graph_threshold: 20,
        vamana_config: VamanaConfig { dimension: 32, max_degree: 8, build_search_size: 16, alpha: 1.2, metric: DistanceMetric::L2 },
        metric: DistanceMetric::L2,
    };
    let index = Arc::new(HybridIndex::new(config));

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let idx = Arc::clone(&index);
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for i in 0..50 {
                    if i % 2 == 0 {
                        let _ = idx.insert(EventId::new(), &(0..32).map(|_| rng.gen_range(-1.0..1.0)).collect::<Vec<f32>>());
                    } else {
                        let _ = idx.search(&(0..32).map(|_| rng.gen_range(-1.0..1.0)).collect::<Vec<f32>>(), 5);
                    }
                }
                t
            })
        })
        .collect();

    // Timeout-based deadlock detection
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for h in handles {
            let _ = h.join();
        }
        let _ = tx.send(());
    });

    assert!(rx.recv_timeout(Duration::from_secs(10)).is_ok(), "Deadlock detected: threads did not complete within 10s");
}
