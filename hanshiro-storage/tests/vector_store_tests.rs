//! # Vector Store Tests
//!
//! Comprehensive tests for mmap'd vector storage.

use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use hanshiro_core::EventId;
use hanshiro_storage::VectorStore;
use tempfile::TempDir;

// ============================================================================
// Basic Operations
// ============================================================================

#[test]
fn test_create_empty_store() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 128).unwrap();
    
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
    assert_eq!(store.dimension(), 128);
}

#[test]
fn test_insert_single_vector() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 64).unwrap();
    let id = EventId::new();
    let vec: Vec<f32> = (0..64).map(|i| i as f32 / 64.0).collect();
    
    store.insert(id, &vec).unwrap();
    
    assert_eq!(store.len(), 1);
    assert!(!store.is_empty());
    
    let retrieved = store.get(&id).unwrap();
    assert_eq!(retrieved.len(), 64);
    
    for (a, b) in retrieved.iter().zip(vec.iter()) {
        assert!((a - b).abs() < 1e-6, "Mismatch: {} vs {}", a, b);
    }
}

#[test]
fn test_insert_multiple_vectors() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 32).unwrap();
    let mut ids = Vec::new();
    
    for i in 0..100 {
        let id = EventId { hi: i, lo: i * 2 };
        let vec: Vec<f32> = (0..32).map(|j| (i * 32 + j) as f32).collect();
        store.insert(id, &vec).unwrap();
        ids.push(id);
    }
    
    assert_eq!(store.len(), 100);
    
    // Verify all vectors
    for (i, id) in ids.iter().enumerate() {
        let vec = store.get(id).unwrap();
        let expected: Vec<f32> = (0..32).map(|j| (i as u64 * 32 + j as u64) as f32).collect();
        assert_eq!(vec, expected);
    }
}

#[test]
fn test_idempotent_insert() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 16).unwrap();
    let id = EventId::new();
    let vec: Vec<f32> = vec![1.0; 16];
    
    // Insert same ID multiple times
    store.insert(id, &vec).unwrap();
    store.insert(id, &vec).unwrap();
    store.insert(id, &vec).unwrap();
    
    // Should only have one entry
    assert_eq!(store.len(), 1);
}

#[test]
fn test_dimension_mismatch() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 64).unwrap();
    let id = EventId::new();
    
    // Wrong dimension
    let vec: Vec<f32> = vec![1.0; 32];
    let result = store.insert(id, &vec);
    
    assert!(result.is_err());
}

#[test]
fn test_get_nonexistent() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 64).unwrap();
    let id = EventId::new();
    
    assert!(store.get(&id).is_none());
}

// ============================================================================
// Persistence
// ============================================================================

#[test]
fn test_persistence_basic() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let id = EventId { hi: 12345, lo: 67890 };
    let vec: Vec<f32> = (0..128).map(|i| i as f32 * 0.1).collect();
    
    // Write
    {
        let store = VectorStore::open(&path, 128).unwrap();
        store.insert(id, &vec).unwrap();
        store.sync().unwrap();
    }
    
    // Read back
    {
        let store = VectorStore::open(&path, 128).unwrap();
        assert_eq!(store.len(), 1);
        
        let retrieved = store.get(&id).unwrap();
        for (a, b) in retrieved.iter().zip(vec.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }
}

#[test]
fn test_persistence_many_vectors() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let count = 1000;
    let dim = 64;
    
    // Write
    {
        let store = VectorStore::open(&path, dim).unwrap();
        for i in 0..count {
            let id = EventId { hi: i as u64, lo: 0 };
            let vec: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32).collect();
            store.insert(id, &vec).unwrap();
        }
        store.sync().unwrap();
    }
    
    // Read back
    {
        let store = VectorStore::open(&path, dim).unwrap();
        assert_eq!(store.len(), count);
        
        // Spot check
        for i in [0, 100, 500, 999] {
            let id = EventId { hi: i as u64, lo: 0 };
            let vec = store.get(&id).unwrap();
            let expected: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32).collect();
            assert_eq!(vec, expected);
        }
    }
}

#[test]
fn test_persistence_append_after_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    // Write first batch
    {
        let store = VectorStore::open(&path, 32).unwrap();
        for i in 0..50 {
            let id = EventId { hi: i, lo: 0 };
            store.insert(id, &vec![i as f32; 32]).unwrap();
        }
        store.sync().unwrap();
    }
    
    // Reopen and append more
    {
        let store = VectorStore::open(&path, 32).unwrap();
        assert_eq!(store.len(), 50);
        
        for i in 50..100 {
            let id = EventId { hi: i, lo: 0 };
            store.insert(id, &vec![i as f32; 32]).unwrap();
        }
        store.sync().unwrap();
    }
    
    // Verify all
    {
        let store = VectorStore::open(&path, 32).unwrap();
        assert_eq!(store.len(), 100);
        
        for i in 0..100u64 {
            let id = EventId { hi: i, lo: 0 };
            let vec = store.get(&id).unwrap();
            assert_eq!(vec, vec![i as f32; 32]);
        }
    }
}

// ============================================================================
// Similarity Search
// ============================================================================

#[test]
fn test_find_similar_exact_match() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 4).unwrap();
    
    let id1 = EventId { hi: 1, lo: 0 };
    let id2 = EventId { hi: 2, lo: 0 };
    let id3 = EventId { hi: 3, lo: 0 };
    
    store.insert(id1, &[1.0, 0.0, 0.0, 0.0]).unwrap();
    store.insert(id2, &[0.0, 1.0, 0.0, 0.0]).unwrap();
    store.insert(id3, &[0.0, 0.0, 1.0, 0.0]).unwrap();
    
    // Query for exact match
    let results = store.find_similar(&[1.0, 0.0, 0.0, 0.0], 1);
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, id1);
    assert!((results[0].1 - 1.0).abs() < 1e-6); // Cosine similarity = 1.0
}

#[test]
fn test_find_similar_ranking() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 4).unwrap();
    
    let id1 = EventId { hi: 1, lo: 0 };
    let id2 = EventId { hi: 2, lo: 0 };
    let id3 = EventId { hi: 3, lo: 0 };
    
    // id1: exactly aligned with query
    store.insert(id1, &[1.0, 0.0, 0.0, 0.0]).unwrap();
    // id2: partially aligned
    store.insert(id2, &[0.7, 0.7, 0.0, 0.0]).unwrap();
    // id3: orthogonal
    store.insert(id3, &[0.0, 0.0, 1.0, 0.0]).unwrap();
    
    let query = [1.0, 0.0, 0.0, 0.0];
    let results = store.find_similar(&query, 3);
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, id1); // Most similar
    assert_eq!(results[1].0, id2); // Second
    assert_eq!(results[2].0, id3); // Least similar (orthogonal)
    
    assert!(results[0].1 > results[1].1);
    assert!(results[1].1 > results[2].1);
}

#[test]
fn test_find_similar_top_k() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 8).unwrap();
    
    // Insert 100 vectors
    for i in 0..100 {
        let id = EventId { hi: i, lo: 0 };
        let mut vec = vec![0.0f32; 8];
        vec[0] = (100 - i) as f32; // Closer to query as i increases
        store.insert(id, &vec).unwrap();
    }
    
    let query = vec![0.0f32; 8];
    
    // Request top 10
    let results = store.find_similar(&query, 10);
    assert_eq!(results.len(), 10);
    
    // Request more than available
    let results = store.find_similar(&query, 200);
    assert_eq!(results.len(), 100);
}

#[test]
fn test_find_similar_empty_store() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 64).unwrap();
    
    let query = vec![1.0f32; 64];
    let results = store.find_similar(&query, 10);
    
    assert!(results.is_empty());
}

#[test]
fn test_find_similar_wrong_dimension() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 64).unwrap();
    store.insert(EventId::new(), &vec![1.0; 64]).unwrap();
    
    // Wrong query dimension
    let query = vec![1.0f32; 32];
    let results = store.find_similar(&query, 10);
    
    assert!(results.is_empty());
}

// ============================================================================
// Iterator
// ============================================================================

#[test]
fn test_iterator() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 16).unwrap();
    
    let mut expected_ids = HashSet::new();
    for i in 0..50 {
        let id = EventId { hi: i, lo: i * 2 };
        store.insert(id, &vec![i as f32; 16]).unwrap();
        expected_ids.insert(id);
    }
    
    let mut found_ids = HashSet::new();
    for (id, vec) in store.iter() {
        found_ids.insert(id);
        assert_eq!(vec.len(), 16);
    }
    
    assert_eq!(found_ids, expected_ids);
}

// ============================================================================
// Concurrent Access
// ============================================================================

#[test]
fn test_concurrent_inserts() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = Arc::new(VectorStore::open(&path, 32).unwrap());
    let mut handles = Vec::new();
    
    // Spawn 4 threads, each inserting 250 vectors
    for t in 0..4 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 0..250 {
                let id = EventId { hi: t * 1000 + i, lo: 0 };
                let vec = vec![(t * 1000 + i) as f32; 32];
                store.insert(id, &vec).unwrap();
            }
        }));
    }
    
    for h in handles {
        h.join().unwrap();
    }
    
    assert_eq!(store.len(), 1000);
    
    // Verify all vectors
    for t in 0..4u64 {
        for i in 0..250u64 {
            let id = EventId { hi: t * 1000 + i, lo: 0 };
            let vec = store.get(&id).unwrap();
            assert_eq!(vec, vec![(t * 1000 + i) as f32; 32]);
        }
    }
}

#[test]
fn test_concurrent_read_write() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = Arc::new(VectorStore::open(&path, 16).unwrap());
    
    // Pre-populate
    for i in 0..100 {
        let id = EventId { hi: i, lo: 0 };
        store.insert(id, &vec![i as f32; 16]).unwrap();
    }
    
    let mut handles = Vec::new();
    
    // Writer thread
    let store_w = Arc::clone(&store);
    handles.push(thread::spawn(move || {
        for i in 100..200 {
            let id = EventId { hi: i, lo: 0 };
            store_w.insert(id, &vec![i as f32; 16]).unwrap();
        }
    }));
    
    // Reader threads
    for _ in 0..3 {
        let store_r = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                // Read existing vectors
                for i in 0..100u64 {
                    let id = EventId { hi: i, lo: 0 };
                    if let Some(vec) = store_r.get(&id) {
                        assert_eq!(vec.len(), 16);
                    }
                }
            }
        }));
    }
    
    for h in handles {
        h.join().unwrap();
    }
    
    assert_eq!(store.len(), 200);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_large_dimension() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let dim = 768; // Common embedding dimension
    let store = VectorStore::open(&path, dim).unwrap();
    
    let id = EventId::new();
    let vec: Vec<f32> = (0..dim).map(|i| (i as f32).sin()).collect();
    
    store.insert(id, &vec).unwrap();
    
    let retrieved = store.get(&id).unwrap();
    assert_eq!(retrieved.len(), dim);
    
    for (a, b) in retrieved.iter().zip(vec.iter()) {
        assert!((a - b).abs() < 1e-6);
    }
}

#[test]
fn test_special_float_values() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 4).unwrap();
    
    let id = EventId::new();
    let vec = vec![0.0, -0.0, f32::MIN_POSITIVE, f32::MAX];
    
    store.insert(id, &vec).unwrap();
    
    let retrieved = store.get(&id).unwrap();
    assert_eq!(retrieved[0], 0.0);
    assert_eq!(retrieved[2], f32::MIN_POSITIVE);
    assert_eq!(retrieved[3], f32::MAX);
}

#[test]
fn test_file_growth() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 128).unwrap();
    
    // Insert enough vectors to trigger file growth
    for i in 0..5000 {
        let id = EventId { hi: i, lo: 0 };
        let vec = vec![i as f32; 128];
        store.insert(id, &vec).unwrap();
    }
    
    assert_eq!(store.len(), 5000);
    
    // Verify random samples
    for i in [0, 1000, 2500, 4999] {
        let id = EventId { hi: i as u64, lo: 0 };
        let vec = store.get(&id).unwrap();
        assert_eq!(vec, vec![i as f32; 128]);
    }
}

// ============================================================================
// Performance Characteristics
// ============================================================================

#[test]
fn test_insert_performance() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 128).unwrap();
    let count = 10_000;
    
    let start = std::time::Instant::now();
    
    for i in 0..count {
        let id = EventId { hi: i, lo: 0 };
        let vec = vec![i as f32; 128];
        store.insert(id, &vec).unwrap();
    }
    
    let elapsed = start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    
    println!("Inserted {} vectors in {:?} ({:.0} vectors/sec)", count, elapsed, rate);
    
    // Should be reasonably fast (at least 10K/sec in debug mode)
    assert!(rate > 5_000.0, "Insert rate too slow: {:.0}/sec", rate);
}

#[test]
fn test_search_performance() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.vec");
    
    let store = VectorStore::open(&path, 128).unwrap();
    
    // Insert vectors
    for i in 0..10_000 {
        let id = EventId { hi: i, lo: 0 };
        let vec: Vec<f32> = (0..128).map(|j| ((i + j as u64) % 100) as f32).collect();
        store.insert(id, &vec).unwrap();
    }
    
    let query: Vec<f32> = (0..128).map(|i| i as f32).collect();
    
    let start = std::time::Instant::now();
    let iterations = 100;
    
    for _ in 0..iterations {
        let _ = store.find_similar(&query, 10);
    }
    
    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_secs_f64() * 1000.0 / iterations as f64;
    
    println!("Search over 10K vectors: {:.2}ms avg", avg_ms);
    
    // Should complete in reasonable time (< 100ms per search in debug)
    assert!(avg_ms < 100.0, "Search too slow: {:.2}ms", avg_ms);
}
