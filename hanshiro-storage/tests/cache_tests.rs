//! Block cache tests

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use bytes::Bytes;
use hanshiro_storage::cache::{BlockCache, CacheKey};

#[test]
fn test_cache_hit_rate() {
    let cache = BlockCache::new(1024 * 1024); // 1MB
    
    // Insert 100 blocks
    for i in 0..100u64 {
        let key = CacheKey::new(1, i * 4096);
        cache.insert(key, Bytes::from(vec![i as u8; 1024]));
    }
    
    // Access same blocks multiple times
    for _ in 0..10 {
        for i in 0..100u64 {
            let key = CacheKey::new(1, i * 4096);
            assert!(cache.get(&key).is_some(), "Block {} should be cached", i);
        }
    }
    
    let stats = cache.stats();
    assert_eq!(stats.entries, 100);
    assert!(stats.hit_rate > 0.99, "Hit rate should be ~100%: {}", stats.hit_rate);
}

#[test]
fn test_cache_eviction_under_pressure() {
    // Very small cache: should evict aggressively
    let cache = BlockCache::with_shards(4096, 4); // 4KB, 4 shards
    
    // Insert way more than capacity
    for i in 0..1000u64 {
        let key = CacheKey::new(1, i * 4096);
        cache.insert(key, Bytes::from(vec![0u8; 256]));
    }
    
    let stats = cache.stats();
    // Should have evicted most entries
    assert!(stats.entries < 100, "Should evict under pressure: {} entries", stats.entries);
}

#[test]
fn test_cache_different_files() {
    let cache = BlockCache::new(10 * 1024 * 1024);
    
    // Insert blocks from different files
    for file_id in 0..10u64 {
        for block in 0..100u64 {
            let key = CacheKey::new(file_id, block * 4096);
            cache.insert(key, Bytes::from(vec![(file_id + block) as u8; 512]));
        }
    }
    
    // Verify all accessible
    for file_id in 0..10u64 {
        for block in 0..100u64 {
            let key = CacheKey::new(file_id, block * 4096);
            let value = cache.get(&key);
            assert!(value.is_some(), "file={} block={} should exist", file_id, block);
        }
    }
    
    let stats = cache.stats();
    assert_eq!(stats.entries, 1000);
}

#[test]
fn test_cache_update_existing() {
    let cache = BlockCache::new(1024 * 1024);
    
    let key = CacheKey::new(1, 0);
    
    // Insert original
    cache.insert(key.clone(), Bytes::from(vec![1u8; 100]));
    assert_eq!(cache.get(&key).unwrap()[0], 1);
    
    // Update with new value
    cache.insert(key.clone(), Bytes::from(vec![2u8; 200]));
    let value = cache.get(&key).unwrap();
    assert_eq!(value[0], 2);
    assert_eq!(value.len(), 200);
    
    // Should still be 1 entry
    assert_eq!(cache.stats().entries, 1);
}

#[test]
fn test_cache_clear() {
    let cache = BlockCache::new(1024 * 1024);
    
    for i in 0..100u64 {
        cache.insert(CacheKey::new(1, i * 4096), Bytes::from(vec![0u8; 100]));
    }
    
    assert_eq!(cache.stats().entries, 100);
    
    cache.clear();
    
    let stats = cache.stats();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.size_bytes, 0);
}

#[test]
fn test_cache_concurrent_performance() {
    let cache = Arc::new(BlockCache::new(100 * 1024 * 1024)); // 100MB
    let num_threads = 8;
    let ops_per_thread = 50_000;
    
    // Pre-populate
    for i in 0..1000u64 {
        cache.insert(CacheKey::new(0, i * 4096), Bytes::from(vec![0u8; 1024]));
    }
    
    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = CacheKey::new(0, ((t * ops_per_thread + i) % 1000) * 4096);
                    if i % 10 == 0 {
                        cache.insert(key, Bytes::from(vec![t as u8; 1024]));
                    } else {
                        let _ = cache.get(&key);
                    }
                }
            })
        })
        .collect();
    
    for h in handles {
        h.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let total_ops = num_threads * ops_per_thread;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    
    println!("\n=== Cache Concurrent Performance ===");
    println!("Threads: {}", num_threads);
    println!("Total ops: {}", total_ops);
    println!("Duration: {:?}", elapsed);
    println!("Throughput: {:.0} ops/sec", ops_per_sec);
    println!("Stats: {:?}", cache.stats());
    
    // Should achieve at least 1M ops/sec
    assert!(ops_per_sec > 1_000_000.0, "Too slow: {} ops/sec", ops_per_sec);
}
