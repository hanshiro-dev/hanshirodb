//! # Comprehensive MemTable Tests
//!
//! This test suite covers:
//! - Basic operations (insert, get, scan)
//! - Concurrent access patterns
//! - Memory limits and flushing
//! - Performance characteristics
//! - Edge cases and stress testing

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

use hanshiro_core::{
    types::*,
    metrics::Metrics,
};
use hanshiro_storage::memtable::{MemTable, MemTableConfig, MemTableManager};

/// Helper to create test events with specific characteristics
fn create_test_event(id: u64, size: usize) -> Event {
    let mut event = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: format!("host-{}", id),
            ip: Some(format!("10.0.0.{}", id % 256).parse().unwrap()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; size],
    );
    event.add_metadata("id", id);
    event.add_metadata("timestamp", chrono::Utc::now().to_rfc3339());
    event
}

#[test]
fn test_memtable_basic_operations() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Test insert
    let event = create_test_event(1, 100);
    let event_id = event.id;
    let seq = memtable.insert(event.clone()).unwrap();
    assert_eq!(seq, 0);
    
    // Test get
    let retrieved = memtable.get(&event_id).unwrap();
    assert_eq!(retrieved.id, event_id);
    
    // Test get non-existent
    let fake_id = EventId::new();
    assert!(memtable.get(&fake_id).is_none());
    
    // Test stats
    let stats = memtable.stats();
    assert_eq!(stats.entry_count, 1);
    assert!(stats.size_bytes > 100);
}

#[test]
fn test_memtable_ordering() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Insert events with small delays to ensure different timestamps
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = create_test_event(i, 100);
        event_ids.push(event.id);
        memtable.insert(event).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    
    // Scan all events
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    let events = memtable.scan(0, now_ns);
    assert_eq!(events.len(), 10);
    
    // Verify events are in timestamp order
    for i in 1..events.len() {
        let prev_meta = events[i-1].get_metadata("id").unwrap().as_u64().unwrap();
        let curr_meta = events[i].get_metadata("id").unwrap().as_u64().unwrap();
        assert!(prev_meta < curr_meta);
    }
}

#[test]
fn test_memtable_size_limits() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 10 * 1024, // 10KB limit
        max_entries: 1_000_000,
        max_age: Duration::from_secs(3600),
    };
    let memtable = MemTable::new(config, metrics);
    
    let mut inserted = 0;
    loop {
        let event = create_test_event(inserted, 1024); // 1KB each
        match memtable.insert(event) {
            Ok(_) => inserted += 1,
            Err(_) => break,
        }
    }
    
    // Should have stopped due to size limit
    assert!(memtable.should_flush());
    assert!(inserted > 5 && inserted < 15); // Rough estimate with overhead
    
    let stats = memtable.stats();
    assert!(stats.size_bytes >= 10 * 1024);
}

#[test]
fn test_memtable_entry_limits() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 1024 * 1024 * 1024, // 1GB (won't hit this)
        max_entries: 10,
        max_age: Duration::from_secs(3600),
    };
    let memtable = MemTable::new(config, metrics);
    
    // Insert exactly max_entries
    for i in 0..10 {
        let event = create_test_event(i, 100);
        memtable.insert(event).unwrap();
    }
    
    assert!(memtable.should_flush());
    
    // Next insert should fail
    let event = create_test_event(10, 100);
    assert!(memtable.insert(event).is_err());
}

#[test]
fn test_memtable_age_limits() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 1024 * 1024 * 1024,
        max_entries: 1_000_000,
        max_age: Duration::from_millis(100), // Very short for testing
    };
    let memtable = MemTable::new(config, metrics);
    
    // Initially shouldn't flush
    assert!(!memtable.should_flush());
    
    // Insert one event
    let event = create_test_event(1, 100);
    memtable.insert(event).unwrap();
    
    // Wait for age limit
    std::thread::sleep(Duration::from_millis(150));
    
    // Now should flush due to age
    assert!(memtable.should_flush());
}

#[test]
fn test_memtable_concurrent_inserts() {
    let metrics = Arc::new(Metrics::new());
    let memtable = Arc::new(MemTable::new(MemTableConfig::default(), metrics));
    
    let num_threads = 10;
    let inserts_per_thread = 100;
    let mut handles = Vec::new();
    
    for thread_id in 0..num_threads {
        let memtable_clone = Arc::clone(&memtable);
        let handle = thread::spawn(move || {
            let mut sequences = Vec::new();
            for i in 0..inserts_per_thread {
                let event = create_test_event(thread_id * 1000 + i, 256);
                let seq = memtable_clone.insert(event).unwrap();
                sequences.push(seq);
            }
            sequences
        });
        handles.push(handle);
    }
    
    // Collect all sequences
    let mut all_sequences = Vec::new();
    for handle in handles {
        let sequences = handle.join().unwrap();
        all_sequences.extend(sequences);
    }
    
    // Verify no duplicate sequences
    all_sequences.sort();
    let unique_count = all_sequences.iter().collect::<std::collections::HashSet<_>>().len();
    assert_eq!(unique_count, num_threads * inserts_per_thread);
    
    // Verify stats
    let stats = memtable.stats();
    assert_eq!(stats.entry_count, num_threads * inserts_per_thread);
}

#[test]
fn test_memtable_concurrent_read_write() {
    let metrics = Arc::new(Metrics::new());
    let memtable = Arc::new(MemTable::new(MemTableConfig::default(), metrics));
    
    // Pre-populate some data
    let mut event_ids = Vec::new();
    for i in 0..100 {
        let event = create_test_event(i, 256);
        event_ids.push(event.id);
        memtable.insert(event).unwrap();
    }
    
    let memtable_clone = Arc::clone(&memtable);
    let event_ids_clone = event_ids.clone();
    
    // Reader thread
    let reader = thread::spawn(move || {
        let mut found = 0;
        for _ in 0..1000 {
            for id in &event_ids_clone {
                if memtable_clone.get(id).is_some() {
                    found += 1;
                }
            }
        }
        found
    });
    
    // Writer thread
    let writer = thread::spawn(move || {
        for i in 100..200 {
            let event = create_test_event(i, 256);
            memtable.insert(event).unwrap();
        }
    });
    
    let found = reader.join().unwrap();
    writer.join().unwrap();
    
    // Reader should have found all pre-populated events multiple times
    assert!(found >= 100 * 1000);
}

#[test]
fn test_memtable_scan_performance() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Insert many events
    for i in 0..10_000 {
        let event = create_test_event(i, 100);
        memtable.insert(event).unwrap();
    }
    
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    // Time full scan
    let start = Instant::now();
    let events = memtable.scan(0, now_ns);
    let duration = start.elapsed();
    
    assert_eq!(events.len(), 10_000);
    println!("Full scan of 10k events took: {:?}", duration);
    assert!(duration < Duration::from_millis(100)); // Should be very fast
    
    // Test partial scans
    let mid_ns = now_ns / 2;
    let partial = memtable.scan(mid_ns, now_ns);
    assert!(partial.len() < events.len());
}

#[test]
fn test_memtable_manager_basic() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig::default();
    let manager = MemTableManager::new(config, metrics);
    
    // Insert events
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = create_test_event(i, 256);
        event_ids.push(event.id);
        let seq = manager.insert(event).unwrap();
        assert_eq!(seq, i);
    }
    
    // Get events
    for id in &event_ids {
        let event = manager.get(id).unwrap();
        assert_eq!(event.id, *id);
    }
    
    // Stats should show one active table
    let stats = manager.stats();
    assert_eq!(stats.active.entry_count, 10);
    assert_eq!(stats.immutable.len(), 0);
}

#[test]
fn test_memtable_manager_rotation() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 5 * 1024, // 5KB - small for testing
        max_entries: 1_000_000,
        max_age: Duration::from_secs(3600),
    };
    let manager = MemTableManager::new(config, metrics);
    
    // Insert enough to trigger rotation
    for i in 0..20 {
        let event = create_test_event(i, 1024); // 1KB each
        manager.insert(event).unwrap();
    }
    
    // Should have rotated at least once
    let immutable = manager.get_immutable_for_flush();
    assert!(immutable.is_some());
    
    // Stats should reflect rotation
    let stats = manager.stats();
    assert!(stats.active.entry_count < 20);
}

#[test]
fn test_memtable_manager_concurrent_rotation() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_entries: 100,
        max_size: 1024 * 1024,
        max_age: Duration::from_secs(3600),
    };
    let manager = Arc::new(MemTableManager::new(config, metrics));
    
    let num_threads = 10;
    let mut handles = Vec::new();
    
    for thread_id in 0..num_threads {
        let manager_clone = Arc::clone(&manager);
        let handle = thread::spawn(move || {
            for i in 0..50 {
                let event = create_test_event(thread_id * 1000 + i, 256);
                manager_clone.insert(event).unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Should have multiple immutable tables
    let mut immutable_count = 0;
    while manager.get_immutable_for_flush().is_some() {
        immutable_count += 1;
    }
    
    println!("Created {} immutable tables", immutable_count);
    assert!(immutable_count >= 3); // With 500 total inserts and limit of 100
}

#[test]
fn test_memtable_read_after_rotate() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_entries: 10,
        max_size: 1024 * 1024,
        max_age: Duration::from_secs(3600),
    };
    let manager = MemTableManager::new(config, metrics);
    
    // Insert events across rotation boundary
    let mut event_ids = Vec::new();
    for i in 0..25 {
        let event = create_test_event(i, 256);
        event_ids.push(event.id);
        manager.insert(event).unwrap();
    }
    
    // All events should still be readable
    for (i, id) in event_ids.iter().enumerate() {
        let event = manager.get(id).unwrap();
        assert_eq!(event.get_metadata("id").unwrap().as_u64().unwrap(), i as u64);
    }
}

#[tokio::test]
async fn test_memtable_performance_benchmark() {
    let metrics = Arc::new(Metrics::new());
    let memtable = Arc::new(MemTable::new(MemTableConfig::default(), metrics));
    
    let num_events = 100_000;
    let event_size = 512;
    
    // Benchmark insert performance
    let start = Instant::now();
    for i in 0..num_events {
        let event = create_test_event(i, event_size);
        memtable.insert(event).unwrap();
    }
    let insert_duration = start.elapsed();
    
    let insert_rate = num_events as f64 / insert_duration.as_secs_f64();
    println!("MemTable Insert Performance:");
    println!("  Events: {}", num_events);
    println!("  Duration: {:?}", insert_duration);
    println!("  Rate: {:.0} events/sec", insert_rate);
    println!("  Latency: {:.2} μs/event", insert_duration.as_micros() as f64 / num_events as f64);
    
    // Benchmark get performance
    let event = create_test_event(num_events / 2, event_size);
    let test_id = event.id;
    memtable.insert(event).unwrap();
    
    let start = Instant::now();
    let iterations = 1_000_000;
    for _ in 0..iterations {
        let _ = memtable.get(&test_id);
    }
    let get_duration = start.elapsed();
    
    let get_rate = iterations as f64 / get_duration.as_secs_f64();
    println!("\nMemTable Get Performance:");
    println!("  Iterations: {}", iterations);
    println!("  Duration: {:?}", get_duration);
    println!("  Rate: {:.0} gets/sec", get_rate);
    println!("  Latency: {:.0} ns/get", get_duration.as_nanos() as f64 / iterations as f64);
}

#[test]
fn test_memtable_memory_estimation() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Insert events with known sizes
    let event_sizes = vec![100, 1024, 10240, 102400];
    let mut total_expected_size = 0;
    
    for (i, &size) in event_sizes.iter().enumerate() {
        let event = create_test_event(i as u64, size);
        memtable.insert(event).unwrap();
        total_expected_size += size;
    }
    
    let stats = memtable.stats();
    println!("Memory estimation accuracy:");
    println!("  Expected data size: {} bytes", total_expected_size);
    println!("  Reported size: {} bytes", stats.size_bytes);
    println!("  Overhead: {} bytes", stats.size_bytes.saturating_sub(total_expected_size));
    
    // Size should be at least the data size plus some overhead
    assert!(stats.size_bytes >= total_expected_size);
}

// Property-based tests
#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;
    
    proptest! {
        #[test]
        fn test_memtable_preserves_all_inserts(sizes in prop::collection::vec(1..10000usize, 1..100)) {
            let metrics = Arc::new(Metrics::new());
            let memtable = MemTable::new(MemTableConfig::default(), metrics);
            
            let mut event_ids = Vec::new();
            for (i, size) in sizes.iter().enumerate() {
                let event = create_test_event(i as u64, *size);
                event_ids.push(event.id);
                memtable.insert(event).unwrap();
            }
            
            // All events should be retrievable
            for id in &event_ids {
                assert!(memtable.get(id).is_some());
            }
            
            // Stats should match
            let stats = memtable.stats();
            assert_eq!(stats.entry_count, sizes.len());
        }
        
        #[test]
        fn test_memtable_scan_consistency(count in 10..1000usize) {
            let metrics = Arc::new(Metrics::new());
            let memtable = MemTable::new(MemTableConfig::default(), metrics);
            
            // Insert events
            for i in 0..count {
                let event = create_test_event(i as u64, 100);
                memtable.insert(event).unwrap();
            }
            
            // Full scan should return all events
            let now_ns = u64::MAX;
            let events = memtable.scan(0, now_ns);
            assert_eq!(events.len(), count);
        }
    }
}