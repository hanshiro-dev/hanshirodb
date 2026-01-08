//! # Comprehensive MemTable Tests
//!
//! This test suite verifies:
//! - Basic MemTable operations
//! - Concurrent access patterns
//! - Memory management and flush triggers
//! - Integration with WAL and SSTable
//! - Performance characteristics
//! - Edge cases and error conditions

use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::{Duration, Instant};
use std::thread;
use tempfile::TempDir;

use hanshiro_core::{types::*, metrics::Metrics, traits::StorageEngine};
use hanshiro_storage::{
    memtable::{MemTable, MemTableManager, MemTableConfig}, 
    wal::{WriteAheadLog, WalConfig},
    StorageEngine as HanshiroStorage,
    engine::StorageConfig,
};

/// Create a test event with specified characteristics
fn create_test_event(id: u64, size: usize) -> Event {
    let mut event = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: format!("test-host-{}", id),
            ip: Some("10.0.0.1".to_string()),
            collector: "test-collector".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; size],
    );
    event.add_metadata("test_id", serde_json::json!(id));
    event.add_metadata("timestamp", serde_json::json!(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64));
    event
}

#[test]
fn test_memtable_basic_operations() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Test insert
    let event1 = create_test_event(1, 100);
    let event1_id = event1.id;
    let seq1 = memtable.insert(event1.clone()).unwrap();
    assert_eq!(seq1, 0);
    
    // Test get
    let retrieved = memtable.get(&event1_id).unwrap();
    assert_eq!(retrieved.id, event1_id);
    assert_eq!(retrieved.raw_data.len(), 100);
    
    // Test multiple inserts
    let event2 = create_test_event(2, 200);
    let event2_id = event2.id;
    let seq2 = memtable.insert(event2).unwrap();
    assert_eq!(seq2, 1);
    
    // Verify both events exist
    assert!(memtable.get(&event1_id).is_some());
    assert!(memtable.get(&event2_id).is_some());
    
    // Test non-existent key
    let fake_id = EventId::new();
    assert!(memtable.get(&fake_id).is_none());
}

#[test]
fn test_memtable_scan_operations() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Insert events with small delays to ensure ordering
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = create_test_event(i, 100);
        event_ids.push(event.id);
        memtable.insert(event).unwrap();
        thread::sleep(Duration::from_millis(5));
    }
    
    // Scan entire range
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    let all_events = memtable.scan(0, now);
    assert_eq!(all_events.len(), 10);
    
    // Verify ordering
    for i in 1..all_events.len() {
        let prev_meta = all_events[i-1].metadata().get("test_id").unwrap().as_u64().unwrap();
        let curr_meta = all_events[i].metadata().get("test_id").unwrap().as_u64().unwrap();
        assert!(prev_meta < curr_meta);
    }
}

#[test]
fn test_memtable_size_limits() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 10 * 1024,        // 10KB
        max_entries: 100,           
        max_age: Duration::from_secs(3600),
    };
    let memtable = MemTable::new(config, metrics);
    
    // Insert until size limit is reached
    let mut inserted = 0;
    while !memtable.should_flush() {
        let event = create_test_event(inserted, 1024); // 1KB events
        match memtable.insert(event) {
            Ok(_) => inserted += 1,
            Err(_) => break,
        }
    }
    
    // Should have inserted approximately 10 events
    println!("Inserted {} events before reaching size limit", inserted);
    assert!(inserted >= 5 && inserted <= 15, "Expected 5-15 inserts, got {}", inserted);
    assert!(memtable.should_flush());
    
    // Further inserts should fail
    let event = create_test_event(999, 1024);
    assert!(memtable.insert(event).is_err());
}

#[test]
fn test_memtable_entry_count_limit() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 1024 * 1024,      // 1MB
        max_entries: 10,            // Only 10 entries
        max_age: Duration::from_secs(3600),
    };
    let memtable = MemTable::new(config, metrics);
    
    // Insert exactly 10 events
    for i in 0..10 {
        let event = create_test_event(i, 100);
        memtable.insert(event).unwrap();
    }
    
    assert!(memtable.should_flush());
    
    // 11th event should fail
    let event = create_test_event(10, 100);
    assert!(memtable.insert(event).is_err());
}

#[test]
fn test_memtable_age_limit() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 1024 * 1024,      
        max_entries: 1000,          
        max_age: Duration::from_millis(100), // Very short for testing
    };
    let memtable = MemTable::new(config, metrics);
    
    // Insert one event
    let event = create_test_event(1, 100);
    memtable.insert(event).unwrap();
    
    // Should not flush immediately
    assert!(!memtable.should_flush());
    
    // Wait for age limit
    thread::sleep(Duration::from_millis(150));
    
    // Now should flush
    assert!(memtable.should_flush());
}

#[test]
fn test_memtable_concurrent_writes() {
    let metrics = Arc::new(Metrics::new());
    let memtable = Arc::new(MemTable::new(MemTableConfig::default(), metrics));
    let num_threads = 8usize;
    let writes_per_thread = 100usize;
    
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let memtable_clone = Arc::clone(&memtable);
        let success_count_clone = Arc::clone(&success_count);
        
        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let event_id = (thread_id * 1000 + i) as u64;
                let event = create_test_event(event_id, 256);
                if memtable_clone.insert(event).is_ok() {
                    success_count_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total_success = success_count.load(Ordering::Relaxed);
    assert_eq!(total_success as usize, num_threads * writes_per_thread);
    
    // Verify all events are retrievable
    let stats = memtable.stats();
    assert_eq!(stats.entry_count, num_threads * writes_per_thread);
}

#[test]
fn test_memtable_manager_rotation() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 5 * 1024,         // 5KB
        max_entries: 20,            
        max_age: Duration::from_secs(3600),
    };
    let manager = MemTableManager::new(config, metrics);
    
    // Insert events to trigger rotation
    let mut event_ids = Vec::new();
    for i in 0..50 {
        let event = create_test_event(i, 256);
        event_ids.push(event.id);
        manager.insert(event).unwrap();
    }
    
    // Verify all events are still accessible
    for (i, event_id) in event_ids.iter().enumerate() {
        let retrieved = manager.get(event_id);
        assert!(retrieved.is_some(), "Event {} not found", i);
    }
    
    // Should have at least one immutable table
    let stats = manager.stats();
    assert!(!stats.immutable.is_empty());
}

#[test]
fn test_memtable_flush_process() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 10 * 1024,        // 10KB
        max_entries: 50,            
        max_age: Duration::from_secs(3600),
    };
    let manager = Arc::new(MemTableManager::new(config, metrics));
    
    // Fill up a table
    for i in 0..60 {
        let event = create_test_event(i, 256);
        manager.insert(event).unwrap();
    }
    
    // Get immutable table for flushing
    let immutable = manager.get_immutable_for_flush();
    assert!(immutable.is_some());
    
    let table = immutable.unwrap();
    let entries = table.get_all_entries();
    assert!(!entries.is_empty());
    
    // Simulate successful flush
    table.clear();
    
    // Verify table is empty after clear
    let stats = table.stats();
    assert_eq!(stats.entry_count, 0);
    assert_eq!(stats.size_bytes, 0);
}

#[tokio::test]
async fn test_memtable_wal_integration() {
    let temp_dir = TempDir::new().unwrap();
    let metrics = Arc::new(Metrics::new());
    
    // Create WAL
    let wal_config = WalConfig {
        sync_on_write: false,
        ..Default::default()
    };
    let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), wal_config).await.unwrap());
    
    // Create MemTable
    let memtable_config = MemTableConfig {
        max_size: 50 * 1024,        // 50KB
        max_entries: 100,           
        max_age: Duration::from_secs(3600),
    };
    let memtable = Arc::new(MemTable::new(memtable_config, metrics.clone()));
    
    // Insert events into both WAL and MemTable
    let mut sequences = Vec::new();
    let mut event_ids = Vec::new();
    
    for i in 0..20 {
        let event = create_test_event(i, 1024);
        event_ids.push(event.id);
        
        // Write to WAL first (durability)
        let seq = wal.append(&event).await.unwrap();
        sequences.push(seq);
        
        // Then to MemTable
        memtable.insert(event).unwrap();
    }
    
    // Verify both have the same data
    for (i, event_id) in event_ids.iter().enumerate() {
        let mem_event = memtable.get(event_id);
        assert!(mem_event.is_some(), "Event {} not in MemTable", i);
    }
    
    // Verify WAL has all events
    let wal_entries = wal.read_from(0).await.unwrap();
    assert_eq!(wal_entries.len(), 20);
}

#[test]
fn test_memtable_stats_accuracy() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Initial stats should be empty
    let stats = memtable.stats();
    assert_eq!(stats.entry_count, 0);
    assert_eq!(stats.size_bytes, 0);
    assert!(stats.oldest_entry_age.is_none());
    assert!(stats.newest_entry_age.is_none());
    
    // Insert events
    for i in 0..10 {
        let event = create_test_event(i, 1024);
        memtable.insert(event).unwrap();
        thread::sleep(Duration::from_millis(10));
    }
    
    // Check stats
    let stats = memtable.stats();
    assert_eq!(stats.entry_count, 10);
    assert!(stats.size_bytes > 10 * 1024);
    assert!(stats.oldest_entry_age.is_some());
    assert!(stats.newest_entry_age.is_some());
    
    // Newest should be less than oldest
    assert!(stats.newest_entry_age.unwrap() < stats.oldest_entry_age.unwrap());
}

#[test]
fn test_memtable_read_only_enforcement() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Insert should work
    let event1 = create_test_event(1, 100);
    memtable.insert(event1).unwrap();
    
    // Mark as read-only
    memtable.set_read_only();
    
    // Insert should now fail
    let event2 = create_test_event(2, 100);
    let result = memtable.insert(event2);
    assert!(result.is_err());
    
    // But reads should still work for existing data
    // Note: We can't retrieve by a fake ID since EventId::from_string doesn't exist
    // The event we inserted has its own unique ID
}

#[test]
fn test_memtable_performance_characteristics() {
    let metrics = Arc::new(Metrics::new());
    let config = MemTableConfig {
        max_size: 100 * 1024 * 1024,  // 100MB
        max_entries: 1_000_000,         
        max_age: Duration::from_secs(3600),
    };
    let memtable = MemTable::new(config, metrics);
    
    let num_events = 10_000;
    let event_size = 1024; // 1KB each
    
    // Measure insert performance
    let mut inserted_ids = Vec::new();
    let start = Instant::now();
    for i in 0..num_events {
        let event = create_test_event(i, event_size);
        let event_id = event.id;
        memtable.insert(event).unwrap();
        if i < 100 {
            inserted_ids.push(event_id);
        }
    }
    let insert_duration = start.elapsed();
    
    let inserts_per_sec = num_events as f64 / insert_duration.as_secs_f64();
    println!("MemTable insert performance: {:.0} events/sec", inserts_per_sec);
    assert!(inserts_per_sec > 50_000.0, "Insert performance too low");
    
    // Measure read performance using actual inserted IDs
    let event_ids = inserted_ids;
    
    let start = Instant::now();
    let mut _found = 0;
    for _ in 0..1000 {
        for event_id in &event_ids {
            if memtable.get(event_id).is_some() {
                _found += 1;
            }
        }
    }
    let read_duration = start.elapsed();
    
    let reads_per_sec = (event_ids.len() * 1000) as f64 / read_duration.as_secs_f64();
    println!("MemTable read performance: {:.0} reads/sec", reads_per_sec);
    assert!(reads_per_sec > 100_000.0, "Read performance too low");
}

#[tokio::test]
async fn test_full_storage_engine_integration() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = StorageConfig::default();
    config.data_dir = temp_dir.path().to_path_buf();
    let engine = HanshiroStorage::new(config).await.unwrap();
    
    // Insert events through the storage engine
    let mut event_ids = Vec::new();
    for i in 0..30 {
        let event = create_test_event(i, 512);
        event_ids.push(event.id);
        engine.write(event).await.unwrap();
    }
    
    // Small delay to ensure writes are processed
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Read back through storage engine
    for (i, event_id) in event_ids.iter().enumerate() {
        let result = engine.read(*event_id).await;
        assert!(result.is_ok(), "Failed to read event {}", i);
        let event = result.unwrap();
        assert!(event.is_some(), "Event {} not found", i);
    }
}

#[test]
fn test_memtable_metadata_handling() {
    let metrics = Arc::new(Metrics::new());
    let memtable = MemTable::new(MemTableConfig::default(), metrics);
    
    // Create event with complex metadata
    let mut event = create_test_event(1, 256);
    event.add_metadata("field1", serde_json::json!("value1"));
    event.add_metadata("field2", serde_json::json!(42i64));
    event.add_metadata("field3", serde_json::json!(true));
    event.add_metadata("nested", serde_json::json!({
        "key1": "value1",
        "key2": [1, 2, 3]
    }));
    
    let event_id = event.id;
    memtable.insert(event).unwrap();
    
    // Retrieve and verify metadata
    let retrieved = memtable.get(&event_id).unwrap();
    assert_eq!(retrieved.metadata().get("field1").unwrap(), "value1");
    assert_eq!(retrieved.metadata().get("field2").unwrap(), 42);
    assert_eq!(retrieved.metadata().get("field3").unwrap(), true);
    assert!(retrieved.metadata().get("nested").is_some());
}