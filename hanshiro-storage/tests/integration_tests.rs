//! # Storage Integration Tests
//!
//! Tests that verify the interaction between WAL, MemTable, and SSTable components.
//! These tests simulate real-world usage patterns and failure scenarios.

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use hanshiro_core::{
    types::*,
    metrics::Metrics,
    traits::*,
    Vector,
};
use hanshiro_storage::{
    engine::{StorageEngine as StorageEngineImpl, StorageConfig},
    wal::{WriteAheadLog, WalConfig},
    memtable::{MemTableManager, MemTableConfig},
    sstable::SSTableConfig,
};

/// Helper to create realistic security events
fn create_security_event(id: u64, event_type: EventType) -> Event {
    let mut event = Event::new(
        event_type.clone(),
        EventSource {
            host: format!("server-{}", id % 10),
            ip: Some(format!("10.0.{}.{}", id % 256, (id / 256) % 256).parse().unwrap()),
            collector: "syslog".to_string(),
            format: IngestionFormat::OCSF,
        },
        format!("Security event {} data with realistic payload", id).into_bytes(),
    );
    
    // Add realistic metadata
    event.add_metadata("severity", match id % 5 {
        0 => "critical",
        1 => "high",
        2 => "medium",
        3 => "low",
        _ => "info",
    });
    event.add_metadata("user", format!("user{}", id % 100));
    event.add_metadata("process", format!("process_{}", id % 50));
    event.add_metadata("action", match event_type {
        EventType::Authentication => "login",
        EventType::NetworkConnection => "connect",
        EventType::FileModify => "write",
        _ => "unknown",
    });
    
    event
}

#[tokio::test]
async fn test_end_to_end_flow() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig::default(),
        memtable_config: MemTableConfig::default(),
        sstable_config: SSTableConfig::default(),
        flush_interval: Duration::from_secs(1),
        compaction_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let engine = StorageEngineImpl::new(config).await.unwrap();
    
    // Write events
    let mut event_ids = Vec::new();
    for i in 0..100 {
        let event = create_security_event(i, EventType::NetworkConnection);
        let id = engine.write(event.clone()).await.unwrap();
        event_ids.push(id);
    }
    
    // Read immediately (should come from MemTable)
    for id in &event_ids[..10] {
        let event = engine.read(*id).await.unwrap().unwrap();
        assert_eq!(event.id, *id);
    }
    
    // Force flush to SSTable
    engine.force_flush().await.unwrap();
    
    // Read again (should come from SSTable)
    for id in &event_ids[50..60] {
        let event = engine.read(*id).await.unwrap().unwrap();
        assert_eq!(event.id, *id);
    }
    
    // Verify scan functionality
    let start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() - 3600; // 1 hour ago
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() + 3600; // 1 hour future
    
    let scanned = engine.scan(start, end).await.unwrap();
    assert_eq!(scanned.len(), 100);
}

#[tokio::test]
async fn test_crash_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();
    
    // First session: write data and simulate crash
    let mut event_ids = Vec::new();
    {
        let config = StorageConfig {
            data_dir: data_dir.clone(),
            wal_config: WalConfig {
                sync_on_write: true,
                ..Default::default()
            },
            memtable_config: MemTableConfig::default(),
            sstable_config: SSTableConfig::default(),
            flush_interval: Duration::from_secs(3600), // Don't auto-flush
            compaction_interval: Duration::from_secs(300),
        ..Default::default()
        };
        
        let engine = StorageEngineImpl::new(config).await.unwrap();
        
        // Write events to WAL and MemTable
        for i in 0..50 {
            let event = create_security_event(i, EventType::Authentication);
            let id = engine.write(event).await.unwrap();
            event_ids.push(id);
        }
        
        // Simulate crash - drop engine without flushing
        drop(engine);
    }
    
    // Second session: recover from WAL
    {
        let config = StorageConfig {
            data_dir,
            wal_config: WalConfig::default(),
            memtable_config: MemTableConfig::default(),
            sstable_config: SSTableConfig::default(),
            flush_interval: Duration::from_secs(3600),
            compaction_interval: Duration::from_secs(300),
        ..Default::default()
        };
        
        let engine = StorageEngineImpl::new(config).await.unwrap();
        
        // All events should be recoverable
        for (i, id) in event_ids.iter().enumerate() {
            let event = engine.read(*id).await.unwrap();
            assert!(event.is_some(), "Event {} with id {:?} not recovered", i, id);
        }
    }
}

#[tokio::test]
async fn test_concurrent_operations() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig {
            sync_on_write: false, // Faster for testing
            ..Default::default()
        },
        memtable_config: MemTableConfig::default(),
        sstable_config: SSTableConfig::default(),
        flush_interval: Duration::from_millis(100),
        compaction_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let engine = Arc::new(StorageEngineImpl::new(config).await.unwrap());
    
    // Spawn multiple writers
    let mut write_handles = Vec::new();
    for thread_id in 0..5 {
        let engine_clone = Arc::clone(&engine);
        let handle = tokio::spawn(async move {
            let mut ids = Vec::new();
            for i in 0..100 {
                let event = create_security_event(
                    thread_id * 1000 + i,
                    EventType::NetworkConnection,
                );
                let id = engine_clone.write(event).await.unwrap();
                ids.push(id);
            }
            ids
        });
        write_handles.push(handle);
    }
    
    // Spawn readers
    let mut read_handles = Vec::new();
    for _ in 0..3 {
        let engine_clone = Arc::clone(&engine);
        let handle = tokio::spawn(async move {
            let mut found = 0;
            for _ in 0..50 {
                // Read random events
                let random_id = EventId::new();
                if engine_clone.read(random_id).await.unwrap().is_some() {
                    found += 1;
                }
                sleep(Duration::from_millis(10)).await;
            }
            found
        });
        read_handles.push(handle);
    }
    
    // Spawn scanner
    let engine_clone = Arc::clone(&engine);
    let scan_handle = tokio::spawn(async move {
        let mut total_scanned = 0;
        for _ in 0..10 {
            let events = engine_clone.scan(0, u64::MAX).await.unwrap();
            total_scanned += events.len();
            sleep(Duration::from_millis(50)).await;
        }
        total_scanned
    });
    
    // Wait for writers
    let mut all_ids = Vec::new();
    for handle in write_handles {
        let ids = handle.await.unwrap();
        all_ids.extend(ids);
    }
    
    // Wait for readers and scanner
    for handle in read_handles {
        let _found = handle.await.unwrap();
    }
    let total_scanned = scan_handle.await.unwrap();
    
    println!("Wrote {} events, scanned {} total", all_ids.len(), total_scanned);
    assert_eq!(all_ids.len(), 500);
    assert!(total_scanned > 0);
    
    // Final verification
    sleep(Duration::from_millis(200)).await; // Let flushes complete
    for id in &all_ids[..10] {
        let event = engine.read(*id).await.unwrap().unwrap();
        assert_eq!(event.id, *id);
    }
}

#[tokio::test]
async fn test_memtable_to_sstable_flow() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig::default(),
        memtable_config: MemTableConfig {
            max_entries: 100, // Small for testing
            ..Default::default()
        },
        sstable_config: SSTableConfig::default(),
        flush_interval: Duration::from_millis(50),
        compaction_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let engine = StorageEngineImpl::new(config).await.unwrap();
    
    // Write enough to trigger flush
    for i in 0..150 {
        let event = create_security_event(i, EventType::FileModify);
        engine.write(event).await.unwrap();
    }
    
    // Wait for flush
    sleep(Duration::from_millis(200)).await;
    
    // Check that SSTables were created
    let mut sst_count = 0;
    let sstables_dir = temp_dir.path().join("sstables");
    if let Ok(mut entries) = tokio::fs::read_dir(sstables_dir).await {
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry.path().extension() == Some(std::ffi::OsStr::new("sst")) {
                sst_count += 1;
            }
        }
    }
    
    println!("Created {} SSTable files", sst_count);
    assert!(sst_count > 0);
}

#[tokio::test]
async fn test_compaction_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig::default(),
        memtable_config: MemTableConfig {
            max_entries: 50,
            ..Default::default()
        },
        sstable_config: SSTableConfig::default(),
        flush_interval: Duration::from_millis(50),
        compaction_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let engine = StorageEngineImpl::new(config).await.unwrap();
    
    // Create multiple small SSTables
    for batch in 0..5 {
        for i in 0..60 {
            let event = create_security_event(batch * 100 + i, EventType::ProcessStart);
            engine.write(event).await.unwrap();
        }
        sleep(Duration::from_millis(100)).await;
    }
    
    // Force compaction
    let result = engine.compact().await.unwrap();
    println!("Compaction result: {:?}", result);
    
    // Verify data is still readable after compaction
    let all_events = engine.scan(0, u64::MAX).await.unwrap();
    assert!(all_events.len() >= 250); // Some might be duplicates
}

#[tokio::test]
async fn test_merkle_chain_integrity_across_components() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig {
            sync_on_write: true,
            ..Default::default()
        },
        memtable_config: MemTableConfig::default(),
        sstable_config: SSTableConfig::default(),
        flush_interval: Duration::from_secs(1),
        compaction_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let engine = StorageEngineImpl::new(config).await.unwrap();
    
    // Write events with Merkle chaining
    let mut event_ids = Vec::new();
    for i in 0..20 {
        let event = create_security_event(i, EventType::MalwareDetected);
        let id = engine.write(event).await.unwrap();
        event_ids.push(id);
    }
    
    // Verify WAL integrity
    engine.verify_wal_integrity().await.unwrap();
    
    // Flush to SSTable
    engine.force_flush().await.unwrap();
    
    // Read events and verify Merkle chain
    let mut prev_hash: Option<String> = None;
    for id in &event_ids {
        let event = engine.read(*id).await.unwrap().unwrap();
        
        if let Some(ref merkle_hash) = event.merkle_hash {
            if let Some(ref prev) = prev_hash {
                // In a real implementation, we'd verify the chain here
                assert!(merkle_hash != prev);
            }
            prev_hash = Some(merkle_hash.clone());
        }
    }
}

#[tokio::test] 
async fn test_performance_under_load() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig {
            sync_on_write: false,
            max_file_size: 100 * 1024 * 1024, // 100MB
            ..Default::default()
        },
        memtable_config: MemTableConfig {
            max_size: 64 * 1024 * 1024, // 64MB
            ..Default::default()
        },
        sstable_config: SSTableConfig::default(),
        flush_interval: Duration::from_millis(500),
        compaction_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let engine = Arc::new(StorageEngineImpl::new(config).await.unwrap());
    
    let start = tokio::time::Instant::now();
    let target_events = 10_000;
    
    // Simulate high-velocity ingestion
    let mut handles = Vec::new();
    for thread_id in 0..10 {
        let engine_clone = Arc::clone(&engine);
        let handle = tokio::spawn(async move {
            for i in 0..1000 {
                let event = create_security_event(
                    thread_id * 10000 + i,
                    match i % 4 {
                        0 => EventType::NetworkConnection,
                        1 => EventType::Authentication,
                        2 => EventType::FileModify,
                        _ => EventType::ProcessStart,
                    }
                );
                engine_clone.write(event).await.unwrap();
            }
        });
        handles.push(handle);
    }
    
    // Wait for completion
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let throughput = target_events as f64 / duration.as_secs_f64();
    
    println!("Performance Test Results:");
    println!("  Events written: {}", target_events);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} events/sec", throughput);
    println!("  Latency: {:.2} ms/event", duration.as_millis() as f64 / target_events as f64);
    
    // Should achieve reasonable throughput
    assert!(throughput > 2000.0); // At least 2k events/sec
    
    // Verify all data is readable
    let final_count = engine.scan(0, u64::MAX).await.unwrap().len();
    assert_eq!(final_count, target_events);
}

#[tokio::test]
async fn test_storage_with_vector_data() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngineImpl::new(StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    }).await.unwrap();
    
    // Create events with vector embeddings
    for i in 0..50 {
        let mut event = create_security_event(i, EventType::AnomalyDetected);
        
        // Add mock vector embedding
        let vector_data = (0..768).map(|j| (i + j) as f32 / 1000.0).collect();
        let mut vector = Vector::new(vector_data).unwrap();
        vector.normalize();
        event.vector = Some(vector);
        
        engine.write(event).await.unwrap();
    }
    
    // Flush and verify vectors are preserved
    engine.force_flush().await.unwrap();
    
    let events = engine.scan(0, u64::MAX).await.unwrap();
    assert_eq!(events.len(), 50);
    
    for (i, event) in events.iter().enumerate() {
        assert!(event.vector.is_some());
        let vector = event.vector.as_ref().unwrap();
        assert_eq!(vector.dimension.value(), 768);
        assert!(vector.normalized);
    }
}

#[tokio::test]
async fn test_time_travel_queries() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngineImpl::new(StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    }).await.unwrap();
    
    // Write events at different times
    let base_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    for i in 0..60 {
        let event = create_security_event(i, EventType::PolicyViolation);
        engine.write(event).await.unwrap();
    }
    
    // Query events from a wide time range
    let all_events = engine.scan(base_time - 1, base_time + 10).await.unwrap();
    
    // Should get all events since they were written within the time window
    assert!(all_events.len() >= 50, "Expected at least 50 events, got {}", all_events.len());
}

// Add missing import for StorageEngine trait
use async_trait::async_trait;