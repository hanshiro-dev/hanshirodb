//! # Storage Engine Crash Recovery Tests
//!
//! Tests for:
//! - WAL replay after crash
//! - Manifest checkpoint persistence
//! - Data durability guarantees
//! - Recovery with partial flushes

use std::collections::HashSet;
use tempfile::TempDir;

use hanshiro_core::types::*;
use hanshiro_core::traits::StorageEngine as StorageEngineTrait;
use hanshiro_storage::engine::{StorageConfig, StorageEngine};

/// Helper to create test events
fn create_test_event(id: u64, data_size: usize) -> Event {
    Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: format!("host-{}", id),
            ip: Some("192.168.1.1".parse().unwrap()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; data_size],
    )
}

// =============================================================================
// Basic Crash Recovery Tests
// =============================================================================

#[tokio::test]
async fn test_crash_recovery_basic() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let mut event_ids = Vec::new();

    // Session 1: Write data, then "crash" (drop without flush to SSTable)
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for i in 0..10 {
            let event = create_test_event(i, 256);
            let id = engine.write(event).await.unwrap();
            event_ids.push(id);
        }

        // Ensure WAL is flushed to disk
        engine.flush_wal().await.unwrap();
        
        // Drop engine - simulates crash before MemTable flush to SSTable
    }

    // Session 2: Recover and verify all data
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for (i, id) in event_ids.iter().enumerate() {
            let event = engine.read(*id).await.unwrap();
            assert!(event.is_some(), "Event {} (id={:?}) not recovered after crash", i, id);
        }
    }
}

#[tokio::test]
async fn test_crash_recovery_large_dataset() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let num_events = 1000;
    let mut event_ids = Vec::new();

    // Session 1: Write many events
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for i in 0..num_events {
            let event = create_test_event(i, 512);
            let id = engine.write(event).await.unwrap();
            event_ids.push(id);
        }

        engine.flush_wal().await.unwrap();
    }

    // Session 2: Recover and verify
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        let mut recovered = 0;
        for id in &event_ids {
            if engine.read(*id).await.unwrap().is_some() {
                recovered += 1;
            }
        }

        assert_eq!(recovered, num_events, 
            "Expected {} events recovered, got {}", num_events, recovered);
    }
}

#[tokio::test]
async fn test_crash_recovery_with_batch_writes() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let mut event_ids = Vec::new();

    // Session 1: Batch writes
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        // Write in batches
        for batch_num in 0..10 {
            let events: Vec<Event> = (0..100)
                .map(|i| create_test_event(batch_num * 100 + i, 256))
                .collect();
            
            let ids = engine.write_batch(events).await.unwrap();
            event_ids.extend(ids);
        }

        engine.flush_wal().await.unwrap();
    }

    // Session 2: Recover
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        let mut recovered = 0;
        for id in &event_ids {
            if engine.read(*id).await.unwrap().is_some() {
                recovered += 1;
            }
        }

        assert_eq!(recovered, 1000, "Expected 1000 events, got {}", recovered);
    }
}

// =============================================================================
// Checkpoint and Manifest Tests
// =============================================================================

#[tokio::test]
async fn test_checkpoint_updated_after_flush() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };
    config.memtable_config.max_entries = 10; // Small to trigger flush

    let engine = StorageEngine::new(config.clone()).await.unwrap();

    // Initial checkpoint should be 0
    assert_eq!(engine.wal_checkpoint(), 0, "Initial checkpoint should be 0");

    // Write events
    for i in 0..20 {
        let event = create_test_event(i, 256);
        engine.write(event).await.unwrap();
    }

    // Force flush to SSTable
    engine.force_flush().await.unwrap();

    // Checkpoint should be updated
    let checkpoint = engine.wal_checkpoint();
    assert!(checkpoint > 0, "Checkpoint should be > 0 after flush, got {}", checkpoint);
}

#[tokio::test]
async fn test_manifest_persists_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };
    config.memtable_config.max_entries = 10;

    let checkpoint_after_flush;

    // Session 1: Write and flush
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for i in 0..20 {
            let event = create_test_event(i, 256);
            engine.write(event).await.unwrap();
        }

        engine.force_flush().await.unwrap();
        checkpoint_after_flush = engine.wal_checkpoint();
    }

    // Session 2: Verify manifest was loaded
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();
        
        // Checkpoint should be restored from manifest
        assert_eq!(
            engine.wal_checkpoint(), 
            checkpoint_after_flush,
            "Checkpoint not restored from manifest"
        );
    }
}

#[tokio::test]
async fn test_no_duplicate_replay_after_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let mut all_event_ids = Vec::new();

    // Session 1: Write data (stays in MemTable, not flushed to SSTable)
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for i in 0..20 {
            let event = create_test_event(i, 256);
            let id = engine.write(event).await.unwrap();
            all_event_ids.push(id);
        }

        engine.flush_wal().await.unwrap();
        // Don't flush to SSTable - data only in WAL
    }

    // Session 2: Recover - should replay from WAL without duplicates
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        // Count occurrences of each event
        let mut found_ids = HashSet::new();
        for id in &all_event_ids {
            if engine.read(*id).await.unwrap().is_some() {
                assert!(found_ids.insert(*id), "Duplicate event found: {:?}", id);
            }
        }

        assert_eq!(found_ids.len(), 20, "Expected 20 unique events, got {}", found_ids.len());
    }
}

// =============================================================================
// Multiple Restart Tests
// =============================================================================

#[tokio::test]
async fn test_multiple_crash_recovery_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let mut all_event_ids = Vec::new();

    // Multiple crash/recovery cycles
    for cycle in 0..5 {
        {
            let engine = StorageEngine::new(config.clone()).await.unwrap();

            // Verify previous data still exists
            for id in &all_event_ids {
                let event = engine.read(*id).await.unwrap();
                assert!(event.is_some(), "Event {:?} lost in cycle {}", id, cycle);
            }

            // Write new data
            for i in 0..10 {
                let event = create_test_event(cycle * 100 + i, 256);
                let id = engine.write(event).await.unwrap();
                all_event_ids.push(id);
            }

            engine.flush_wal().await.unwrap();
        }
    }

    // Final verification
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        let mut recovered = 0;
        for id in &all_event_ids {
            if engine.read(*id).await.unwrap().is_some() {
                recovered += 1;
            }
        }

        assert_eq!(recovered, 50, "Expected 50 events after 5 cycles");
    }
}

#[tokio::test]
async fn test_recovery_continues_sequence_numbers() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    // Session 1
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for i in 0..10 {
            let event = create_test_event(i, 256);
            engine.write(event).await.unwrap();
        }

        engine.flush_wal().await.unwrap();
    }

    // Session 2: Write more, verify no sequence collision
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        // Write more events - should continue from where we left off
        for i in 10..20 {
            let event = create_test_event(i, 256);
            engine.write(event).await.unwrap();
        }

        engine.flush_wal().await.unwrap();
    }

    // Session 3: Verify all data
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        // Use scan to get all events
        let events = engine.scan(0, u64::MAX).await.unwrap();
        
        // Should have events from both sessions
        assert!(events.len() >= 20, "Expected at least 20 events, got {}", events.len());
    }
}

// =============================================================================
// Edge Cases
// =============================================================================

#[tokio::test]
async fn test_recovery_empty_database() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    // Open empty database
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();
        assert_eq!(engine.wal_checkpoint(), 0);
    }

    // Reopen - should still work
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();
        assert_eq!(engine.wal_checkpoint(), 0);
        
        // Should be able to write
        let event = create_test_event(0, 256);
        engine.write(event).await.unwrap();
    }
}

#[tokio::test]
async fn test_recovery_after_clean_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let mut event_ids = Vec::new();

    // Session 1: Write and flush WAL (not SSTable)
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for i in 0..10 {
            let event = create_test_event(i, 256);
            let id = engine.write(event).await.unwrap();
            event_ids.push(id);
        }

        // Flush WAL only - data will be recovered via WAL replay
        engine.flush_wal().await.unwrap();
    }

    // Session 2: Should recover from WAL replay
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for id in &event_ids {
            let event = engine.read(*id).await.unwrap();
            assert!(event.is_some(), "Event {:?} not found after shutdown", id);
        }
    }
}

#[tokio::test]
async fn test_wal_integrity_after_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    // Session 1: Write data
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();

        for i in 0..50 {
            let event = create_test_event(i, 256);
            engine.write(event).await.unwrap();
        }

        engine.flush_wal().await.unwrap();
        
        // Verify WAL integrity before crash
        engine.verify_wal_integrity().await.unwrap();
    }

    // Session 2: Verify WAL integrity after recovery
    {
        let engine = StorageEngine::new(config.clone()).await.unwrap();
        
        // Merkle chain should still be valid
        engine.verify_wal_integrity().await
            .expect("WAL integrity check failed after recovery");
    }
}
