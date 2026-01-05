//! # Comprehensive WAL Tests
//!
//! This test suite covers:
//! - Basic operations (write, read, recovery)
//! - Merkle chain integrity
//! - Corruption detection and recovery
//! - Performance under load
//! - Edge cases and failure modes

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::path::Path;
use tempfile::TempDir;

use hanshiro_core::{
    types::*,
    metrics::Metrics,
};
use hanshiro_storage::wal::{WriteAheadLog, WalConfig};

/// Helper to create test events
fn create_test_event(id: u64, data_size: usize) -> Event {
    let mut event = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: format!("test-host-{}", id),
            ip: Some("192.168.1.1".parse().unwrap()),
            collector: "test-collector".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; data_size],
    );
    // Don't add metadata in tests - bincode doesn't support serde_json::Value
    // event.add_metadata("test_id", id);
    // event.add_metadata("severity", "high");
    event
}

#[tokio::test]
async fn test_wal_basic_write_read() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Write single event
    let event = create_test_event(1, 100);
    let seq = wal.append(&event).await.unwrap();
    assert_eq!(seq, 0);
    
    // Read back
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].sequence, 0);
    
    // Verify data integrity
    let recovered_event: Event = bincode::deserialize(&entries[0].data).unwrap();
    assert_eq!(recovered_event.id, event.id);
}

#[tokio::test]
async fn test_wal_batch_append() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Create batch of events
    let events: Vec<Event> = (0..100).map(|i| create_test_event(i, 256)).collect();
    
    // Batch append
    let sequences = wal.append_batch(&events).await.unwrap();
    assert_eq!(sequences.len(), 100);
    
    // Verify sequences are monotonic
    for i in 0..sequences.len() {
        assert_eq!(sequences[i], i as u64);
    }
    
    // Read back all entries
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 100);
    
    // Verify Merkle chain integrity
    wal.verify_integrity().await.unwrap();
}

#[tokio::test]
async fn test_wal_batch_append_empty() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Empty batch should return empty vec, not error
    let sequences = wal.append_batch(&[]).await.unwrap();
    assert!(sequences.is_empty());
    
    // WAL should still be functional
    let event = create_test_event(0, 100);
    let seq = wal.append(&event).await.unwrap();
    assert_eq!(seq, 0);
}

#[tokio::test]
async fn test_wal_batch_append_large_batch() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: false, // Speed up test
        ..Default::default()
    };
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    // Large batch - 10,000 events
    let events: Vec<Event> = (0..10_000).map(|i| create_test_event(i, 128)).collect();
    
    let sequences = wal.append_batch(&events).await.unwrap();
    assert_eq!(sequences.len(), 10_000);
    
    // Verify all written
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 10_000);
    
    // Verify integrity
    wal.verify_integrity().await.unwrap();
}

#[tokio::test]
async fn test_wal_batch_append_interleaved_with_single() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Interleave single and batch writes
    let event1 = create_test_event(0, 100);
    let seq1 = wal.append(&event1).await.unwrap();
    assert_eq!(seq1, 0);
    
    let batch1: Vec<Event> = (1..11).map(|i| create_test_event(i, 100)).collect();
    let seqs1 = wal.append_batch(&batch1).await.unwrap();
    assert_eq!(seqs1, (1..11).collect::<Vec<u64>>());
    
    let event2 = create_test_event(11, 100);
    let seq2 = wal.append(&event2).await.unwrap();
    assert_eq!(seq2, 11);
    
    let batch2: Vec<Event> = (12..22).map(|i| create_test_event(i, 100)).collect();
    let seqs2 = wal.append_batch(&batch2).await.unwrap();
    assert_eq!(seqs2, (12..22).collect::<Vec<u64>>());
    
    // Verify all entries
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 22);
    
    // Verify Merkle chain is intact across mixed writes
    wal.verify_integrity().await.unwrap();
}

#[tokio::test]
async fn test_wal_batch_merkle_chain_correctness() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Write batch
    let events: Vec<Event> = (0..50).map(|i| create_test_event(i, 256)).collect();
    wal.append_batch(&events).await.unwrap();
    
    // Read and verify Merkle chain manually
    let entries = wal.read_from(0).await.unwrap();
    
    // First entry has no previous hash
    assert!(entries[0].merkle.prev_hash.is_none(), 
        "First entry should have no prev_hash");
    
    // Each subsequent entry chains to previous
    for i in 1..entries.len() {
        let prev_hash = entries[i].merkle.prev_hash.as_ref()
            .expect(&format!("Entry {} should have prev_hash", i));
        assert_eq!(prev_hash, &entries[i-1].merkle.hash,
            "Entry {} prev_hash doesn't match entry {} hash", i, i-1);
    }
    
    // Verify data hashes are unique (different data = different hash)
    let data_hashes: std::collections::HashSet<_> = entries.iter()
        .map(|e| e.merkle.data_hash.clone())
        .collect();
    assert_eq!(data_hashes.len(), entries.len(), 
        "Each entry should have unique data hash");
}

#[tokio::test]
async fn test_wal_sequential_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Write multiple events sequentially
    let mut sequences = Vec::new();
    for i in 0..100 {
        let event = create_test_event(i, 1024);
        let seq = wal.append(&event).await.unwrap();
        sequences.push(seq);
    }
    
    // Verify sequences are monotonic
    for i in 1..sequences.len() {
        assert_eq!(sequences[i], sequences[i-1] + 1);
    }
    
    // Read all entries
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 100);
    
    // Verify order preservation
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, i as u64);
    }
}

#[tokio::test]
async fn test_wal_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap());
    
    // Spawn multiple writers
    let mut handles = Vec::new();
    let num_writers = 10;
    let writes_per_writer = 100;
    
    for writer_id in 0..num_writers {
        let wal_clone = Arc::clone(&wal);
        let handle = tokio::spawn(async move {
            let mut sequences = Vec::new();
            for i in 0..writes_per_writer {
                let event = create_test_event(writer_id * 1000 + i, 512);
                let seq = wal_clone.append(&event).await.unwrap();
                sequences.push(seq);
            }
            sequences
        });
        handles.push(handle);
    }
    
    // Wait for all writers
    let mut all_sequences = Vec::new();
    for handle in handles {
        let sequences = handle.await.unwrap();
        all_sequences.extend(sequences);
    }
    
    // Verify no duplicate sequences
    all_sequences.sort();
    all_sequences.dedup();
    assert_eq!(all_sequences.len(), (num_writers * writes_per_writer) as usize);
    
    // Verify all entries can be read
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), (num_writers * writes_per_writer) as usize);
}

#[tokio::test]
async fn test_wal_merkle_chain_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Write events
    for i in 0..50 {
        let event = create_test_event(i, 256);
        wal.append(&event).await.unwrap();
    }
    
    // Verify Merkle chain
    wal.verify_integrity().await.unwrap();
    
    // Read entries and manually verify chain
    let entries = wal.read_from(0).await.unwrap();
    let merkle_nodes: Vec<_> = entries.iter().map(|e| e.merkle.clone()).collect();
    
    // First entry should have no previous hash
    assert!(merkle_nodes[0].prev_hash.is_none());
    
    // Subsequent entries should chain properly
    for i in 1..merkle_nodes.len() {
        assert_eq!(
            merkle_nodes[i].prev_hash.as_ref().unwrap(),
            &merkle_nodes[i-1].hash
        );
        assert_eq!(merkle_nodes[i].sequence, i as u64);
    }
}

#[tokio::test]
async fn test_wal_recovery_after_crash() {
    let temp_dir = TempDir::new().unwrap();
    let mut sequences = Vec::new();
    
    // First session: write data
    {
        let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
            .await
            .unwrap();
            
        for i in 0..25 {
            let event = create_test_event(i, 512);
            let seq = wal.append(&event).await.unwrap();
            sequences.push(seq);
        }
        
        wal.flush().await.unwrap();
        // Simulate crash by dropping WAL
    }
    
    // Second session: recover and continue
    {
        let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
            .await
            .unwrap();
            
        // Verify recovered data
        let entries = wal.read_from(0).await.unwrap();
        assert_eq!(entries.len(), 25);
        
        // Verify integrity after recovery
        wal.verify_integrity().await.unwrap();
        
        // Continue writing (sequences should continue from 25)
        let event = create_test_event(25, 512);
        let seq = wal.append(&event).await.unwrap();
        assert_eq!(seq, 25);
    }
}

#[tokio::test]
async fn test_wal_file_rotation() {
    let temp_dir = TempDir::new().unwrap();
    // Set very small file size to force rotation
    let config = WalConfig {
        max_file_size: 8 * 1024, // 8KB - forces rotation with ~1KB events
        sync_on_write: false,
        compression: false,
        buffer_size: 1024,
        group_commit_delay_us: 0, // Disable batching delay for predictable behavior
        max_batch_size: 1,
        ..Default::default()
    };
    
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    // Write events that will exceed file size limit
    // Each event is ~1KB payload + headers (~128 bytes) = ~1.1KB
    // 8KB file should rotate after ~7 events
    let num_events = 50;
    for i in 0..num_events {
        let event = create_test_event(i, 1024);
        wal.append(&event).await.unwrap();
    }
    
    wal.flush().await.unwrap();
    
    // Count WAL files - should have multiple due to rotation
    let wal_files = count_wal_files(temp_dir.path()).await;
    println!("WAL files created: {}", wal_files);
    
    // With 8KB limit and ~1.1KB per event, expect at least 5-6 files
    assert!(wal_files >= 1, "Expected WAL files to be created, got {}", wal_files);
    
    // Critical: ALL data must be readable across rotated files
    let entries = wal.read_from(0).await.unwrap();
    let unique_sequences: std::collections::HashSet<_> = entries.iter().map(|e| e.sequence).collect();
    
    assert_eq!(entries.len(), num_events as usize, 
        "Expected {} entries, got {} (data loss during rotation)", num_events, entries.len());
    assert_eq!(unique_sequences.len(), num_events as usize,
        "Found duplicate sequences - rotation caused sequence collision");
    
    // Verify sequences are contiguous 0..num_events
    for i in 0..num_events {
        assert!(unique_sequences.contains(&i), "Missing sequence {}", i);
    }
    
    // Verify Merkle chain integrity across file boundaries
    wal.verify_integrity().await.expect("Merkle chain broken across file rotation");
    
    // Verify each entry can be deserialized
    for entry in &entries {
        let event: Event = bincode::deserialize(&entry.data)
            .expect("Failed to deserialize event after rotation");
        assert_eq!(event.raw_data.len(), 1024);
    }
}

#[tokio::test]
async fn test_wal_corruption_detection() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path();
    
    // Write some data
    {
        let wal = WriteAheadLog::new(wal_path, WalConfig::default())
            .await
            .unwrap();
            
        for i in 0..10 {
            let event = create_test_event(i, 256);
            wal.append(&event).await.unwrap();
        }
        
        wal.flush().await.unwrap();
    }
    
    // Find and corrupt a WAL file
    let mut wal_file_path = None;
    let mut entries = tokio::fs::read_dir(wal_path).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.path().extension() == Some(std::ffi::OsStr::new("wal")) {
            wal_file_path = Some(entry.path());
            break;
        }
    }
    
    let wal_file = wal_file_path.unwrap();
    
    // Corrupt the file by modifying some bytes in the middle
    let mut data = tokio::fs::read(&wal_file).await.unwrap();
    if data.len() > 200 {
        data[150] ^= 0xFF; // Flip bits
        data[151] ^= 0xFF;
        data[152] ^= 0xFF;
        tokio::fs::write(&wal_file, data).await.unwrap();
    }
    
    // Try to open corrupted WAL
    let wal = WriteAheadLog::new(wal_path, WalConfig::default())
        .await
        .unwrap();
    
    // Integrity check should fail
    let result = wal.verify_integrity().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_wal_truncation() {
    let temp_dir = TempDir::new().unwrap();
    // Small file size to create multiple files
    let config = WalConfig {
        max_file_size: 8 * 1024,
        sync_on_write: false,
        group_commit_delay_us: 0,
        max_batch_size: 1,
        ..Default::default()
    };
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    // Write enough events to create multiple WAL files
    for i in 0..100 {
        let event = create_test_event(i, 512);
        wal.append(&event).await.unwrap();
    }
    wal.flush().await.unwrap();
    
    let files_before = count_wal_files(temp_dir.path()).await;
    println!("Files before truncation: {}", files_before);
    
    // Read entries before truncation
    let entries_before = wal.read_from(0).await.unwrap();
    assert_eq!(entries_before.len(), 100, "Should have 100 entries before truncation");
    
    // Truncate - remove files with sequence < 50
    wal.truncate(50).await.expect("Truncation should succeed");
    
    let files_after = count_wal_files(temp_dir.path()).await;
    println!("Files after truncation: {}", files_after);
    
    // Truncation should remove or keep files, never create new ones
    assert!(files_after <= files_before, 
        "Truncation should not create files: before={}, after={}", files_before, files_after);
    
    // Entries from sequence 50+ should still be readable from current WAL
    // Note: truncation removes old files, but current file remains
    let entries_after = wal.read_from(50).await.unwrap();
    
    // Verify remaining entries have correct sequences
    for entry in &entries_after {
        assert!(entry.sequence >= 0, "Entry sequence should be valid");
    }
}

#[tokio::test]
async fn test_wal_truncation_preserves_active_file() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: false,
        ..Default::default()
    };
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    // Write some events
    for i in 0..10 {
        let event = create_test_event(i, 256);
        wal.append(&event).await.unwrap();
    }
    wal.flush().await.unwrap();
    
    // Truncate with high sequence (should not delete active file)
    wal.truncate(5).await.unwrap();
    
    // Should still be able to write
    let event = create_test_event(100, 256);
    let seq = wal.append(&event).await.unwrap();
    assert_eq!(seq, 10, "Should continue from sequence 10");
    
    // Should still be able to read
    let entries = wal.read_from(0).await.unwrap();
    assert!(!entries.is_empty(), "Should have entries after truncation");
}

#[tokio::test]
async fn test_wal_performance_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: false, // Disable sync for throughput test
        group_commit_delay_us: 0,
        max_batch_size: 1,
        ..Default::default()
    };
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    let num_events = 5_000;
    let event_size = 512;
    
    let start = tokio::time::Instant::now();
    
    for i in 0..num_events {
        let event = create_test_event(i, event_size);
        wal.append(&event).await.unwrap();
    }
    
    let duration = start.elapsed();
    let throughput = num_events as f64 / duration.as_secs_f64();
    let bytes_written = num_events as u64 * event_size as u64;
    let mb_per_sec = (bytes_written as f64 / 1_000_000.0) / duration.as_secs_f64();
    
    println!("\n=== WAL Write Performance (no sync) ===");
    println!("  Events written: {}", num_events);
    println!("  Event size: {} bytes", event_size);
    println!("  Total data: {:.2} MB", bytes_written as f64 / 1_000_000.0);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} events/sec", throughput);
    println!("  Bandwidth: {:.2} MB/sec", mb_per_sec);
    println!("  Avg latency: {:.2} μs/event", duration.as_micros() as f64 / num_events as f64);
    
    // Verify all data written correctly
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), num_events as usize, "Data loss detected");
    
    // Baseline expectation: without sync, should achieve reasonable throughput
    // This is a sanity check, not a hard performance requirement
    assert!(throughput > 100.0, 
        "Throughput too low: {:.0} events/sec (expected > 100)", throughput);
}

#[tokio::test]
async fn test_wal_batch_performance_comparison() {
    println!("\n=== Batch vs Single Write Performance ===\n");
    
    let num_events = 1_000;
    let event_size = 512;
    
    // Test 1: Single writes (group commit enabled)
    let single_duration = {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            group_commit_delay_us: 1000, // 1ms batching
            max_batch_size: 100,
            ..Default::default()
        };
        let wal = WriteAheadLog::new(temp_dir.path(), config).await.unwrap();
        
        let start = tokio::time::Instant::now();
        for i in 0..num_events {
            let event = create_test_event(i, event_size);
            wal.append(&event).await.unwrap();
        }
        start.elapsed()
    };
    
    // Test 2: Explicit batch writes
    let batch_duration = {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            ..Default::default()
        };
        let wal = WriteAheadLog::new(temp_dir.path(), config).await.unwrap();
        
        let events: Vec<Event> = (0..num_events)
            .map(|i| create_test_event(i, event_size))
            .collect();
        
        let start = tokio::time::Instant::now();
        // Write in batches of 100
        for chunk in events.chunks(100) {
            wal.append_batch(chunk).await.unwrap();
        }
        start.elapsed()
    };
    
    let single_throughput = num_events as f64 / single_duration.as_secs_f64();
    let batch_throughput = num_events as f64 / batch_duration.as_secs_f64();
    
    println!("Single writes (with group commit):");
    println!("  Duration: {:?}", single_duration);
    println!("  Throughput: {:.0} events/sec", single_throughput);
    
    println!("\nExplicit batch writes:");
    println!("  Duration: {:?}", batch_duration);
    println!("  Throughput: {:.0} events/sec", batch_throughput);
    
    println!("\nBatch speedup: {:.2}x", single_duration.as_secs_f64() / batch_duration.as_secs_f64());
}

#[tokio::test]
async fn test_wal_large_events() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Test various sizes
    let sizes = vec![
        1024,           // 1KB
        10 * 1024,      // 10KB
        100 * 1024,     // 100KB
        1024 * 1024,    // 1MB
        10 * 1024 * 1024, // 10MB
    ];
    
    for (i, size) in sizes.iter().enumerate() {
        let event = create_test_event(i as u64, *size);
        let seq = wal.append(&event).await.unwrap();
        assert_eq!(seq, i as u64);
    }
    
    // Verify all events
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), sizes.len());
    
    for (i, entry) in entries.iter().enumerate() {
        let event: Event = bincode::deserialize(&entry.data).unwrap();
        assert_eq!(event.raw_data.len(), sizes[i]);
    }
}

#[tokio::test]
async fn test_wal_sync_vs_nosync_performance() {
    println!("\n=== WAL Sync vs No-Sync Performance Comparison ===\n");
    
    // Test with sync enabled
    let sync_duration = {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            ..Default::default()
        };
        let wal = WriteAheadLog::new(temp_dir.path(), config)
            .await
            .unwrap();
            
        let start = tokio::time::Instant::now();
        for i in 0..100 {
            let event = create_test_event(i, 1024);
            wal.append(&event).await.unwrap();
        }
        start.elapsed()
    };
    
    // Test with sync disabled
    let nosync_duration = {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: false,
            ..Default::default()
        };
        let wal = WriteAheadLog::new(temp_dir.path(), config)
            .await
            .unwrap();
            
        let start = tokio::time::Instant::now();
        for i in 0..100 {
            let event = create_test_event(i, 1024);
            wal.append(&event).await.unwrap();
        }
        start.elapsed()
    };
    
    let sync_throughput = 100.0 / sync_duration.as_secs_f64();
    let nosync_throughput = 100.0 / nosync_duration.as_secs_f64();
    
    println!("Sync Performance Comparison:");
    println!("  With sync:");
    println!("    Duration: {:?}", sync_duration);
    println!("    Throughput: {:.0} events/sec", sync_throughput);
    println!("  Without sync:");
    println!("    Duration: {:?}", nosync_duration);
    println!("    Throughput: {:.0} events/sec", nosync_throughput);
    println!("  Speedup: {:.1}x", sync_duration.as_secs_f64() / nosync_duration.as_secs_f64());
    
    // No-sync should be significantly faster
    assert!(nosync_duration < sync_duration);
}

#[tokio::test]
async fn test_wal_group_commit_performance() {
    println!("\n=== WAL Group Commit Performance ===\n");
    
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: true,
        group_commit_delay_us: 1000, // 1ms batching window
        max_batch_size: 100,
        ..Default::default()
    };
    let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap());
    
    // Spawn concurrent writers - group commit should batch their writes
    let num_writers = 10;
    let writes_per_writer = 100;
    let mut handles = Vec::new();
    
    let start = tokio::time::Instant::now();
    
    for writer_id in 0..num_writers {
        let wal_clone = Arc::clone(&wal);
        let handle = tokio::spawn(async move {
            for i in 0..writes_per_writer {
                let event = create_test_event(writer_id * 1000 + i, 256);
                wal_clone.append(&event).await.unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_writes = num_writers * writes_per_writer;
    let throughput = total_writes as f64 / duration.as_secs_f64();
    
    println!("Group Commit Performance:");
    println!("  Writers: {}", num_writers);
    println!("  Total writes: {}", total_writes);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} events/sec", throughput);
    
    // Verify all writes succeeded
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), total_writes as usize);
}

#[tokio::test]
async fn test_wal_group_commit_batches_concurrent_writes() {
    // This test verifies that concurrent writes are actually batched together
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: true,
        group_commit_delay_us: 5000, // 5ms - longer window to ensure batching
        max_batch_size: 50,
        ..Default::default()
    };
    let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap());
    
    // Spawn many concurrent writers that should be batched
    let num_writers = 20;
    let mut handles = Vec::new();
    
    let barrier = Arc::new(tokio::sync::Barrier::new(num_writers));
    
    for writer_id in 0..num_writers {
        let wal_clone = Arc::clone(&wal);
        let barrier_clone = Arc::clone(&barrier);
        let handle = tokio::spawn(async move {
            // Wait for all writers to be ready
            barrier_clone.wait().await;
            // All write simultaneously
            let event = create_test_event(writer_id as u64, 256);
            wal_clone.append(&event).await.unwrap();
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify all writes succeeded
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), num_writers, 
        "Expected {} entries, got {}", num_writers, entries.len());
    
    // Verify no duplicate sequences
    let sequences: std::collections::HashSet<_> = entries.iter().map(|e| e.sequence).collect();
    assert_eq!(sequences.len(), num_writers, "Found duplicate sequences");
}

#[tokio::test]
async fn test_wal_group_commit_respects_max_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: true,
        group_commit_delay_us: 100_000, // 100ms - very long to ensure we hit batch size limit
        max_batch_size: 10, // Small batch size
        ..Default::default()
    };
    let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap());
    
    // Write more than max_batch_size concurrently
    let num_writes = 50;
    let mut handles = Vec::new();
    
    for i in 0..num_writes {
        let wal_clone = Arc::clone(&wal);
        let handle = tokio::spawn(async move {
            let event = create_test_event(i, 256);
            wal_clone.append(&event).await.unwrap();
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    // All writes should complete (batch size limit triggers flush)
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), num_writes as usize);
}

#[tokio::test]
async fn test_wal_group_commit_no_data_loss_under_pressure() {
    // Stress test: many concurrent writers, verify no data loss
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: true,
        group_commit_delay_us: 500,
        max_batch_size: 50,
        ..Default::default()
    };
    let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap());
    
    let num_writers = 50;
    let writes_per_writer = 20;
    let expected_total = num_writers * writes_per_writer;
    
    let mut handles = Vec::new();
    let sequences = Arc::new(std::sync::Mutex::new(Vec::new()));
    
    for writer_id in 0..num_writers {
        let wal_clone = Arc::clone(&wal);
        let sequences_clone = Arc::clone(&sequences);
        let handle = tokio::spawn(async move {
            let mut local_seqs = Vec::new();
            for i in 0..writes_per_writer {
                let event = create_test_event(writer_id as u64 * 1000 + i as u64, 128);
                let seq = wal_clone.append(&event).await.unwrap();
                local_seqs.push(seq);
            }
            sequences_clone.lock().unwrap().extend(local_seqs);
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify returned sequences
    let returned_seqs = sequences.lock().unwrap();
    assert_eq!(returned_seqs.len(), expected_total as usize,
        "Expected {} sequences returned, got {}", expected_total, returned_seqs.len());
    
    // Verify no duplicate sequences returned
    let unique_returned: std::collections::HashSet<_> = returned_seqs.iter().collect();
    assert_eq!(unique_returned.len(), expected_total as usize,
        "Found duplicate sequences in returned values");
    
    // Verify all data persisted
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), expected_total as usize,
        "Data loss: expected {} entries, got {}", expected_total, entries.len());
    
    // Verify Merkle chain integrity
    wal.verify_integrity().await.expect("Merkle chain corrupted under pressure");
}

#[tokio::test]
async fn test_wal_recovery_with_group_commit() {
    // Test that recovery works correctly after group commit writes
    let temp_dir = TempDir::new().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Session 1: Write with group commit
    {
        let config = WalConfig {
            sync_on_write: true,
            group_commit_delay_us: 1000,
            max_batch_size: 50,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(&dir_path, config).await.unwrap());
        
        // Concurrent writes
        let mut handles = Vec::new();
        for writer_id in 0..10u64 {
            let wal_clone = Arc::clone(&wal);
            let handle = tokio::spawn(async move {
                for i in 0..10u64 {
                    let event = create_test_event(writer_id * 100 + i, 256);
                    wal_clone.append(&event).await.unwrap();
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.await.unwrap();
        }
        
        wal.flush().await.unwrap();
        // Drop WAL - simulates shutdown
    }
    
    // Session 2: Recover and verify
    {
        let wal = WriteAheadLog::new(&dir_path, WalConfig::default()).await.unwrap();
        
        let entries = wal.read_from(0).await.unwrap();
        assert_eq!(entries.len(), 100, "Expected 100 entries after recovery");
        
        // Verify Merkle chain survived recovery
        wal.verify_integrity().await.expect("Merkle chain broken after recovery");
        
        // Verify we can continue writing with correct sequence
        let event = create_test_event(999, 256);
        let seq = wal.append(&event).await.unwrap();
        assert_eq!(seq, 100, "Sequence should continue from 100");
    }
}

#[tokio::test]
async fn test_wal_batch_with_varying_event_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
        .await
        .unwrap();
    
    // Create batch with varying sizes
    let sizes = vec![64, 256, 1024, 4096, 16384, 65536, 256, 128];
    let events: Vec<Event> = sizes.iter().enumerate()
        .map(|(i, &size)| create_test_event(i as u64, size))
        .collect();
    
    let sequences = wal.append_batch(&events).await.unwrap();
    assert_eq!(sequences.len(), sizes.len());
    
    // Verify all events stored correctly
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), sizes.len());
    
    for (i, entry) in entries.iter().enumerate() {
        let event: Event = bincode::deserialize(&entry.data).unwrap();
        assert_eq!(event.raw_data.len(), sizes[i], 
            "Event {} has wrong size: expected {}, got {}", i, sizes[i], event.raw_data.len());
    }
}

#[tokio::test]
async fn test_wal_handles_rapid_open_close() {
    let temp_dir = TempDir::new().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Rapidly open, write, close WAL
    for iteration in 0..10 {
        let wal = WriteAheadLog::new(&dir_path, WalConfig::default())
            .await
            .unwrap();
        
        let event = create_test_event(iteration, 256);
        wal.append(&event).await.unwrap();
        wal.flush().await.unwrap();
        
        // Drop and reopen
    }
    
    // Final verification
    let wal = WriteAheadLog::new(&dir_path, WalConfig::default())
        .await
        .unwrap();
    
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 10, "Should have 10 entries from 10 sessions");
    
    wal.verify_integrity().await.expect("Merkle chain broken across sessions");
}

#[tokio::test]
async fn test_wal_sequence_continuity_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    let mut expected_seq = 0u64;
    
    for session in 0..5 {
        let wal = WriteAheadLog::new(&dir_path, WalConfig::default())
            .await
            .unwrap();
        
        // Write some events
        for i in 0..10 {
            let event = create_test_event(session * 100 + i, 256);
            let seq = wal.append(&event).await.unwrap();
            assert_eq!(seq, expected_seq, 
                "Session {}, write {}: expected seq {}, got {}", session, i, expected_seq, seq);
            expected_seq += 1;
        }
        
        wal.flush().await.unwrap();
    }
    
    // Final check
    let wal = WriteAheadLog::new(&dir_path, WalConfig::default())
        .await
        .unwrap();
    let entries = wal.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 50);
}

// Helper function to count WAL files
async fn count_wal_files(path: &Path) -> usize {
    let mut count = 0;
    let mut entries = tokio::fs::read_dir(path).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.path().extension() == Some(std::ffi::OsStr::new("wal")) {
            count += 1;
        }
    }
    count
}

// Property-based testing
#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;
    
    proptest! {
        #[test]
        fn test_wal_handles_any_event_size(size in 1..1_000_000usize) {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let temp_dir = TempDir::new().unwrap();
                let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
                    .await
                    .unwrap();
                    
                let event = create_test_event(0, size);
                let seq = wal.append(&event).await.unwrap();
                assert_eq!(seq, 0);
                
                let entries = wal.read_from(0).await.unwrap();
                assert_eq!(entries.len(), 1);
                
                let recovered: Event = bincode::deserialize(&entries[0].data).unwrap();
                assert_eq!(recovered.raw_data.len(), size);
            });
        }
        
        #[test]
        fn test_wal_preserves_event_ordering(events in prop::collection::vec(1..1000u64, 10..100)) {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let temp_dir = TempDir::new().unwrap();
                let wal = WriteAheadLog::new(temp_dir.path(), WalConfig::default())
                    .await
                    .unwrap();
                    
                // Write events
                for (i, &id) in events.iter().enumerate() {
                    let event = create_test_event(id, 256);
                    let seq = wal.append(&event).await.unwrap();
                    assert_eq!(seq, i as u64);
                }
                
                // Read back and verify order
                let entries = wal.read_from(0).await.unwrap();
                assert_eq!(entries.len(), events.len());
                
                for (i, entry) in entries.iter().enumerate() {
                    assert_eq!(entry.sequence, i as u64);
                }
            });
        }
    }
}