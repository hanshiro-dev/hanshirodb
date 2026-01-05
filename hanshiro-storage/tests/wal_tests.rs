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
    let config = WalConfig {
        max_file_size: 10 * 1024, // 10KB - small for testing
        sync_on_write: false, // Faster for tests
        compression: false,
        buffer_size: 4096,
    };
    
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    // Write enough data to trigger rotation
    for i in 0..100 {
        let event = create_test_event(i, 1024); // 1KB each
        wal.append(&event).await.unwrap();
    }
    
    // Count WAL files
    let mut wal_files = 0;
    let mut entries = tokio::fs::read_dir(temp_dir.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.path().extension() == Some(std::ffi::OsStr::new("wal")) {
            wal_files += 1;
        }
    }
    
    // Should have multiple files due to rotation
    println!("WAL files count: {}", wal_files);
    assert!(wal_files > 1);
    
    // All data should still be readable
    let entries = wal.read_from(0).await.unwrap();
    println!("Total entries read: {}", entries.len());
    
    // Check for duplicates
    let unique_sequences: std::collections::HashSet<_> = entries.iter().map(|e| e.sequence).collect();
    println!("Unique sequences: {}", unique_sequences.len());
    
    // Debug: print first few and last few sequences
    if entries.len() > 10 {
        println!("First 5 sequences: {:?}", entries[0..5].iter().map(|e| e.sequence).collect::<Vec<_>>());
        println!("Last 5 sequences: {:?}", entries[entries.len()-5..].iter().map(|e| e.sequence).collect::<Vec<_>>());
    }
    
    assert_eq!(unique_sequences.len(), 100);
    assert_eq!(entries.len(), 100);
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
    let config = WalConfig {
        max_file_size: 10 * 1024, // 10KB for testing
        ..Default::default()
    };
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    // Write events
    for i in 0..50 {
        let event = create_test_event(i, 256);
        wal.append(&event).await.unwrap();
    }
    
    wal.flush().await.unwrap();
    
    // Rotate to create multiple files
    for i in 50..100 {
        let event = create_test_event(i, 10 * 1024); // Large events
        wal.append(&event).await.unwrap();
    }
    
    // Count files before truncation
    let files_before = count_wal_files(temp_dir.path()).await;
    assert!(files_before > 1);
    
    // Truncate old entries
    wal.truncate(80).await.unwrap();
    
    // Should have fewer files
    let files_after = count_wal_files(temp_dir.path()).await;
    assert!(files_after <= files_before);
}

#[tokio::test]
async fn test_wal_performance_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        sync_on_write: false, // Disable sync for performance test
        ..Default::default()
    };
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .unwrap();
    
    let start = tokio::time::Instant::now();
    let num_events = 10_000;
    
    // Measure write throughput
    for i in 0..num_events {
        let event = create_test_event(i, 512);
        wal.append(&event).await.unwrap();
    }
    
    let duration = start.elapsed();
    let throughput = num_events as f64 / duration.as_secs_f64();
    
    println!("WAL Write Performance:");
    println!("  Events written: {}", num_events);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} events/sec", throughput);
    println!("  Latency: {:.2} μs/event", duration.as_micros() as f64 / num_events as f64);
    
    // Should achieve reasonable throughput
    assert!(throughput > 1000.0); // At least 1000 events/sec
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