//! Simple WAL test to verify basic functionality

use tempfile::TempDir;
use hanshiro_core::types::*;
use hanshiro_storage::wal::{WriteAheadLog, WalConfig};

#[tokio::test]
async fn test_wal_basic() {
    // Create temp directory
    let temp_dir = TempDir::new().unwrap();
    println!("WAL directory: {:?}", temp_dir.path());
    
    // Create WAL
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(temp_dir.path(), config)
        .await
        .expect("Failed to create WAL");
    
    // Create a test event
    let event = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: "test-host".to_string(),
            ip: Some("127.0.0.1".parse().unwrap()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        b"test event data".to_vec(),
    );
    
    // Write event
    println!("Writing event to WAL...");
    let sequence = wal.append(&event).await.expect("Failed to append to WAL");
    println!("Event written with sequence: {}", sequence);
    
    // Flush WAL to ensure data is written
    println!("Flushing WAL...");
    wal.flush().await.expect("Failed to flush WAL");
    
    // List WAL files in directory
    let mut entries = tokio::fs::read_dir(temp_dir.path()).await.unwrap();
    println!("WAL files in directory:");
    while let Some(entry) = entries.next_entry().await.unwrap() {
        println!("  - {:?}", entry.path());
    }
    
    // Read back
    println!("Reading from WAL...");
    let entries = wal.read_from(0).await.expect("Failed to read from WAL");
    println!("Read {} entries", entries.len());
    
    // Also try reading from sequence 1
    println!("Reading from WAL with start_sequence=1...");
    let entries2 = wal.read_from(1).await.expect("Failed to read from WAL");
    println!("Read {} entries from sequence 1", entries2.len());
    
    // Verify
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].sequence, 0);
    
    // Deserialize and verify event
    let recovered: Event = rmp_serde::from_slice(&entries[0].data)
        .expect("Failed to deserialize event");
    assert_eq!(recovered.event_type, EventType::NetworkConnection);
    assert_eq!(recovered.raw_data, b"test event data");
    
    println!("✓ WAL test passed!");
}