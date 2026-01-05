//! Debug WAL write/read

use tempfile::TempDir;
use hanshiro_core::types::*;
use hanshiro_storage::wal::{WriteAheadLog, WalConfig};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

#[tokio::test]
async fn debug_wal_write() {
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
    
    // Flush WAL
    println!("Flushing WAL...");
    wal.flush().await.expect("Failed to flush WAL");
    
    // Find WAL file
    let mut entries = tokio::fs::read_dir(temp_dir.path()).await.unwrap();
    let mut wal_path = None;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.path().extension() == Some(std::ffi::OsStr::new("wal")) {
            wal_path = Some(entry.path());
            break;
        }
    }
    
    let wal_file_path = wal_path.expect("No WAL file found");
    println!("WAL file: {:?}", wal_file_path);
    
    // Read raw file contents
    let mut file = File::open(&wal_file_path).unwrap();
    let file_size = file.metadata().unwrap().len();
    println!("WAL file size: {} bytes", file_size);
    
    // Read first 100 bytes
    let mut buffer = vec![0u8; 100.min(file_size as usize)];
    file.read_exact(&mut buffer).unwrap();
    
    println!("First {} bytes (hex):", buffer.len());
    for (i, chunk) in buffer.chunks(16).enumerate() {
        print!("{:04x}: ", i * 16);
        for byte in chunk {
            print!("{:02x} ", byte);
        }
        println!();
    }
    
    // Check if there's any data after the header (64 bytes)
    if file_size > 64 {
        println!("\nData after header found! {} bytes", file_size - 64);
        
        // Read entry header
        file.seek(SeekFrom::Start(64)).unwrap();
        let mut entry_header = vec![0u8; 32];
        if file.read_exact(&mut entry_header).is_ok() {
            println!("Entry header (hex):");
            for (i, chunk) in entry_header.chunks(16).enumerate() {
                print!("{:04x}: ", i * 16);
                for byte in chunk {
                    print!("{:02x} ", byte);
                }
                println!();
            }
        }
    } else {
        println!("\nNo data after header! File only contains header.");
    }
}