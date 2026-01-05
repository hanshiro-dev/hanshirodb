//! WAL Performance Testing and Optimization

use tempfile::TempDir;
use hanshiro_core::types::*;
use hanshiro_storage::wal::{WriteAheadLog, WalConfig};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

fn create_test_event_no_metadata(id: u64, data_size: usize) -> Event {
    Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: format!("host-{}", id % 1000),
            ip: Some("127.0.0.1".parse().unwrap()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; data_size],
    )
}

#[tokio::test]
async fn test_wal_performance_comparison() {
    println!("\n=== WAL Performance Comparison ===\n");
    
    // Test 1: Single-threaded with sync
    {
        println!("Test 1: Single-threaded WITH sync_on_write");
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_events = 1000; // Less events for sync test
        
        for i in 0..num_events {
            let event = create_test_event_no_metadata(i, 512);
            wal.append(&event).await.unwrap();
        }
        
        let duration = start.elapsed();
        let throughput = num_events as f64 / duration.as_secs_f64();
        println!("  Events: {}", num_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Latency: {:.2} μs/event", duration.as_micros() as f64 / num_events as f64);
    }
    
    // Test 2: Single-threaded without sync
    {
        println!("\nTest 2: Single-threaded WITHOUT sync_on_write");
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: false,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_events = 100_000;
        
        for i in 0..num_events {
            let event = create_test_event_no_metadata(i, 512);
            wal.append(&event).await.unwrap();
        }
        
        let duration = start.elapsed();
        let throughput = num_events as f64 / duration.as_secs_f64();
        println!("  Events: {}", num_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Latency: {:.2} μs/event", duration.as_micros() as f64 / num_events as f64);
    }
    
    // Test 3: Multi-threaded without sync
    {
        println!("\nTest 3: Multi-threaded (8 threads) WITHOUT sync_on_write");
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: false,
            buffer_size: 256 * 1024, // Larger buffer
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_threads = 8;
        let events_per_thread = 100_000 / num_threads;
        let mut handles = JoinSet::new();
        
        for thread_id in 0..num_threads {
            let wal_clone = Arc::clone(&wal);
            handles.spawn(async move {
                for i in 0..events_per_thread {
                    let event = create_test_event_no_metadata(thread_id * 1000000 + i, 512);
                    wal_clone.append(&event).await.unwrap();
                }
            });
        }
        
        while let Some(_) = handles.join_next().await {}
        
        let duration = start.elapsed();
        let total_events = num_threads * events_per_thread;
        let throughput = total_events as f64 / duration.as_secs_f64();
        println!("  Threads: {}", num_threads);
        println!("  Events: {}", total_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Latency: {:.2} μs/event", duration.as_micros() as f64 / total_events as f64);
    }
    
    // Test 4: Batch writes (simulating optimized write path)
    {
        println!("\nTest 4: Batched writes WITHOUT sync_on_write");
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: false,
            buffer_size: 1024 * 1024, // 1MB buffer
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_batches = 1000;
        let batch_size = 100;
        
        for batch in 0..num_batches {
            // Simulate batch by writing multiple events quickly
            for i in 0..batch_size {
                let event = create_test_event_no_metadata(batch * batch_size + i, 512);
                wal.append(&event).await.unwrap();
            }
        }
        
        let duration = start.elapsed();
        let total_events = num_batches * batch_size;
        let throughput = total_events as f64 / duration.as_secs_f64();
        println!("  Batches: {}", num_batches);
        println!("  Batch size: {}", batch_size);
        println!("  Total events: {}", total_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Latency: {:.2} μs/event", duration.as_micros() as f64 / total_events as f64);
    }
    
    println!("\n=== Performance Summary ===");
    println!("Target: >100,000 events/sec for security use cases");
}