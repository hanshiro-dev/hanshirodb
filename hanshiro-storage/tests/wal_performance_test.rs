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
    println!("\n{}", "=".repeat(60));
    println!("=== WAL Performance Comparison ===");
    println!("{}\n", "=".repeat(60));
    
    let event_size = 512;
    
    // Test 1: Single-threaded with sync (baseline - worst case)
    {
        println!("Test 1: Single writes WITH sync (baseline)");
        println!("{}", "-".repeat(50));
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            group_commit_delay_us: 0, // Disable group commit
            max_batch_size: 1,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_events = 500; // Small count - sync is slow
        
        for i in 0..num_events {
            let event = create_test_event_no_metadata(i, event_size);
            wal.append(&event).await.unwrap();
        }
        
        let duration = start.elapsed();
        let throughput = num_events as f64 / duration.as_secs_f64();
        println!("  Events: {}", num_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Latency: {:.2} ms/event\n", duration.as_millis() as f64 / num_events as f64);
    }
    
    // Test 2: Group commit with sync (concurrent writers batched)
    {
        println!("Test 2: Group commit WITH sync (concurrent writers)");
        println!("{}", "-".repeat(50));
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            group_commit_delay_us: 2000, // 2ms batching window
            max_batch_size: 100,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_threads = 8;
        let events_per_thread = 500;
        let mut handles = JoinSet::new();
        
        for thread_id in 0..num_threads {
            let wal_clone = Arc::clone(&wal);
            handles.spawn(async move {
                for i in 0..events_per_thread {
                    let event = create_test_event_no_metadata(thread_id * 1000000 + i, event_size);
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
        println!("  Latency: {:.2} μs/event\n", duration.as_micros() as f64 / total_events as f64);
    }
    
    // Test 3: Explicit batch API with sync
    {
        println!("Test 3: Explicit append_batch() WITH sync");
        println!("{}", "-".repeat(50));
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_batches = 100;
        let batch_size = 100;
        
        for batch_num in 0..num_batches {
            let events: Vec<Event> = (0..batch_size)
                .map(|i| create_test_event_no_metadata(batch_num * batch_size + i, event_size))
                .collect();
            wal.append_batch(&events).await.unwrap();
        }
        
        let duration = start.elapsed();
        let total_events = num_batches * batch_size;
        let throughput = total_events as f64 / duration.as_secs_f64();
        println!("  Batches: {} x {} events", num_batches, batch_size);
        println!("  Total events: {}", total_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Latency: {:.2} μs/event\n", duration.as_micros() as f64 / total_events as f64);
    }
    
    // Test 4: Single-threaded without sync
    {
        println!("Test 4: Single writes WITHOUT sync");
        println!("{}", "-".repeat(50));
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: false,
            group_commit_delay_us: 0,
            max_batch_size: 1,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_events = 50_000;
        
        for i in 0..num_events {
            let event = create_test_event_no_metadata(i, event_size);
            wal.append(&event).await.unwrap();
        }
        
        let duration = start.elapsed();
        let throughput = num_events as f64 / duration.as_secs_f64();
        println!("  Events: {}", num_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Latency: {:.2} μs/event\n", duration.as_micros() as f64 / num_events as f64);
    }
    
    // Test 5: Explicit batch API without sync (maximum throughput)
    {
        println!("Test 5: Explicit append_batch() WITHOUT sync (max throughput)");
        println!("{}", "-".repeat(50));
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
        
        for batch_num in 0..num_batches {
            let events: Vec<Event> = (0..batch_size)
                .map(|i| create_test_event_no_metadata(batch_num * batch_size + i, event_size))
                .collect();
            wal.append_batch(&events).await.unwrap();
        }
        
        let duration = start.elapsed();
        let total_events = num_batches * batch_size;
        let throughput = total_events as f64 / duration.as_secs_f64();
        let mb_per_sec = (total_events as f64 * event_size as f64) / 1_000_000.0 / duration.as_secs_f64();
        println!("  Batches: {} x {} events", num_batches, batch_size);
        println!("  Total events: {}", total_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Bandwidth: {:.1} MB/sec", mb_per_sec);
        println!("  Latency: {:.2} μs/event\n", duration.as_micros() as f64 / total_events as f64);
    }
    
    // Test 6: Multi-threaded batch writes without sync
    {
        println!("Test 6: Multi-threaded append_batch() WITHOUT sync");
        println!("{}", "-".repeat(50));
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: false,
            buffer_size: 1024 * 1024,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(temp_dir.path(), config).await.unwrap());
        
        let start = Instant::now();
        let num_threads = 8;
        let batches_per_thread = 100;
        let batch_size = 100;
        let mut handles = JoinSet::new();
        
        for thread_id in 0..num_threads {
            let wal_clone = Arc::clone(&wal);
            handles.spawn(async move {
                for batch_num in 0..batches_per_thread {
                    let events: Vec<Event> = (0..batch_size)
                        .map(|i| create_test_event_no_metadata(
                            thread_id * 1000000 + batch_num * batch_size + i, 
                            512
                        ))
                        .collect();
                    wal_clone.append_batch(&events).await.unwrap();
                }
            });
        }
        
        while let Some(_) = handles.join_next().await {}
        
        let duration = start.elapsed();
        let total_events = num_threads * batches_per_thread * batch_size;
        let throughput = total_events as f64 / duration.as_secs_f64();
        let mb_per_sec = (total_events as f64 * event_size as f64) / 1_000_000.0 / duration.as_secs_f64();
        println!("  Threads: {}", num_threads);
        println!("  Total events: {}", total_events);
        println!("  Duration: {:?}", duration);
        println!("  Throughput: {:.0} events/sec", throughput);
        println!("  Bandwidth: {:.1} MB/sec", mb_per_sec);
        println!("  Latency: {:.2} μs/event\n", duration.as_micros() as f64 / total_events as f64);
    }
    
    println!("{}", "=".repeat(60));
    println!("=== Performance Summary ===");
    println!("{}", "=".repeat(60));
    println!("Target for SecOps: >100,000 events/sec");
    println!("LevelDB baseline: ~164,000 random writes/sec (async)");
    println!("LevelDB batch: ~221,000 entries/sec");
}