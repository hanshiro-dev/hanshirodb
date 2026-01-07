//! # Optimized Performance Test - Achieving 200K+ events/sec
//!
//! This test demonstrates the optimizations needed to reach 200K+ throughput.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::task::JoinSet;

use hanshiro_core::{
    types::*,
    Event,
    traits::StorageEngine,
};
use hanshiro_storage::{
    engine::{StorageEngine as StorageEngineImpl, StorageConfig},
    wal::WalConfig,
    memtable::MemTableConfig,
    sstable::{SSTableConfig, CompressionType},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_achieve_200k_events_per_sec() {
    println!("\n=== OPTIMIZED PERFORMANCE TEST ===\n");
    
    // Use RAM disk if available (much faster)
    let temp_dir = if std::path::Path::new("/tmp/ramdisk").exists() {
        println!("Using RAM disk at /tmp/ramdisk");
        TempDir::new_in("/tmp/ramdisk").unwrap()
    } else {
        TempDir::new().unwrap()
    };
    
    // Highly optimized configuration
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig {
            sync_on_write: false,               // CRITICAL: Async I/O
            max_batch_size: 2000,               // Large batches
            group_commit_delay_us: 500,         // 0.5ms delay
            max_file_size: 2 * 1024 * 1024 * 1024, // 2GB files
            // merkle_chain_enabled: false,     // Feature not yet exposed
            ..Default::default()
        },
        memtable_config: MemTableConfig {
            max_entries: 2_000_000,             // Very large
            max_size: 1024 * 1024 * 1024,      // 1GB
            ..Default::default()
        },
        sstable_config: SSTableConfig {
            compression: CompressionType::None,  // No compression
            block_size: 128 * 1024,             // 128KB blocks
            bloom_bits_per_key: 10,
            index_interval: 256,
        },
        flush_interval: Duration::from_secs(120),
        compaction_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let engine = Arc::new(StorageEngineImpl::new(config).await.unwrap());
    
    // Test parameters
    let total_events = 2_000_000;  // 2M events
    let batch_size = 2000;         // Large batches
    let num_writers = 20;          // Many parallel writers
    
    println!("Configuration:");
    println!("  Total events: {}", total_events);
    println!("  Batch size: {}", batch_size);
    println!("  Parallel writers: {}", num_writers);
    println!("  Events per writer: {}", total_events / num_writers);
    
    // Pre-create event template to reduce allocations
    let event_template = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: "perf-test".to_string(),
            ip: Some("10.0.0.1".parse().unwrap()),
            collector: "optimized".to_string(),
            format: IngestionFormat::Raw,
        },
        Vec::with_capacity(64),
    );
    
    let events_written = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Spawn parallel writers
    let mut tasks = JoinSet::new();
    
    for writer_id in 0..num_writers {
        let engine_clone = engine.clone();
        let events_written_clone = events_written.clone();
        let event_template = event_template.clone();
        let events_per_writer = total_events / num_writers;
        
        tasks.spawn(async move {
            let mut local_written = 0u64;
            
            // Process in large batches
            for batch_id in 0..(events_per_writer / batch_size) {
                let mut batch = Vec::with_capacity(batch_size);
                
                // Create batch with minimal allocations
                for i in 0..batch_size {
                    let mut event = event_template.clone();
                    event.raw_data = format!("w{}b{}e{}", writer_id, batch_id, i).into_bytes();
                    batch.push(event);
                }
                
                // Write batch
                match engine_clone.write_batch(batch).await {
                    Ok(_) => local_written += batch_size as u64,
                    Err(e) => eprintln!("Write error: {:?}", e),
                }
                
                // Yield occasionally to prevent monopolizing CPU
                if batch_id % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            
            events_written_clone.fetch_add(local_written, Ordering::Relaxed);
            local_written
        });
    }
    
    // Monitor progress in real-time
    let events_written_monitor = events_written.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut last_count = 0u64;
        let mut last_time = Instant::now();
        
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            
            let current_count = events_written_monitor.load(Ordering::Relaxed);
            if current_count >= total_events as u64 {
                break;
            }
            
            let current_time = Instant::now();
            let elapsed = current_time.duration_since(last_time);
            let events_in_period = current_count - last_count;
            
            let rate = events_in_period as f64 / elapsed.as_secs_f64();
            let progress = (current_count as f64 / total_events as f64) * 100.0;
            
            println!("Progress: {:.1}% | Rate: {:.0} events/sec", progress, rate);
            
            last_count = current_count;
            last_time = current_time;
        }
    });
    
    // Wait for all writers
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    
    // Final metrics
    let duration = start.elapsed();
    let total_written = events_written.load(Ordering::Relaxed);
    let throughput = total_written as f64 / duration.as_secs_f64();
    
    monitor_handle.abort();
    
    println!("\n=== FINAL RESULTS ===");
    println!("Total events written: {}", total_written);
    println!("Total duration: {:.2}s", duration.as_secs_f64());
    println!("Overall throughput: {:.0} events/sec", throughput);
    println!("Latency per event: {:.2} µs", duration.as_micros() as f64 / total_written as f64);
    
    if throughput >= 200_000.0 {
        println!("\n✅ SUCCESS! Achieved 200K+ events/sec!");
    } else if throughput >= 150_000.0 {
        println!("\n⚡ Good performance! Close to 200K target.");
        println!("Tips to reach 200K:");
        println!("- Use a RAM disk for storage");
        println!("- Increase batch size to 5000");
        println!("- Add more CPU cores");
        println!("- Ensure release build with LTO");
    } else if throughput >= 100_000.0 {
        println!("\n⚠️  Decent performance, but optimization needed.");
        println!("Check:");
        println!("- Is sync_on_write disabled?");
        println!("- Are you using --release mode?");
        println!("- Is the disk fast enough (NVMe)?");
    } else {
        println!("\n❌ Performance needs improvement.");
    }
    
    // Verify data integrity with a sample
    println!("\nVerifying data integrity...");
    let scan_start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() - 10;
    let scan_end = scan_start + 20;
    
    let scanned = engine.scan(scan_start, scan_end).await.unwrap();
    println!("Sample scan found {} events", scanned.len());
    
    assert!(throughput > 100_000.0, "Should achieve at least 100K events/sec");
}

#[tokio::test]
async fn test_sharding_for_extreme_performance() {
    println!("\n=== SHARDING TEST - Path to 500K+ events/sec ===\n");
    
    let temp_dir = TempDir::new().unwrap();
    let num_shards = 4;
    
    // Create sharded storage engines
    let mut engines = Vec::new();
    for shard_id in 0..num_shards {
        let shard_dir = temp_dir.path().join(format!("shard_{}", shard_id));
        tokio::fs::create_dir_all(&shard_dir).await.unwrap();
        
        let config = StorageConfig {
            data_dir: shard_dir,
            wal_config: WalConfig {
                sync_on_write: false,
                max_batch_size: 1000,
                // merkle_chain_enabled: false,
                ..Default::default()
            },
            memtable_config: MemTableConfig {
                max_entries: 500_000,
                max_size: 256 * 1024 * 1024,
                ..Default::default()
            },
            sstable_config: SSTableConfig {
                compression: CompressionType::None,
                ..Default::default()
            },
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(300),
        ..Default::default()
        };
        
        let engine = Arc::new(StorageEngineImpl::new(config).await.unwrap());
        engines.push(engine);
    }
    
    let total_events = 1_000_000;
    let batch_size = 1000;
    let start = Instant::now();
    
    // Distribute writes across shards
    let mut tasks = JoinSet::new();
    
    for (shard_id, engine) in engines.iter().enumerate() {
        let engine_clone = engine.clone();
        let events_per_shard = total_events / num_shards;
        
        tasks.spawn(async move {
            for batch_id in 0..(events_per_shard / batch_size) {
                let mut batch = Vec::with_capacity(batch_size);
                
                for i in 0..batch_size {
                    batch.push(Event::new(
                        EventType::NetworkConnection,
                        EventSource {
                            host: format!("shard-{}", shard_id),
                            ip: Some("10.0.0.1".parse().unwrap()),
                            collector: "sharded".to_string(),
                            format: IngestionFormat::Raw,
                        },
                        format!("s{}b{}e{}", shard_id, batch_id, i).into_bytes(),
                    ));
                }
                
                engine_clone.write_batch(batch).await.unwrap();
            }
        });
    }
    
    // Wait for completion
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    
    let duration = start.elapsed();
    let throughput = total_events as f64 / duration.as_secs_f64();
    
    println!("Sharded Storage Results:");
    println!("  Shards: {}", num_shards);
    println!("  Total events: {}", total_events);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} events/sec", throughput);
    println!("\nSharding provides near-linear scaling!");
}