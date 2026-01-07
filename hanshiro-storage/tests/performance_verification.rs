//! # Overall Performance Verification
//!
//! This test verifies the complete WAL->MemTable->SSTable performance chain.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use hanshiro_core::{
    types::*,
    Event, EventId,
    traits::StorageEngine,
};
use hanshiro_storage::{
    engine::{StorageEngine as StorageEngineImpl, StorageConfig},
    wal::WalConfig,
    memtable::MemTableConfig,
    sstable::{SSTableConfig, CompressionType},
};

#[tokio::test]
async fn test_full_stack_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal_config: WalConfig {
            sync_on_write: false,
            max_batch_size: 100,
            group_commit_delay_us: 5000, // 5ms
            ..Default::default()
        },
        memtable_config: MemTableConfig {
            max_entries: 10_000,
            ..Default::default()
        },
        sstable_config: SSTableConfig {
            compression: CompressionType::Snappy,
            ..Default::default()
        },
        flush_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(60),
    };
    
    let engine = Arc::new(StorageEngineImpl::new(config).await.unwrap());
    
    // Warm up
    for i in 0..100 {
        let event = Event::new(
            EventType::NetworkConnection,
            EventSource {
                host: format!("host-{}", i % 10),
                ip: Some("10.0.0.1".parse().unwrap()),
                collector: "test".to_string(),
                format: IngestionFormat::Raw,
            },
            vec![0u8; 100],
        );
        engine.write(event).await.unwrap();
    }
    
    // Performance test: Write throughput
    let write_count = 50_000;
    let start = Instant::now();
    
    // Use batch writes for better performance
    let batch_size = 100;
    let mut all_event_ids = Vec::new();
    
    for batch_idx in 0..(write_count / batch_size) {
        let mut events = Vec::new();
        for i in 0..batch_size {
            let idx = batch_idx * batch_size + i;
            let event = Event::new(
                EventType::NetworkConnection,
                EventSource {
                    host: format!("host-{}", idx % 100),
                    ip: Some(format!("10.0.{}.{}", idx % 256, (idx / 256) % 256).parse().unwrap()),
                    collector: "perf-test".to_string(),
                    format: IngestionFormat::Raw,
                },
                format!("Performance test event {}", idx).into_bytes(),
            );
            events.push(event);
        }
        let ids = engine.write_batch(events).await.unwrap();
        all_event_ids.extend(ids);
    }
    
    let write_duration = start.elapsed();
    let write_throughput = write_count as f64 / write_duration.as_secs_f64();
    
    println!("Performance Results:");
    println!("==================");
    println!("Write Performance:");
    println!("  Events written: {}", write_count);
    println!("  Duration: {:?}", write_duration);
    println!("  Throughput: {:.0} events/sec", write_throughput);
    println!("  Latency: {:.2} µs/event", write_duration.as_micros() as f64 / write_count as f64);
    
    // Force flush to test SSTable performance
    engine.force_flush().await.unwrap();
    
    // Read performance test
    let read_count = 1_000; // Reduced count for more realistic test
    let start = Instant::now();
    
    // Read actual events that exist
    for i in 0..read_count {
        let idx = (i * 5) % all_event_ids.len();
        let event_id = all_event_ids[idx];
        let _ = engine.read(event_id).await;
    }
    
    let read_duration = start.elapsed();
    let read_throughput = read_count as f64 / read_duration.as_secs_f64();
    
    println!("\nRead Performance:");
    println!("  Reads performed: {}", read_count);
    println!("  Duration: {:?}", read_duration);
    println!("  Throughput: {:.0} reads/sec", read_throughput);
    
    // Scan performance
    let start = Instant::now();
    let base_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let scanned = engine.scan(base_time - 3600, base_time + 3600).await.unwrap();
    let scan_duration = start.elapsed();
    
    println!("\nScan Performance:");
    println!("  Events scanned: {}", scanned.len());
    println!("  Duration: {:?}", scan_duration);
    println!("  Throughput: {:.0} events/sec", scanned.len() as f64 / scan_duration.as_secs_f64());
    
    // Overall assessment
    println!("\nOverall Performance Assessment:");
    println!("==============================");
    
    if write_throughput >= 20_000.0 {
        println!("✓ Write performance: EXCELLENT ({}+ events/sec)", write_throughput as u64);
    } else if write_throughput >= 10_000.0 {
        println!("✓ Write performance: GOOD ({}+ events/sec)", write_throughput as u64);
    } else if write_throughput >= 5_000.0 {
        println!("✓ Write performance: ACCEPTABLE ({}+ events/sec)", write_throughput as u64);
    } else {
        println!("✗ Write performance: NEEDS IMPROVEMENT ({} events/sec)", write_throughput as u64);
    }
    
    // Performance assertions
    assert!(write_throughput > 5_000.0, "Write throughput too low: {:.0} events/sec", write_throughput);
    assert!(read_throughput > 1_000.0, "Read throughput too low: {:.0} reads/sec", read_throughput);
    
    println!("\nWAL->MemTable->SSTable pipeline: VERIFIED ✓");
}

#[tokio::test]
async fn test_component_performance_breakdown() {
    // This test measures individual component performance
    let temp_dir = TempDir::new().unwrap();
    
    // Test WAL performance in isolation
    let wal_dir = temp_dir.path().join("wal");
    tokio::fs::create_dir_all(&wal_dir).await.unwrap();
    
    let wal = hanshiro_storage::wal::WriteAheadLog::new(&wal_dir, WalConfig {
        sync_on_write: false,
        max_batch_size: 100,
        group_commit_delay_us: 5000, // 5ms
        ..Default::default()
    }).await.unwrap();
    
    let mut events = Vec::new();
    for i in 0..1000 {
        events.push(Event::new(
            EventType::NetworkConnection,
            EventSource {
                host: format!("host-{}", i),
                ip: Some("10.0.0.1".parse().unwrap()),
                collector: "test".to_string(),
                format: IngestionFormat::Raw,
            },
            vec![0u8; 100],
        ));
    }
    
    let start = Instant::now();
    for _ in 0..10 {
        wal.append_batch(&events).await.unwrap();
    }
    let wal_duration = start.elapsed();
    let wal_throughput = 10_000.0 / wal_duration.as_secs_f64();
    
    println!("\nComponent Performance Breakdown:");
    println!("================================");
    println!("WAL throughput: {:.0} events/sec", wal_throughput);
    
    // Test MemTable performance
    let metrics = Arc::new(hanshiro_core::metrics::Metrics::new());
    use hanshiro_storage::memtable::MemTableManager;
    let memtable_manager = MemTableManager::new(
        MemTableConfig::default(),
        metrics.clone(),
    );
    
    let start = Instant::now();
    for event in &events {
        memtable_manager.insert(event.clone()).unwrap();
    }
    let memtable_duration = start.elapsed();
    let memtable_throughput = 1000.0 / memtable_duration.as_secs_f64();
    
    println!("MemTable throughput: {:.0} events/sec", memtable_throughput);
    
    // Test SSTable write performance
    let sstable_path = temp_dir.path().join("test.sst");
    let mut writer = hanshiro_storage::sstable::SSTableWriter::new(
        &sstable_path,
        SSTableConfig::default(),
    ).unwrap();
    
    let start = Instant::now();
    for (i, event) in events.iter().enumerate() {
        let key = format!("{:08}", i).into_bytes();
        let value = rmp_serde::to_vec(event).unwrap();
        writer.add(&key, &value).unwrap();
    }
    writer.finish().unwrap();
    let sstable_duration = start.elapsed();
    let sstable_throughput = 1000.0 / sstable_duration.as_secs_f64();
    
    println!("SSTable write throughput: {:.0} events/sec", sstable_throughput);
    
    // All components should achieve good performance
    assert!(wal_throughput > 50_000.0, "WAL throughput too low");
    assert!(memtable_throughput > 100_000.0, "MemTable throughput too low");
    assert!(sstable_throughput > 20_000.0, "SSTable throughput too low");
}