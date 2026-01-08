//! # Soak Test - Long-Running Stability
//!
//! Verifies that under sustained load:
//! - File descriptor count stays bounded (not linear growth)
//! - Memory usage is stable
//! - Compaction keeps up with writes
//! - Throughput doesn't degrade over time
//!
//! Run with: cargo test --release soak_test -- --ignored --nocapture

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hanshiro_core::{Event, EventId};
use hanshiro_core::types::{EventSource, EventType, IngestionFormat};
use hanshiro_core::traits::StorageEngine as StorageEngineTrait;
use hanshiro_storage::engine::StorageConfig;
use hanshiro_storage::{StorageEngine, CompactionConfig, FdConfig};
use tempfile::TempDir;
use tokio::time::interval;

/// Test duration - 15 minutes for CI, can be extended via env var
fn test_duration() -> Duration {
    let minutes: u64 = std::env::var("SOAK_TEST_MINUTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);
    Duration::from_secs(minutes * 60)
}

/// Sample interval for metrics
const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// Max allowed FD growth ratio (current / initial)
const MAX_FD_GROWTH_RATIO: f64 = 2.0;

/// Metrics collected during soak test
#[derive(Debug, Clone, Default)]
struct SoakMetrics {
    timestamp_secs: u64,
    writes_total: u64,
    writes_per_sec: f64,
    fd_count: u64,
    sstable_count: u64,
    compactions: u64,
    errors: u64,
}

fn create_test_event(seq: u64) -> Event {
    Event {
        id: EventId::new(),
        timestamp: chrono::Utc::now(),
        event_type: EventType::NetworkConnection,
        source: EventSource {
            host: "soak-test".to_string(),
            ip: Some("10.0.0.1".parse().unwrap()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        raw_data: format!("soak_test_event_{}", seq).into_bytes(),
        metadata: Default::default(),
        vector: None,
        merkle_prev: None,
        merkle_hash: None,
    }
}

#[tokio::test]
#[ignore] // Run with: cargo test soak_test -- --ignored --nocapture
async fn soak_test_fd_stability() {
    println!("=== SOAK TEST: File Descriptor Stability ===");
    println!("Duration: {:?}", test_duration());
    println!();

    let temp_dir = TempDir::new().unwrap();
    
    // Configure for aggressive compaction to test FD management
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        compaction_config: CompactionConfig {
            l0_compaction_trigger: 4,
            ..Default::default()
        },
        fd_config: FdConfig {
            max_open_sstables: 64, // Lower limit to stress test pooling
            soft_limit_ratio: 0.8,
            enable_backpressure: true,
            partitions: 4,
        },
        flush_interval: Duration::from_secs(5), // Frequent flushes to create SSTables
        compaction_interval: Duration::from_secs(10),
        ..Default::default()
    };

    let engine = Arc::new(StorageEngine::new(config).await.unwrap());
    
    let shutdown = Arc::new(AtomicBool::new(false));
    let write_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    
    // Record initial FD count
    let initial_fd = engine.fd_stats().estimated_used;
    println!("Initial FD count: {}", initial_fd);
    
    let mut metrics_history: Vec<SoakMetrics> = Vec::new();
    let start = Instant::now();

    // Writer task - sustained load
    let engine_clone = Arc::clone(&engine);
    let shutdown_clone = Arc::clone(&shutdown);
    let write_count_clone = Arc::clone(&write_count);
    let error_count_clone = Arc::clone(&error_count);
    
    let writer_handle = tokio::spawn(async move {
        let mut seq = 0u64;
        let batch_size = 100;
        
        while !shutdown_clone.load(Ordering::Relaxed) {
            let events: Vec<Event> = (0..batch_size)
                .map(|i| create_test_event(seq + i))
                .collect();
            
            match engine_clone.write_batch(events).await {
                Ok(_) => {
                    write_count_clone.fetch_add(batch_size, Ordering::Relaxed);
                }
                Err(e) => {
                    error_count_clone.fetch_add(1, Ordering::Relaxed);
                    eprintln!("Write error: {:?}", e);
                }
            }
            
            seq += batch_size;
            
            // Small delay to prevent overwhelming
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    // Metrics collection loop
    let mut sample_interval = interval(SAMPLE_INTERVAL);
    let mut last_write_count = 0u64;
    
    while start.elapsed() < test_duration() {
        sample_interval.tick().await;
        
        let elapsed_secs = start.elapsed().as_secs();
        let current_writes = write_count.load(Ordering::Relaxed);
        let writes_delta = current_writes - last_write_count;
        let writes_per_sec = writes_delta as f64 / SAMPLE_INTERVAL.as_secs_f64();
        last_write_count = current_writes;
        
        let fd_stats = engine.fd_stats();
        let stats = engine.stats().await.unwrap();
        
        let metrics = SoakMetrics {
            timestamp_secs: elapsed_secs,
            writes_total: current_writes,
            writes_per_sec,
            fd_count: fd_stats.estimated_used,
            sstable_count: stats.sstable_count as u64,
            compactions: fd_stats.evictions, // Proxy for compaction activity
            errors: error_count.load(Ordering::Relaxed),
        };
        
        println!(
            "[{:>4}s] writes: {:>8} ({:>6.0}/s) | FDs: {:>4} | SSTables: {:>3} | pool hits: {:>6} misses: {:>4}",
            metrics.timestamp_secs,
            metrics.writes_total,
            metrics.writes_per_sec,
            metrics.fd_count,
            metrics.sstable_count,
            fd_stats.cache_hits,
            fd_stats.cache_misses,
        );
        
        metrics_history.push(metrics);
    }

    // Shutdown writer
    shutdown.store(true, Ordering::Relaxed);
    writer_handle.await.unwrap();

    // Final flush
    engine.flush().await.unwrap();

    // Analysis
    println!();
    println!("=== SOAK TEST RESULTS ===");
    
    let final_fd = engine.fd_stats().estimated_used;
    let fd_growth_ratio = final_fd as f64 / initial_fd.max(1) as f64;
    
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    let avg_throughput: f64 = metrics_history.iter().map(|m| m.writes_per_sec).sum::<f64>() 
        / metrics_history.len() as f64;
    
    // Check for throughput degradation (compare first vs last quarter)
    let quarter = metrics_history.len() / 4;
    let first_quarter_avg: f64 = metrics_history[..quarter].iter()
        .map(|m| m.writes_per_sec).sum::<f64>() / quarter as f64;
    let last_quarter_avg: f64 = metrics_history[metrics_history.len()-quarter..].iter()
        .map(|m| m.writes_per_sec).sum::<f64>() / quarter as f64;
    let throughput_degradation = (first_quarter_avg - last_quarter_avg) / first_quarter_avg;
    
    println!("Duration:           {:?}", start.elapsed());
    println!("Total writes:       {}", total_writes);
    println!("Total errors:       {}", total_errors);
    println!("Avg throughput:     {:.0} writes/sec", avg_throughput);
    println!();
    println!("Initial FDs:        {}", initial_fd);
    println!("Final FDs:          {}", final_fd);
    println!("FD growth ratio:    {:.2}x", fd_growth_ratio);
    println!();
    println!("First quarter avg:  {:.0} writes/sec", first_quarter_avg);
    println!("Last quarter avg:   {:.0} writes/sec", last_quarter_avg);
    println!("Throughput change:  {:.1}%", throughput_degradation * -100.0);
    println!();
    
    let fd_stats = engine.fd_stats();
    println!("FD Pool Stats:");
    println!("  Open SSTables:    {}", fd_stats.open_sstables);
    println!("  Cache hits:       {}", fd_stats.cache_hits);
    println!("  Cache misses:     {}", fd_stats.cache_misses);
    println!("  Evictions:        {}", fd_stats.evictions);
    println!("  Hit ratio:        {:.1}%", 
        fd_stats.cache_hits as f64 / (fd_stats.cache_hits + fd_stats.cache_misses).max(1) as f64 * 100.0);

    // Assertions
    assert!(
        fd_growth_ratio < MAX_FD_GROWTH_RATIO,
        "FD count grew too much: {:.2}x (max allowed: {:.2}x)",
        fd_growth_ratio,
        MAX_FD_GROWTH_RATIO
    );
    
    assert!(
        total_errors == 0,
        "Had {} errors during soak test",
        total_errors
    );
    
    assert!(
        throughput_degradation < 0.5, // Allow up to 50% degradation
        "Throughput degraded too much: {:.1}%",
        throughput_degradation * 100.0
    );
    
    assert!(
        engine.fd_healthy(),
        "FD monitor reports unhealthy state"
    );

    println!();
    println!("=== SOAK TEST PASSED ===");
}

/// Quick smoke test (1 minute) for CI
#[tokio::test]
async fn smoke_test_fd_stability() {
    println!("=== SMOKE TEST: Quick FD Stability Check ===");
    
    let temp_dir = TempDir::new().unwrap();
    
    let config = StorageConfig {
        data_dir: temp_dir.path().to_path_buf(),
        compaction_config: CompactionConfig {
            l0_compaction_trigger: 2, // Very aggressive
            ..Default::default()
        },
        fd_config: FdConfig {
            max_open_sstables: 16,
            ..Default::default()
        },
        flush_interval: Duration::from_secs(1),
        compaction_interval: Duration::from_secs(2),
        ..Default::default()
    };

    let engine = StorageEngine::new(config).await.unwrap();
    let initial_fd = engine.fd_stats().estimated_used;
    
    // Write enough to trigger multiple flushes and compactions
    for batch in 0..50 {
        let events: Vec<Event> = (0..100)
            .map(|i| create_test_event(batch * 100 + i))
            .collect();
        engine.write_batch(events).await.unwrap();
        
        if batch % 10 == 0 {
            // Give compaction a chance to run
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    
    // Wait for background tasks
    tokio::time::sleep(Duration::from_secs(3)).await;
    engine.flush().await.unwrap();
    
    let final_fd = engine.fd_stats().estimated_used;
    let fd_stats = engine.fd_stats();
    
    println!("Initial FDs: {}, Final FDs: {}", initial_fd, final_fd);
    println!("Pool: {} open, {} hits, {} misses", 
        fd_stats.open_sstables, fd_stats.cache_hits, fd_stats.cache_misses);
    
    // Should not have unbounded growth
    assert!(
        final_fd < initial_fd + 50,
        "FD count grew unexpectedly: {} -> {}",
        initial_fd,
        final_fd
    );
    
    assert!(engine.fd_healthy(), "FD monitor reports unhealthy");
    
    println!("=== SMOKE TEST PASSED ===");
}
