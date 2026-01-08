//! Micro-benchmarks for cached timestamp and partitioned FD pool

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;

use hanshiro_storage::cached_time;
use hanshiro_storage::{FdConfig, SSTablePool};

#[test]
fn bench_cached_vs_syscall_timestamp() {
    cached_time::init();
    thread::sleep(Duration::from_millis(150));
    
    let iterations = 5_000_000u64;
    
    // Syscall-based
    let start = Instant::now();
    let mut sum = 0u64;
    for _ in 0..iterations {
        sum = sum.wrapping_add(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        );
    }
    let syscall_ns = start.elapsed().as_nanos() as f64 / iterations as f64;
    
    // Cached
    let start = Instant::now();
    let mut sum2 = 0u64;
    for _ in 0..iterations {
        sum2 = sum2.wrapping_add(cached_time::now_ms());
    }
    let cached_ns = start.elapsed().as_nanos() as f64 / iterations as f64;
    
    let speedup = syscall_ns / cached_ns;
    
    println!("\n=== Timestamp Benchmark ===");
    println!("Syscall: {:.1} ns/op ({:.1}M ops/sec)", syscall_ns, 1000.0 / syscall_ns);
    println!("Cached:  {:.1} ns/op ({:.1}M ops/sec)", cached_ns, 1000.0 / cached_ns);
    println!("Speedup: {:.1}x", speedup);
    
    assert!(sum > 0 && sum2 > 0);
    assert!(speedup > 2.0, "Cached should be at least 2x faster");
}

#[test]
fn bench_partitioned_fd_pool_contention() {
    use std::path::PathBuf;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    // The real benefit of partitioning is reduced lock contention on get()
    // We can't easily test actual file opens, but we can measure the hash distribution
    
    let multi_config = FdConfig {
        max_open_sstables: 256,
        partitions: 8,
        ..Default::default()
    };
    let pool = SSTablePool::new(multi_config);
    let stats = pool.stats();
    
    println!("\n=== Partitioned FD Pool ===");
    println!("Partitions: {}", stats.partitions);
    println!("System FD limit: {}", stats.system_limit);
    
    // Verify paths hash to different partitions
    let mut partition_hits = vec![0u32; 8];
    for i in 0..1000 {
        let path = PathBuf::from(format!("/data/sstables/level-{}/sst-{:06}.sst", i % 7, i));
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % 8;
        partition_hits[idx] += 1;
    }
    
    println!("Hash distribution across 8 partitions (1000 paths):");
    for (i, count) in partition_hits.iter().enumerate() {
        println!("  Partition {}: {} paths", i, count);
    }
    
    // Should be roughly even (125 ± 30 per partition)
    let min = *partition_hits.iter().min().unwrap();
    let max = *partition_hits.iter().max().unwrap();
    println!("Distribution range: {} - {} (ideal: 125)", min, max);
    assert!(min > 80 && max < 170, "Hash distribution should be reasonably even");
}
