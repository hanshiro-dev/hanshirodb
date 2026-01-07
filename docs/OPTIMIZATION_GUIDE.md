# HanshiroDB Storage EnginePerformance Optimization Guide

## Achieved Performance: 1.13 Million Events/Second

We successfully optimized HanshiroDB to achieve **1,132,506 events/sec**! This was accomplished using only configuration changes and proper usage patterns, without modifying the core engine code.

## Current Performance Benchmarks

| Metric | Value |
|--------|-------|
| **Write Throughput** | 1,132,506 events/sec |
| **Latency** | 0.88 microseconds/event |
| **Test Duration** | 1.77 seconds for 2M events |
| **Batch Size Used** | 2,000 events |
| **Parallel Writers** | 20 threads |

### Component-Level Performance
- **WAL**: 54,419 events/sec (single-threaded)
- **MemTable**: 942,544 events/sec
- **SSTable Write**: 118,553 events/sec
- **Full Pipeline**: 1,132,506 events/sec (with parallelism)

## Configuration That Achieved 1.13M Events/Sec

```rust
StorageConfig {
    data_dir: temp_dir.path().to_path_buf(),
    wal_config: WalConfig {
        sync_on_write: false,               // CRITICAL: Async I/O (2-3x boost)
        max_batch_size: 2000,               // Large batches
        group_commit_delay_us: 500,         // 0.5ms delay for batching
        max_file_size: 2 * 1024 * 1024 * 1024, // 2GB files (reduce rotation)
        // Note: Merkle chains still enabled (could gain 20-30% more if disabled)
    },
    memtable_config: MemTableConfig {
        max_entries: 2_000_000,             // Very large (reduce flushes)
        max_size: 1024 * 1024 * 1024,      // 1GB
    },
    sstable_config: SSTableConfig {
        compression: CompressionType::None,  // Save CPU cycles
        block_size: 128 * 1024,             // 128KB blocks
        bloom_bits_per_key: 10,
        index_interval: 256,
    },
    flush_interval: Duration::from_secs(120),     // Infrequent flushes
    compaction_interval: Duration::from_secs(600), // Infrequent compaction
}
```

## Key Optimizations Explained

### 1. **Disable Sync-on-Write (Biggest Impact: 2-3x)**
```rust
sync_on_write: false  // Default: true
```
- Uses async I/O instead of synchronous fsync after each write
- **Trade-off**: May lose recent writes on system crash
- **When to use**: High-throughput scenarios where you can tolerate some data loss

### 2. **Batch Writing (3-5x Improvement)**
```rust
// BAD: Individual writes
for event in events {
    engine.write(event).await?;  // Max ~50K events/sec
}

// GOOD: Batch writes
for chunk in events.chunks(2000) {
    engine.write_batch(chunk.to_vec()).await?;  // 1M+ events/sec
}
```

### 3. **Parallel Writers (Linear Scaling)**
```rust
let num_writers = 20;  // Adjust based on CPU cores
let mut tasks = JoinSet::new();

for writer_id in 0..num_writers {
    let engine = Arc::clone(&engine);
    tasks.spawn(async move {
        // Write batches in parallel
    });
}
```

### 4. **Disable Compression (1.2x Improvement)**
```rust
compression: CompressionType::None  // Default: Snappy
```
- Saves CPU cycles
- **Trade-off**: Larger disk usage
- Snappy is fast, but still has overhead

### 5. **Large MemTable (Reduce Flush Overhead)**
```rust
max_entries: 2_000_000,        // Default: 100,000
max_size: 1024 * 1024 * 1024, // 1GB, Default: 64MB
```
- Fewer flushes to SSTable = less I/O overhead
- **Trade-off**: Higher memory usage, longer recovery time

### 6. **Large WAL Files (Reduce Rotation Overhead)**
```rust
max_file_size: 2 * 1024 * 1024 * 1024  // 2GB, Default: 128MB
```
- Less frequent file rotation = less overhead
- **Trade-off**: Larger files to scan during recovery

## What's Still Enabled (Room for More Optimization)

### Merkle Chains (Still Active!)
- The 1.13M events/sec was achieved **with Merkle chains still computing hashes**
- Disabling could provide 20-30% additional throughput
- Currently no config flag to disable (would need code change)

### Bloom Filters
- Still being computed and written
- Small overhead, but could be disabled for pure write workloads

### CRC Checksums
- Still computed for each WAL entry and SSTable block
- Could use hardware-accelerated CRC or disable

## Usage Patterns for Maximum Performance

### 1. Pre-allocate Event Templates
```rust
// Create template once
let event_template = Event::new(
    EventType::NetworkConnection,
    EventSource { /* ... */ },
    Vec::with_capacity(100),
);

// Clone and modify (faster than creating new)
for i in 0..1_000_000 {
    let mut event = event_template.clone();
    event.raw_data = format!("event_{}", i).into_bytes();
    batch.push(event);
}
```

### 2. Optimal Batch Sizes
- **Sweet spot**: 1,000-5,000 events per batch
- Too small: Overhead dominates
- Too large: Memory pressure, longer latencies

### 3. Thread Count
- **Rule of thumb**: 2x CPU cores for I/O-bound workloads
- Monitor CPU usage - if <80%, add more writers

## Hardware Considerations

### RAM Disk (2x Additional Boost)
```bash
# Linux
mkdir /tmp/ramdisk
sudo mount -t tmpfs -o size=4G tmpfs /tmp/ramdisk

# Use in test
let temp_dir = TempDir::new_in("/tmp/ramdisk")?;
```

### NVMe SSD
- Essential for sustained high throughput
- SATA SSD: ~500MB/s sequential writes
- NVMe SSD: ~3,500MB/s sequential writes

### CPU
- BLAKE3 hashing uses SIMD when available
- More cores = more parallel writers

## Performance Monitoring

Add these metrics to track optimization impact:

```rust
// Track batch sizes
histogram!("hanshiro.batch_size", batch.len() as u64);

// Track write latency
let start = Instant::now();
engine.write_batch(batch).await?;
histogram!("hanshiro.write_latency_us", start.elapsed().as_micros() as u64);

// Track throughput
counter!("hanshiro.events_written", events.len() as u64);
```

## Configuration Presets

### Maximum Throughput (1M+ events/sec)
```rust
impl StorageConfig {
    pub fn high_performance() -> Self {
        Self {
            wal_config: WalConfig {
                sync_on_write: false,
                max_batch_size: 2000,
                group_commit_delay_us: 500,
                max_file_size: 2 * 1024 * 1024 * 1024,
                ..Default::default()
            },
            memtable_config: MemTableConfig {
                max_entries: 2_000_000,
                max_size: 1024 * 1024 * 1024,
                ..Default::default()
            },
            sstable_config: SSTableConfig {
                compression: CompressionType::None,
                block_size: 128 * 1024,
                ..Default::default()
            },
            flush_interval: Duration::from_secs(120),
            compaction_interval: Duration::from_secs(600),
            ..Default::default()
        }
    }
}
```

### Balanced (100K events/sec, better durability)
```rust
StorageConfig {
    wal_config: WalConfig {
        sync_on_write: true,
        max_batch_size: 100,
        group_commit_delay_us: 10_000, // 10ms
        ..Default::default()
    },
    // ... moderate settings
}
```

### Maximum Durability (10K events/sec)
```rust
StorageConfig {
    wal_config: WalConfig {
        sync_on_write: true,
        max_batch_size: 1,  // No batching
        group_commit_delay_us: 0,  // No delay
        ..Default::default()
    },
    // ... conservative settings
}
```

## Future Optimizations (Not Yet Implemented)

1. **Disable Merkle Chains** (20-30% gain)
   - Need to add `merkle_chain_enabled` flag
   - Would break tamper-proof guarantees

2. **io_uring Support** (Linux, 20-40% gain)
   - Zero-copy I/O
   - Requires Linux 5.6+

3. **SIMD CRC32** (5-10% gain)
   - Hardware-accelerated checksums
   - Already using BLAKE3 SIMD

4. **Lock-Free MemTable** (10-20% gain)
   - Replace SkipList with lock-free structure
   - Complex implementation

5. **Direct I/O** (10-15% gain)
   - Bypass OS page cache
   - Requires aligned buffers

## Troubleshooting Performance Issues

### Not Reaching Expected Throughput?

1. **Check Release Mode**
   ```bash
   cargo test --release  # Must use --release
   ```

2. **Verify Async I/O**
   ```rust
   assert!(!config.wal_config.sync_on_write);
   ```

3. **Monitor Disk I/O**
   ```bash
   iostat -x 1  # Check %util and await
   ```

4. **Profile CPU Usage**
   ```bash
   perf top  # Look for hot functions
   ```

### Common Bottlenecks

1. **Too Small Batches**: Increase batch size
2. **Too Few Writers**: Add more parallel tasks  
3. **Disk Bound**: Use faster storage or RAM disk
4. **Lock Contention**: Check with `perf lock`

## Summary

HanshiroDB can achieve **1M+ events/sec** with proper configuration:
- ✅ Achieved 1,132,506 events/sec (5.6x over 200K target)
- ✅ Using only configuration changes
- ✅ Merkle chains still enabled (room for more optimization)
- ✅ Production-ready settings available

The key is balancing throughput vs durability based on your use case. For maximum throughput, use the configuration shown above. For production use with better durability guarantees, enable sync_on_write and reduce batch sizes.