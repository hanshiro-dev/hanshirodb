# HanshiroDB Storage Engine

A high-performance LSM-tree storage engine with tamper-proof Merkle chaining, optimized for security event ingestion at 1M+ events/sec.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Write Path                               │
│                                                                 │
│  Events ──► WAL (Merkle Chain) ──► MemTable ──► SSTable         │
│              │                       │            │             │
│              ▼                       ▼            ▼             │
│         Durability              In-Memory     On-Disk           │
│         + Integrity             Sorted        Immutable         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        Read Path                                │
│                                                                 │
│  Query ──► MemTable ──► L0 SSTables ──► L1-L6 SSTables          │
│              │              │               │                   │
│              ▼              ▼               ▼                   │
│           Hot Data     Bloom Filter    Binary Search            │
└─────────────────────────────────────────────────────────────────┘
```

## Performance Characteristics

__On Apple M3 Pro 36GB Memory__

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Batched writes (20 writers) | 1,004,326 events/sec | 1.00 µs |
| Single writes | 250,000 events/sec | 4.00 µs |
| WAL only | 425,000 events/sec | 2.35 µs |
| MemTable | 3,300,000 events/sec | 0.30 µs |
| Scans | 1,490,000 events/sec | - |

---

## Module Reference

### Core Engine

#### `engine.rs`
The main storage engine orchestrating all components.

**Key structures:**
- `StorageEngine` - Main entry point, coordinates WAL → MemTable → SSTable flow
- `StorageConfig` - Configuration for all subsystems

**Design decisions:**
- Pre-creates L0-L6 directories at startup (eliminates mkdir latency during flushes)
- Initializes `cached_time` early for syscall-free timestamps
- Background tasks for flush and compaction run on separate tokio tasks

---

### Write-Ahead Log (WAL)

#### `wal/mod.rs`
Tamper-proof write-ahead log with Merkle chain integrity.

**Key features:**
- Group commit batches multiple writes for efficiency
- Merkle chain links every entry cryptographically
- Parallel data hashing with sequential chain hashing

**Design decisions:**
- 128-byte overhead per entry (32B header + 96B Merkle hashes)
- BLAKE3 for speed (~3 GB/s on modern CPUs)
- `append_batch()` bypasses group commit for bulk ingestion

#### `wal/file.rs`
Low-level WAL file operations.

**File format:**
```
┌──────────────────────────────────────┐
│ Header (64 bytes)                    │
│   Magic: "HANSHIRO"                  │
│   Version, timestamps, sequence range│
├──────────────────────────────────────┤
│ Entry 1                              │
│   Header (32B) + Merkle (96B) + Data │
├──────────────────────────────────────┤
│ Entry 2...N (chained via hashes)     │
└──────────────────────────────────────┘
```

#### `wal/types.rs`
WAL entry types and configuration.

- `WalEntry` - Sequence, timestamp, data, Merkle node
- `EntryType` - Data, Checkpoint, Truncate
- `WalConfig` - File size limits, sync mode, batch settings

#### `wal/iterator.rs`
Sequential iteration over WAL entries for recovery and verification.

---

### MemTable

#### `memtable/table.rs`
In-memory sorted structure using lock-free skip list.

**Design decisions:**
- `crossbeam-skiplist` for lock-free concurrent access
- Keys are `(event_id, timestamp)` for natural ordering
- 4MB default size before flush to SSTable

#### `memtable/manager.rs`
Manages active and immutable MemTables.

**Double-buffering strategy:**
- `active` - Current writable MemTable
- `immutable` - Being flushed to SSTable, still readable
- Seamless rotation without blocking writes

#### `memtable/types.rs`
MemTable configuration (size limits, thresholds).

---

### SSTable

#### `sstable/mod.rs`
Module exports and SSTable configuration.

**SSTable properties:**
- Immutable once written
- Sorted by key for binary search
- Block-based with compression

#### `sstable/builder.rs`
Constructs SSTables from sorted key-value pairs.

**Block format:**
- 4KB blocks (configurable)
- Prefix compression within blocks
- Block index for O(log n) lookup

#### `sstable/writer.rs`
Streaming SSTable writer for flush and compaction.

**Design decisions:**
- Buffered writes to minimize syscalls
- Bloom filter built incrementally
- Footer written last with checksums

#### `sstable/reader.rs`
Memory-mapped SSTable reader.

**Design decisions:**
- `mmap` for zero-copy reads
- Bloom filter and index kept in memory
- Block cache for hot data

#### `sstable/bloom.rs`
Bloom filter for fast negative lookups.

**Parameters:**
- 10 bits per key (~1% false positive rate)
- Multiple hash functions via double hashing

#### `sstable/iterator.rs`
Block-by-block iteration for range scans and compaction.

#### `sstable/compression.rs`
Block compression (Zstd, Snappy, None).

**Trade-offs:**
- Zstd: Best ratio, slower
- Snappy: Fast, moderate ratio
- None: Fastest, largest files

#### `sstable/types.rs`
SSTable metadata structures.

---

### Compaction

#### `compaction.rs`
Background compaction merges SSTables to bound space amplification.

**Strategy: Size-Tiered with Aggressive L0**
```
L0: Flush target, compact at 4+ files (overlapping keys)
L1: 10MB total
L2: 100MB total
L3: 1GB total
...
L6: 1TB total (10x multiplier per level)
```

**Algorithm:**
1. Pick compaction (L0 priority, then largest level)
2. Select overlapping SSTables from next level
3. K-way merge using min-heap
4. Write new SSTable(s) to target level
5. Atomically update manifest

**Design decisions:**
- Streaming merge (constant memory regardless of input size)
- L0 compaction triggers at 4 files to bound read amplification
- Single-threaded compaction (avoids write stalls)

---

### File Descriptor Management

#### `fd.rs`
Prevents FD exhaustion under sustained load.

**Partitioned LRU Pool:**
```
┌─────────────────────────────────────┐
│ SSTablePool (8 partitions default)  │
├─────────────────────────────────────┤
│ Partition 0: LRU<Path, Reader>      │
│ Partition 1: LRU<Path, Reader>      │
│ ...                                 │
│ Partition 7: LRU<Path, Reader>      │
└─────────────────────────────────────┘
```

**Design decisions:**
- 2 partitions per CPU core (reduces lock contention)
- Path hashing distributes load evenly
- Soft limit at 80% of system FD limit
- Backpressure when approaching limits

**FdMonitor:**
- Tracks system-wide FD usage
- `/proc/self/fd` on Linux, `/dev/fd` on macOS

---

### Time-Based Partitioning

#### `partitioning.rs`
Organizes SSTables by time windows for efficient retention.

**Partition scheme:**
```
sstables/
├── 2024-01-15/
│   ├── 00_*.sst  (00:00-06:00)
│   ├── 06_*.sst  (06:00-12:00)
│   ├── 12_*.sst  (12:00-18:00)
│   └── 18_*.sst  (18:00-24:00)
├── 2024-01-14/
│   └── ...
```

**Benefits:**
- Time-range queries scan only relevant partitions
- Retention = delete entire directories
- Pre-creation eliminates mkdir latency

**Window options:**
- `Hourly` - High-volume workloads
- `SixHour` - Default, good balance
- `Daily` - Low-volume workloads

---

### Manifest

#### `manifest.rs`
Tracks database state for crash recovery.

**Contents:**
- List of live SSTables (path, size, key range, level)
- WAL checkpoint (last flushed sequence)
- Compaction state

**Atomicity:**
- Write to temp file, then atomic rename
- CRC32 checksum for corruption detection

---

### Utilities

#### `cached_time.rs`
Eliminates timestamp syscalls (77x faster).

**How it works:**
- Background thread updates atomic timestamp every 100ms
- `now_ms()` returns cached value (0.3ns vs 22ns syscall)
- ±100ms accuracy (sufficient for TTL and ordering)

#### `cache.rs`
Block cache for hot SSTable data (placeholder for future LRU cache).

---

## Key Algorithms

### Merkle Chain (Tamper-Proofing)

```
Entry 1: hash1 = H(data1)
Entry 2: hash2 = H(hash1 || data2)
Entry 3: hash3 = H(hash2 || data3)
```

Modifying any entry invalidates all subsequent hashes.

### LSM-Tree Write Path
1. Append to WAL (durability)
2. Insert into MemTable (sorted, in-memory)
3. When MemTable full → flush to L0 SSTable
4. Background compaction merges levels

### LSM-Tree Read Path
1. Check MemTable (hot data)
2. Check L0 SSTables (newest first, all checked)
3. Check L1+ SSTables (binary search, one per level)
4. Bloom filters skip files that definitely don't contain key

### K-Way Merge (Compaction)

```rust
// Min-heap of (key, value, source_index)
while !heap.is_empty() {
    let (key, value, src) = heap.pop();
    output.write(key, value);
    if let Some(next) = sources[src].next() {
        heap.push(next);
    }
}
```

---

## Configuration Tuning

| Parameter | Default | Tune For |
|-----------|---------|----------|
| `memtable_size` | 4MB | Larger = fewer flushes, more memory |
| `l0_compaction_trigger` | 4 | Lower = less read amplification |
| `max_open_sstables` | 256 | Higher if many SSTables |
| `sync_on_write` | false | true for durability (slower) |
| `partition_window` | 6 hours | Smaller for faster retention |

---

## File Layout

```
data/
├── wal/
│   ├── 000001.wal
│   ├── 000002.wal
│   └── ...
├── sstables/
│   ├── L0/
│   │   └── 000001.sst
│   ├── L1/
│   ├── L2/
│   └── ...
└── MANIFEST
```
