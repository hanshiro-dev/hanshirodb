//! # Comprehensive SSTable Tests
//!
//! This test suite verifies:
//! - Basic SSTable write/read operations
//! - Compression algorithms
//! - Bloom filter functionality
//! - Block and index structures
//! - Iterator behavior
//! - Large file handling
//! - Concurrent access patterns
//! - Integration with other components

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use bytes::Bytes;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use hanshiro_storage::sstable::{
    SSTableWriter, SSTableReader, SSTableConfig, SSTableInfo, CompressionType,
};

/// Generate test key with specified pattern
fn test_key(prefix: &str, num: u64) -> Vec<u8> {
    format!("{}_key_{:08}", prefix, num).into_bytes()
}

/// Generate test value of specified size
fn test_value(size: usize, seed: u8) -> Vec<u8> {
    vec![seed; size]
}

#[test]
fn test_sstable_basic_write_read() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test.sst");
    
    // Write SSTable
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Write some entries
    let entries = vec![
        (b"key001".to_vec(), b"value1".to_vec()),
        (b"key002".to_vec(), b"value2".to_vec()),
        (b"key003".to_vec(), b"value3".to_vec()),
        (b"key004".to_vec(), b"value4".to_vec()),
        (b"key005".to_vec(), b"value5".to_vec()),
    ];
    
    for (key, value) in &entries {
        writer.add(key, value).unwrap();
    }
    
    let info = writer.finish().unwrap();
    assert_eq!(info.entry_count, 5);
    assert!(info.file_size > 0);
    
    // Read SSTable
    let reader = SSTableReader::open(&sstable_path).unwrap();
    
    // Verify all entries
    for (key, expected_value) in &entries {
        let value = reader.get(key).unwrap();
        assert_eq!(value.as_ref().map(|v| v.as_ref()), Some(expected_value.as_slice()));
    }
    
    // Verify non-existent key
    assert!(reader.get(b"nonexistent").unwrap().is_none());
}

#[test]
fn test_sstable_compression_types() {
    let temp_dir = TempDir::new().unwrap();
    
    let compression_types = vec![
        CompressionType::None,
        CompressionType::Zstd,
        CompressionType::Snappy,
    ];
    
    for compression in compression_types {
        let sstable_path = temp_dir.path().join(format!("test_{:?}.sst", compression));
        
        let mut config = SSTableConfig::default();
        config.compression = compression;
        
        let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
        
        // Write entries with various sizes
        for i in 0..100 {
            let key = test_key("comp", i);
            let value = test_value(1000, i as u8); // 1KB values
            writer.add(&key, &value).unwrap();
        }
        
        let info = writer.finish().unwrap();
        println!("Compression {:?}: {} bytes for 100KB data", compression, info.file_size);
        
        // Verify data integrity
        let reader = SSTableReader::open(&sstable_path).unwrap();
        for i in 0..100 {
            let key = test_key("comp", i);
            let value = reader.get(&key).unwrap().unwrap();
            assert_eq!(value.len(), 1000);
            assert!(value.iter().all(|&b| b == i as u8));
        }
    }
}

#[test]
fn test_sstable_iterator() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_iter.sst");
    
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Write entries in sorted order
    let mut expected = Vec::new();
    for i in 0..50 {
        let key = test_key("iter", i);
        let value = test_value(100, i as u8);
        writer.add(&key, &value).unwrap();
        expected.push((Bytes::from(key), Bytes::from(value)));
    }
    
    writer.finish().unwrap();
    
    // Read with iterator
    let reader = SSTableReader::open(&sstable_path).unwrap();
    let mut iter = reader.iter();
    
    let mut count = 0;
    while let Some(result) = iter.next() {
        let (key, value) = result.unwrap();
        assert_eq!(key, expected[count].0);
        assert_eq!(value, expected[count].1);
        count += 1;
    }
    
    assert_eq!(count, 50);
}

#[test]
fn test_sstable_large_values() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_large.sst");
    
    let mut config = SSTableConfig::default();
    config.block_size = 16 * 1024; // 16KB blocks
    
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Write entries with large values
    for i in 0..10 {
        let key = test_key("large", i);
        let value = test_value(10 * 1024, i as u8); // 10KB values
        writer.add(&key, &value).unwrap();
    }
    
    writer.finish().unwrap();
    
    // Verify large values
    let reader = SSTableReader::open(&sstable_path).unwrap();
    for i in 0..10 {
        let key = test_key("large", i);
        let value = reader.get(&key).unwrap().unwrap();
        assert_eq!(value.len(), 10 * 1024);
        assert!(value.iter().all(|&b| b == i as u8));
    }
}

#[test]
fn test_sstable_block_boundaries() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_blocks.sst");
    
    let mut config = SSTableConfig::default();
    config.block_size = 1024; // Small 1KB blocks
    config.index_interval = 4; // Index every 4th key
    
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Write entries that will span multiple blocks
    for i in 0..100 {
        let key = test_key("block", i);
        let value = test_value(100, i as u8);
        writer.add(&key, &value).unwrap();
    }
    
    writer.finish().unwrap();
    
    // Verify random access across blocks
    let reader = SSTableReader::open(&sstable_path).unwrap();
    let mut rng = StdRng::seed_from_u64(42);
    
    for _ in 0..50 {
        let i = rng.gen_range(0..100);
        let key = test_key("block", i);
        let value = reader.get(&key).unwrap().unwrap();
        assert_eq!(value.len(), 100);
        assert!(value.iter().all(|&b| b == i as u8));
    }
}

#[test]
fn test_sstable_bloom_filter_effectiveness() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_bloom.sst");
    
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Write entries
    let num_entries = 1000;
    for i in 0..num_entries {
        let key = test_key("bloom", i);
        let value = test_value(100, i as u8);
        writer.add(&key, &value).unwrap();
    }
    
    writer.finish().unwrap();
    
    // Test bloom filter with non-existent keys
    let reader = SSTableReader::open(&sstable_path).unwrap();
    let mut false_positives = 0;
    let test_count = 10000;
    
    for i in num_entries..(num_entries + test_count) {
        let key = test_key("bloom", i);
        if reader.get(&key).unwrap().is_some() {
            false_positives += 1;
        }
    }
    
    let false_positive_rate = false_positives as f64 / test_count as f64;
    println!("Bloom filter false positive rate: {:.2}%", false_positive_rate * 100.0);
    
    // Should be close to the target rate (1%)
    assert!(false_positive_rate < 0.02); // Allow up to 2%
}

#[test]
fn test_sstable_min_max_keys() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_minmax.sst");
    
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Write entries
    let keys: Vec<Vec<u8>> = vec![
        b"aaa".to_vec(),
        b"bbb".to_vec(),
        b"ccc".to_vec(),
        b"ddd".to_vec(),
        b"eee".to_vec(),
    ];
    
    for key in &keys {
        writer.add(key, b"value").unwrap();
    }
    
    let info = writer.finish().unwrap();
    assert_eq!(info.min_key, b"aaa");
    assert_eq!(info.max_key, b"eee");
}

#[test]
fn test_sstable_empty_values() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_empty.sst");
    
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Write entries with empty values
    writer.add(b"key1", b"").unwrap();
    writer.add(b"key2", b"value").unwrap();
    writer.add(b"key3", b"").unwrap();
    
    writer.finish().unwrap();
    
    // Verify empty values are preserved
    let reader = SSTableReader::open(&sstable_path).unwrap();
    assert_eq!(reader.get(b"key1").unwrap().unwrap().len(), 0);
    assert_eq!(reader.get(b"key2").unwrap().unwrap().as_ref(), b"value");
    assert_eq!(reader.get(b"key3").unwrap().unwrap().len(), 0);
}

#[test]
fn test_sstable_performance() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_perf.sst");
    
    let mut config = SSTableConfig::default();
    config.compression = CompressionType::Snappy;
    
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    let num_entries = 100_000;
    let value_size = 100;
    
    // Measure write performance
    let start = Instant::now();
    for i in 0..num_entries {
        let key = test_key("perf", i);
        let value = test_value(value_size, (i % 256) as u8);
        writer.add(&key, &value).unwrap();
    }
    let info = writer.finish().unwrap();
    let write_duration = start.elapsed();
    
    let write_throughput = (num_entries as f64 * (value_size + 20) as f64) / write_duration.as_secs_f64() / 1_048_576.0;
    println!("SSTable write throughput: {:.2} MB/s", write_throughput);
    println!("SSTable size: {} MB", info.file_size / 1_048_576);
    
    // Measure read performance
    let reader = SSTableReader::open(&sstable_path).unwrap();
    let mut rng = StdRng::seed_from_u64(42);
    
    let start = Instant::now();
    let num_reads = 10_000;
    for _ in 0..num_reads {
        let i = rng.gen_range(0..num_entries);
        let key = test_key("perf", i);
        let _ = reader.get(&key).unwrap();
    }
    let read_duration = start.elapsed();
    
    let reads_per_sec = num_reads as f64 / read_duration.as_secs_f64();
    println!("SSTable random read performance: {:.0} reads/sec", reads_per_sec);
    
    // Verify performance meets expectations
    assert!(write_throughput > 40.0, "Write throughput too low");
    assert!(reads_per_sec > 20_000.0, "Read performance too low");
}

#[test]
fn test_sstable_concurrent_reads() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_concurrent.sst");
    
    // First, create an SSTable
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    for i in 0..1000 {
        let key = test_key("concurrent", i);
        let value = test_value(100, i as u8);
        writer.add(&key, &value).unwrap();
    }
    
    writer.finish().unwrap();
    
    // Now test concurrent reads
    let reader = Arc::new(SSTableReader::open(&sstable_path).unwrap());
    let num_threads = 8;
    let reads_per_thread = 1000;
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let reader_clone = Arc::clone(&reader);
        
        let handle = std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id);
            
            for _ in 0..reads_per_thread {
                let i = rng.gen_range(0..1000);
                let key = test_key("concurrent", i);
                let value = reader_clone.get(&key).unwrap().unwrap();
                assert_eq!(value.len(), 100);
                assert!(value.iter().all(|&b| b == i as u8));
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_sstable_sorted_order() {
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_sorted.sst");
    
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    // Keys must be added in sorted order
    let keys = vec![
        b"apple".to_vec(),
        b"banana".to_vec(),
        b"cherry".to_vec(),
        b"date".to_vec(),
        b"elderberry".to_vec(),
    ];
    
    for (i, key) in keys.iter().enumerate() {
        writer.add(key, &[i as u8]).unwrap();
    }
    
    writer.finish().unwrap();
    
    // Verify with iterator that order is preserved
    let reader = SSTableReader::open(&sstable_path).unwrap();
    let mut iter = reader.iter();
    
    let mut prev_key: Option<Vec<u8>> = None;
    while let Some(result) = iter.next() {
        let (key, _) = result.unwrap();
        if let Some(ref prev) = prev_key {
            assert!(key.as_ref() > prev.as_slice());
        }
        prev_key = Some(key.to_vec());
    }
}

#[test]
fn test_sstable_integration_with_storage() {
    use hanshiro_storage::memtable::{MemTable, MemTableConfig};
    use hanshiro_core::metrics::Metrics;
    use hanshiro_core::types::{Event, EventType, EventSource, IngestionFormat};
    use std::sync::Arc;
    
    let temp_dir = TempDir::new().unwrap();
    let sstable_path = temp_dir.path().join("test_integration.sst");
    
    // Create events
    let mut events = Vec::new();
    for i in 0..100 {
        let event = Event::new(
            EventType::NetworkConnection,
            EventSource {
                host: format!("host-{}", i),
                ip: Some("10.0.0.1".parse().unwrap()),
                collector: "test".to_string(),
                format: IngestionFormat::Raw,
            },
            vec![i as u8; 100],
        );
        events.push(event);
    }
    
    // Sort events by ID for SSTable
    events.sort_by(|a, b| a.id.to_string().cmp(&b.id.to_string()));
    
    // Write to SSTable
    let config = SSTableConfig::default();
    let mut writer = SSTableWriter::new(&sstable_path, config).unwrap();
    
    for event in &events {
        let key = event.id.to_string().into_bytes();
        let value = rmp_serde::to_vec(event).unwrap();
        writer.add(&key, &value).unwrap();
    }
    
    writer.finish().unwrap();
    
    // Read back and verify
    let reader = SSTableReader::open(&sstable_path).unwrap();
    
    for event in &events {
        let key = event.id.to_string().into_bytes();
        let value = reader.get(&key).unwrap().unwrap();
        let decoded: Event = rmp_serde::from_slice(&value).unwrap();
        assert_eq!(decoded.id, event.id);
        assert_eq!(decoded.raw_data, event.raw_data);
    }
}