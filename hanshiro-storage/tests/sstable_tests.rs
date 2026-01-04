//! # Comprehensive SSTable Tests
//!
//! This test suite covers:
//! - Basic write/read operations
//! - Block compression and decompression
//! - Index and bloom filter functionality
//! - Iterator correctness
//! - Performance characteristics
//! - Corruption handling

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};

use hanshiro_core::{
    types::*,
    metrics::Metrics,
};
use hanshiro_storage::sstable::{
    SSTableWriter, SSTableReader, SSTableConfig, CompressionType, SSTableInfo
};

/// Helper to generate test key-value pairs
fn generate_test_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut data = Vec::new();
    for i in 0..count {
        let key = format!("key_{:0width$}", i, width = key_size);
        let value = format!("value_{}_", i).repeat(value_size / 10);
        data.push((key.into_bytes(), value.into_bytes()));
    }
    data
}

/// Helper to create events as key-value pairs
fn event_to_kv(event: &Event) -> (Vec<u8>, Vec<u8>) {
    let key = event.id.to_string().into_bytes();
    let value = bincode::serialize(event).unwrap();
    (key, value)
}

#[test]
fn test_sstable_basic_write_read() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    // Write SSTable
    let info = {
        let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
        
        let data = generate_test_data(100, 10, 100);
        for (key, value) in &data {
            writer.add(key, value).unwrap();
        }
        
        writer.finish().unwrap()
    };
    
    assert_eq!(info.entry_count, 100);
    assert!(info.file_size > 0);
    
    // Read SSTable
    {
        let reader = SSTableReader::open(path).unwrap();
        
        // Test point queries
        let value = reader.get(b"key_0000000050").unwrap().unwrap();
        assert!(String::from_utf8_lossy(&value).contains("value_50"));
        
        // Test missing key
        assert!(reader.get(b"key_9999999999").unwrap().is_none());
        
        // Test iteration
        let mut count = 0;
        for result in reader.iter() {
            let (key, value) = result.unwrap();
            count += 1;
            
            // Verify ordering
            if count > 1 {
                let prev_key = format!("key_{:010}", count - 2);
                assert!(key.as_ref() > prev_key.as_bytes());
            }
        }
        assert_eq!(count, 100);
    }
}

#[test]
fn test_sstable_compression_types() {
    let compression_types = vec![
        CompressionType::None,
        CompressionType::Lz4,
        CompressionType::Zstd,
        CompressionType::Snappy,
    ];
    
    for compression in compression_types {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let config = SSTableConfig {
            compression,
            ..Default::default()
        };
        
        // Write with compression
        let uncompressed_size = {
            let mut writer = SSTableWriter::new(path, config).unwrap();
            
            // Use repetitive data for better compression
            let mut total_size = 0;
            for i in 0..1000 {
                let key = format!("key_{:08}", i);
                let value = format!("This is value {} with lots of repetitive text repetitive text repetitive text", i);
                total_size += key.len() + value.len();
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            let info = writer.finish().unwrap();
            println!("{:?} compression: {} bytes (from ~{} bytes)", 
                compression, info.file_size, total_size);
            
            total_size
        };
        
        // Read and verify
        {
            let reader = SSTableReader::open(path).unwrap();
            
            // Verify some entries
            for i in (0..1000).step_by(100) {
                let key = format!("key_{:08}", i);
                let value = reader.get(key.as_bytes()).unwrap().unwrap();
                let expected = format!("This is value {} with lots of repetitive text repetitive text repetitive text", i);
                assert_eq!(&value[..], expected.as_bytes());
            }
            
            // Count all entries
            let count = reader.iter().count();
            assert_eq!(count, 1000);
        }
        
        // Compressed files should be smaller (except None)
        if compression != CompressionType::None {
            let file_size = std::fs::metadata(path).unwrap().len();
            assert!(file_size < uncompressed_size as u64 / 2);
        }
    }
}

#[test]
fn test_sstable_large_values() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    // Test various value sizes
    let sizes = vec![
        100,           // 100B
        1024,          // 1KB
        10 * 1024,     // 10KB
        100 * 1024,    // 100KB
        1024 * 1024,   // 1MB
    ];
    
    {
        let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
        
        for (i, size) in sizes.iter().enumerate() {
            let key = format!("key_{:04}", i);
            let value = vec![b'x'; *size];
            writer.add(key.as_bytes(), &value).unwrap();
        }
        
        writer.finish().unwrap();
    }
    
    // Read back and verify
    {
        let reader = SSTableReader::open(path).unwrap();
        
        for (i, size) in sizes.iter().enumerate() {
            let key = format!("key_{:04}", i);
            let value = reader.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(value.len(), *size);
            assert!(value.iter().all(|&b| b == b'x'));
        }
    }
}

#[test]
fn test_sstable_block_boundaries() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    let config = SSTableConfig {
        block_size: 1024, // Small blocks for testing
        index_interval: 10, // Index every 10th key
        ..Default::default()
    };
    
    {
        let mut writer = SSTableWriter::new(path, config).unwrap();
        
        // Write enough data to span multiple blocks
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = vec![b'v'; 100]; // Each entry ~110 bytes
            writer.add(key.as_bytes(), &value).unwrap();
        }
        
        writer.finish().unwrap();
    }
    
    // Read and verify all entries are accessible
    {
        let reader = SSTableReader::open(path).unwrap();
        
        // Test entries that should be in different blocks
        for i in (0..100).step_by(15) {
            let key = format!("key_{:04}", i);
            let value = reader.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(value.len(), 100);
        }
        
        // Verify iteration works across blocks
        let count = reader.iter().count();
        assert_eq!(count, 100);
    }
}

#[test]
fn test_sstable_bloom_filter_effectiveness() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    let config = SSTableConfig {
        bloom_bits_per_key: 10,
        ..Default::default()
    };
    
    // Write SSTable with bloom filter
    {
        let mut writer = SSTableWriter::new(path, config).unwrap();
        
        for i in 0..10000 {
            let key = format!("existing_key_{:06}", i);
            let value = format!("value_{}", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        writer.finish().unwrap();
    }
    
    // Test bloom filter with non-existent keys
    {
        let reader = SSTableReader::open(path).unwrap();
        
        // These keys don't exist
        let mut false_positives = 0;
        let tests = 1000;
        
        for i in 0..tests {
            let key = format!("nonexistent_key_{:06}", i);
            if reader.get(key.as_bytes()).unwrap().is_some() {
                false_positives += 1;
            }
        }
        
        let fp_rate = false_positives as f64 / tests as f64;
        println!("Bloom filter false positive rate: {:.2}%", fp_rate * 100.0);
        
        // Should be less than 2% with 10 bits per key
        assert!(fp_rate < 0.02);
    }
}

#[test]
fn test_sstable_iterator_correctness() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    // Write SSTable
    let expected_data = {
        let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
        
        let data = generate_test_data(500, 8, 50);
        for (key, value) in &data {
            writer.add(key, value).unwrap();
        }
        
        writer.finish().unwrap();
        data
    };
    
    // Read with iterator and verify
    {
        let reader = SSTableReader::open(path).unwrap();
        let mut iter_data = Vec::new();
        
        for result in reader.iter() {
            let (key, value) = result.unwrap();
            iter_data.push((key.to_vec(), value.to_vec()));
        }
        
        assert_eq!(iter_data.len(), expected_data.len());
        
        // Verify all data matches
        for (i, (key, value)) in iter_data.iter().enumerate() {
            assert_eq!(key, &expected_data[i].0);
            assert_eq!(value, &expected_data[i].1);
        }
    }
}

#[test]
fn test_sstable_concurrent_reads() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    // Write SSTable
    {
        let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
        
        for i in 0..1000 {
            let key = format!("key_{:06}", i);
            let value = format!("value_{}", i).repeat(10);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        writer.finish().unwrap();
    }
    
    // Spawn multiple readers
    let num_threads = 10;
    let mut handles = Vec::new();
    
    for thread_id in 0..num_threads {
        let path = path.to_path_buf();
        let handle = std::thread::spawn(move || {
            let reader = SSTableReader::open(&path).unwrap();
            
            let mut found = 0;
            for i in 0..100 {
                let key = format!("key_{:06}", thread_id * 100 + i);
                if reader.get(key.as_bytes()).unwrap().is_some() {
                    found += 1;
                }
            }
            found
        });
        handles.push(handle);
    }
    
    // All threads should find all their keys
    for handle in handles {
        let found = handle.join().unwrap();
        assert_eq!(found, 100);
    }
}

#[test]
fn test_sstable_min_max_keys() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    // Write SSTable
    let info = {
        let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
        
        // Write keys in specific order
        let keys = vec!["aaa", "bbb", "ccc", "xxx", "yyy", "zzz"];
        for key in &keys {
            writer.add(key.as_bytes(), b"value").unwrap();
        }
        
        writer.finish().unwrap()
    };
    
    // Check min/max keys
    assert_eq!(&info.min_key[..], b"aaa");
    assert_eq!(&info.max_key[..], b"zzz");
    
    // Verify with reader
    {
        let reader = SSTableReader::open(path).unwrap();
        
        // Keys within range should exist
        assert!(reader.get(b"bbb").unwrap().is_some());
        assert!(reader.get(b"xxx").unwrap().is_some());
        
        // Keys outside range should not exist
        assert!(reader.get(b"000").unwrap().is_none());
        assert!(reader.get(b"~~~").unwrap().is_none());
    }
}

#[test]
fn test_sstable_performance_benchmark() {
    let temp_dir = TempDir::new().unwrap();
    
    // Benchmark write performance
    let write_path = temp_dir.path().join("write_bench.sst");
    let write_start = std::time::Instant::now();
    let num_entries = 100_000;
    
    let info = {
        let mut writer = SSTableWriter::new(&write_path, SSTableConfig::default()).unwrap();
        
        for i in 0..num_entries {
            let key = format!("key_{:08}", i);
            let value = format!("value_{}", i).repeat(10);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        writer.finish().unwrap()
    };
    
    let write_duration = write_start.elapsed();
    let write_throughput = num_entries as f64 / write_duration.as_secs_f64();
    
    println!("SSTable Write Performance:");
    println!("  Entries: {}", num_entries);
    println!("  File size: {} MB", info.file_size / (1024 * 1024));
    println!("  Duration: {:?}", write_duration);
    println!("  Throughput: {:.0} entries/sec", write_throughput);
    
    // Benchmark read performance
    let reader = SSTableReader::open(&write_path).unwrap();
    
    // Random reads
    let read_start = std::time::Instant::now();
    let num_reads = 10_000;
    let mut found = 0;
    
    for i in 0..num_reads {
        let key = format!("key_{:08}", i * 10);
        if reader.get(key.as_bytes()).unwrap().is_some() {
            found += 1;
        }
    }
    
    let read_duration = read_start.elapsed();
    let read_throughput = num_reads as f64 / read_duration.as_secs_f64();
    
    println!("\nSSTable Read Performance:");
    println!("  Reads: {}", num_reads);
    println!("  Found: {}", found);
    println!("  Duration: {:?}", read_duration);
    println!("  Throughput: {:.0} reads/sec", read_throughput);
    println!("  Latency: {:.0} μs/read", read_duration.as_micros() as f64 / num_reads as f64);
    
    // Benchmark iteration
    let iter_start = std::time::Instant::now();
    let count = reader.iter().count();
    let iter_duration = iter_start.elapsed();
    
    println!("\nSSTable Iteration Performance:");
    println!("  Entries: {}", count);
    println!("  Duration: {:?}", iter_duration);
    println!("  Throughput: {:.0} entries/sec", count as f64 / iter_duration.as_secs_f64());
}

#[test]
fn test_sstable_corruption_handling() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    // Write valid SSTable
    {
        let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
        
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{}", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        writer.finish().unwrap();
    }
    
    // Corrupt the file
    {
        let mut data = std::fs::read(path).unwrap();
        // Corrupt some bytes in the middle
        if data.len() > 1000 {
            data[500] ^= 0xFF;
            data[501] ^= 0xFF;
            data[502] ^= 0xFF;
        }
        std::fs::write(path, data).unwrap();
    }
    
    // Try to read corrupted file
    match SSTableReader::open(path) {
        Ok(reader) => {
            // May succeed opening, but reads might fail
            let mut failures = 0;
            for i in 0..100 {
                let key = format!("key_{:04}", i);
                if reader.get(key.as_bytes()).is_err() {
                    failures += 1;
                }
            }
            println!("Read failures due to corruption: {}/100", failures);
            assert!(failures > 0);
        }
        Err(e) => {
            println!("Failed to open corrupted SSTable: {:?}", e);
        }
    }
}

// Property-based tests
#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;
    
    proptest! {
        #[test]
        fn test_sstable_handles_any_kv_size(
            key_size in 1..1000usize,
            value_size in 1..100_000usize,
            count in 1..100usize
        ) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();
            
            // Write
            {
                let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
                
                for i in 0..count {
                    let key = vec![b'k'; key_size];
                    let mut value = vec![b'v'; value_size];
                    // Make keys unique
                    if key.len() >= 8 {
                        key[0..8].copy_from_slice(&i.to_be_bytes());
                    }
                    writer.add(&key, &value).unwrap();
                }
                
                writer.finish().unwrap();
            }
            
            // Read and verify
            {
                let reader = SSTableReader::open(path).unwrap();
                let entries: Vec<_> = reader.iter().collect();
                assert_eq!(entries.len(), count);
                
                for (_, value) in entries {
                    assert_eq!(value.unwrap().1.len(), value_size);
                }
            }
        }
        
        #[test]
        fn test_sstable_maintains_sort_order(
            mut keys in prop::collection::vec(prop::collection::vec(0u8..255, 1..50), 10..100)
        ) {
            // Ensure unique keys
            keys.sort();
            keys.dedup();
            
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();
            
            // Write in sorted order
            {
                let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
                
                for key in &keys {
                    writer.add(key, b"value").unwrap();
                }
                
                writer.finish().unwrap();
            }
            
            // Read and verify order
            {
                let reader = SSTableReader::open(path).unwrap();
                let read_keys: Vec<_> = reader.iter()
                    .map(|r| r.unwrap().0.to_vec())
                    .collect();
                
                assert_eq!(read_keys.len(), keys.len());
                assert_eq!(read_keys, keys);
            }
        }
    }
}