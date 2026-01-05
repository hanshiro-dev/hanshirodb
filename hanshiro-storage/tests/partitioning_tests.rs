//! # Time-Based Partitioning Tests
//!
//! Comprehensive tests for:
//! - Partition ID generation and boundaries
//! - Time range to partition mapping
//! - Retention policy enforcement
//! - SSTable organization by time
//! - Edge cases (midnight, DST, year boundaries)

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{Datelike, NaiveDate, TimeZone, Timelike, Utc};
use tempfile::TempDir;

use hanshiro_storage::partitioning::{
    PartitionId, PartitionManager, PartitionWindow, RetentionPolicy,
};

// =============================================================================
// Partition ID Tests
// =============================================================================

#[test]
fn test_partition_id_hourly() {
    // 2024-01-15 14:30:00 UTC = 1705329000
    let ts = 1705329000u64;
    
    let partition = PartitionId::from_timestamp(ts, PartitionWindow::Hourly);
    
    assert_eq!(partition.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    assert_eq!(partition.window, 14); // Hour 14
    assert_eq!(partition.dir_name(), "2024-01-15");
    assert_eq!(partition.file_prefix(PartitionWindow::Hourly), "14");
}

#[test]
fn test_partition_id_six_hour() {
    // Test each 6-hour window
    let base_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    
    // 00:00-06:00 -> window 0
    let ts_0 = Utc.from_utc_datetime(&base_date.and_hms_opt(3, 0, 0).unwrap()).timestamp() as u64;
    let p0 = PartitionId::from_timestamp(ts_0, PartitionWindow::SixHour);
    assert_eq!(p0.window, 0);
    assert_eq!(p0.file_prefix(PartitionWindow::SixHour), "00");
    
    // 06:00-12:00 -> window 1
    let ts_1 = Utc.from_utc_datetime(&base_date.and_hms_opt(9, 0, 0).unwrap()).timestamp() as u64;
    let p1 = PartitionId::from_timestamp(ts_1, PartitionWindow::SixHour);
    assert_eq!(p1.window, 1);
    assert_eq!(p1.file_prefix(PartitionWindow::SixHour), "06");
    
    // 12:00-18:00 -> window 2
    let ts_2 = Utc.from_utc_datetime(&base_date.and_hms_opt(15, 0, 0).unwrap()).timestamp() as u64;
    let p2 = PartitionId::from_timestamp(ts_2, PartitionWindow::SixHour);
    assert_eq!(p2.window, 2);
    assert_eq!(p2.file_prefix(PartitionWindow::SixHour), "12");
    
    // 18:00-24:00 -> window 3
    let ts_3 = Utc.from_utc_datetime(&base_date.and_hms_opt(21, 0, 0).unwrap()).timestamp() as u64;
    let p3 = PartitionId::from_timestamp(ts_3, PartitionWindow::SixHour);
    assert_eq!(p3.window, 3);
    assert_eq!(p3.file_prefix(PartitionWindow::SixHour), "18");
}

#[test]
fn test_partition_id_daily() {
    let ts = 1705329000u64; // 2024-01-15 14:30:00
    
    let partition = PartitionId::from_timestamp(ts, PartitionWindow::Daily);
    
    assert_eq!(partition.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    assert_eq!(partition.window, 0); // Daily always has window 0
    assert_eq!(partition.file_prefix(PartitionWindow::Daily), "00");
}

#[test]
fn test_partition_timestamps() {
    let partition = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 2, // 12:00-18:00 for SixHour
    };
    
    let start = partition.start_timestamp(PartitionWindow::SixHour);
    let end = partition.end_timestamp(PartitionWindow::SixHour);
    
    // Verify 6-hour duration
    assert_eq!(end - start, 6 * 3600);
    
    // Verify start is at 12:00
    let start_dt = chrono::DateTime::<Utc>::from_timestamp(start as i64, 0).unwrap();
    assert_eq!(start_dt.hour(), 12);
    assert_eq!(start_dt.minute(), 0);
}

#[test]
fn test_partition_ordering() {
    let p1 = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 0,
    };
    let p2 = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 1,
    };
    let p3 = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(),
        window: 0,
    };
    
    assert!(p1 < p2);
    assert!(p2 < p3);
    assert!(p1 < p3);
}

// =============================================================================
// Partition Manager Tests
// =============================================================================

#[test]
fn test_partition_manager_generate_path() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    let partition = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 2,
    };
    
    let path = manager.generate_sstable_path(&partition, 42, 1705329000);
    
    assert!(path.to_string_lossy().contains("2024-01-15"));
    assert!(path.to_string_lossy().contains("12_")); // Window 2 starts at hour 12
    assert!(path.to_string_lossy().ends_with(".sst"));
}

#[test]
fn test_partition_manager_ensure_dir() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    let partition = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 0,
    };
    
    let dir = manager.ensure_partition_dir(&partition).unwrap();
    
    assert!(dir.exists());
    assert!(dir.is_dir());
    assert!(dir.to_string_lossy().contains("2024-01-15"));
}

#[test]
fn test_partitions_for_range_same_day() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    let base = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    
    // 08:00 to 16:00 on same day
    let start = Utc.from_utc_datetime(&base.and_hms_opt(8, 0, 0).unwrap()).timestamp() as u64;
    let end = Utc.from_utc_datetime(&base.and_hms_opt(16, 0, 0).unwrap()).timestamp() as u64;
    
    let partitions = manager.partitions_for_range(start, end);
    
    // Should cover windows 1 (06-12) and 2 (12-18)
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].window, 1);
    assert_eq!(partitions[1].window, 2);
}

#[test]
fn test_partitions_for_range_multi_day() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    // 2024-01-15 20:00 to 2024-01-16 10:00
    let start = Utc.from_utc_datetime(
        &NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(20, 0, 0).unwrap()
    ).timestamp() as u64;
    let end = Utc.from_utc_datetime(
        &NaiveDate::from_ymd_opt(2024, 1, 16).unwrap().and_hms_opt(10, 0, 0).unwrap()
    ).timestamp() as u64;
    
    let partitions = manager.partitions_for_range(start, end);
    
    // Should cover: 01-15 window 3, 01-16 window 0, 01-16 window 1
    assert!(partitions.len() >= 3);
    
    // First partition should be 01-15
    assert_eq!(partitions[0].date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    
    // Last partition should be 01-16
    assert_eq!(partitions.last().unwrap().date, NaiveDate::from_ymd_opt(2024, 1, 16).unwrap());
}

#[test]
fn test_list_partitions_empty() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    let partitions = manager.list_partitions().unwrap();
    assert!(partitions.is_empty());
}

#[test]
fn test_list_partitions_with_data() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    // Create some partition directories with SST files
    let dates = ["2024-01-13", "2024-01-14", "2024-01-15"];
    for date in dates {
        let dir = temp_dir.path().join(date);
        fs::create_dir_all(&dir).unwrap();
        
        // Create SST files for different windows
        for hour in [0, 6, 12, 18] {
            let filename = format!("{:02}_1234567890_000001.sst", hour);
            File::create(dir.join(filename)).unwrap();
        }
    }
    
    let partitions = manager.list_partitions().unwrap();
    
    // 3 dates * 4 windows = 12 partitions
    assert_eq!(partitions.len(), 12);
    
    // Should be sorted
    assert!(partitions.windows(2).all(|w| w[0] <= w[1]));
}

#[test]
fn test_list_sstables_in_partition() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    // Create partition directory
    let partition = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 2, // 12:00-18:00
    };
    
    let dir = manager.ensure_partition_dir(&partition).unwrap();
    
    // Create SST files for this partition (prefix "12")
    File::create(dir.join("12_1705329000_000001.sst")).unwrap();
    File::create(dir.join("12_1705329100_000002.sst")).unwrap();
    File::create(dir.join("12_1705329200_000003.sst")).unwrap();
    
    // Create SST file for different partition (should not be included)
    File::create(dir.join("06_1705329000_000001.sst")).unwrap();
    
    let sstables = manager.list_sstables_in_partition(&partition).unwrap();
    
    assert_eq!(sstables.len(), 3);
    for sst in &sstables {
        assert!(sst.file_name().unwrap().to_string_lossy().starts_with("12_"));
    }
}

// =============================================================================
// Retention Policy Tests
// =============================================================================

#[test]
fn test_retention_default() {
    let policy = RetentionPolicy::default();
    
    assert_eq!(policy.retention_period, Duration::from_secs(30 * 24 * 3600));
    assert_eq!(policy.min_partitions, 24);
}

#[test]
fn test_partitions_to_delete_none_old() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy {
            retention_period: Duration::from_secs(7 * 24 * 3600), // 7 days
            cleanup_interval: Duration::from_secs(3600),
            min_partitions: 4,
        },
    );
    
    // Create recent partitions (today)
    let today = Utc::now().date_naive();
    let dir = temp_dir.path().join(today.format("%Y-%m-%d").to_string());
    fs::create_dir_all(&dir).unwrap();
    File::create(dir.join("00_1234567890_000001.sst")).unwrap();
    
    let to_delete = manager.partitions_to_delete().unwrap();
    
    assert!(to_delete.is_empty(), "Recent partitions should not be deleted");
}

#[test]
fn test_partitions_to_delete_old_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::Daily,
        RetentionPolicy {
            retention_period: Duration::from_secs(3 * 24 * 3600), // 3 days
            cleanup_interval: Duration::from_secs(3600),
            min_partitions: 1,
        },
    );
    
    // Create old partition (30 days ago)
    let old_date = Utc::now().date_naive() - chrono::Duration::days(30);
    let old_dir = temp_dir.path().join(old_date.format("%Y-%m-%d").to_string());
    fs::create_dir_all(&old_dir).unwrap();
    File::create(old_dir.join("00_1234567890_000001.sst")).unwrap();
    
    // Create recent partition (today)
    let today = Utc::now().date_naive();
    let today_dir = temp_dir.path().join(today.format("%Y-%m-%d").to_string());
    fs::create_dir_all(&today_dir).unwrap();
    File::create(today_dir.join("00_1234567890_000002.sst")).unwrap();
    
    let to_delete = manager.partitions_to_delete().unwrap();
    
    assert_eq!(to_delete.len(), 1);
    assert_eq!(to_delete[0].date, old_date);
}

#[test]
fn test_partitions_to_delete_respects_min_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::Daily,
        RetentionPolicy {
            retention_period: Duration::from_secs(1), // Very short - everything is "old"
            cleanup_interval: Duration::from_secs(3600),
            min_partitions: 10, // But keep at least 10
        },
    );
    
    // Create 5 partitions
    for i in 0..5 {
        let date = Utc::now().date_naive() - chrono::Duration::days(i);
        let dir = temp_dir.path().join(date.format("%Y-%m-%d").to_string());
        fs::create_dir_all(&dir).unwrap();
        File::create(dir.join("00_1234567890_000001.sst")).unwrap();
    }
    
    let to_delete = manager.partitions_to_delete().unwrap();
    
    // Should not delete anything because we have fewer than min_partitions
    assert!(to_delete.is_empty());
}

#[test]
fn test_delete_partition() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    // Create partition with files
    let partition = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 0,
    };
    
    let dir = manager.ensure_partition_dir(&partition).unwrap();
    
    // Create SST files with some content
    for i in 0..3 {
        let mut file = File::create(dir.join(format!("00_123456789{}_00000{}.sst", i, i))).unwrap();
        file.write_all(&vec![0u8; 1024]).unwrap(); // 1KB each
    }
    
    assert!(dir.exists());
    
    let bytes_deleted = manager.delete_partition(&partition).unwrap();
    
    assert_eq!(bytes_deleted, 3 * 1024);
    assert!(!dir.exists(), "Directory should be removed when empty");
}

#[test]
fn test_enforce_retention() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::Daily,
        RetentionPolicy {
            retention_period: Duration::from_secs(3 * 24 * 3600), // 3 days
            cleanup_interval: Duration::from_secs(3600),
            min_partitions: 1,
        },
    );
    
    // Create old partitions
    for i in 10..15 {
        let date = Utc::now().date_naive() - chrono::Duration::days(i);
        let dir = temp_dir.path().join(date.format("%Y-%m-%d").to_string());
        fs::create_dir_all(&dir).unwrap();
        let mut file = File::create(dir.join("00_1234567890_000001.sst")).unwrap();
        file.write_all(&vec![0u8; 512]).unwrap();
    }
    
    // Create recent partition
    let today = Utc::now().date_naive();
    let today_dir = temp_dir.path().join(today.format("%Y-%m-%d").to_string());
    fs::create_dir_all(&today_dir).unwrap();
    File::create(today_dir.join("00_1234567890_000001.sst")).unwrap();
    
    let result = manager.enforce_retention().unwrap();
    
    assert_eq!(result.partitions_deleted, 5);
    assert_eq!(result.bytes_reclaimed, 5 * 512);
    assert!(result.errors.is_empty());
    
    // Verify old directories are gone
    for i in 10..15 {
        let date = Utc::now().date_naive() - chrono::Duration::days(i);
        let dir = temp_dir.path().join(date.format("%Y-%m-%d").to_string());
        assert!(!dir.exists());
    }
    
    // Verify recent directory still exists
    assert!(today_dir.exists());
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_partition_at_midnight() {
    // Exactly at midnight
    let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let midnight = Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap()).timestamp() as u64;
    
    let partition = PartitionId::from_timestamp(midnight, PartitionWindow::SixHour);
    
    assert_eq!(partition.date, date);
    assert_eq!(partition.window, 0);
}

#[test]
fn test_partition_at_window_boundary() {
    let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    
    // Exactly at 06:00:00
    let boundary = Utc.from_utc_datetime(&date.and_hms_opt(6, 0, 0).unwrap()).timestamp() as u64;
    let partition = PartitionId::from_timestamp(boundary, PartitionWindow::SixHour);
    
    assert_eq!(partition.window, 1); // Should be in window 1 (06-12)
    
    // One second before
    let before = boundary - 1;
    let partition_before = PartitionId::from_timestamp(before, PartitionWindow::SixHour);
    
    assert_eq!(partition_before.window, 0); // Should be in window 0 (00-06)
}

#[test]
fn test_partition_year_boundary() {
    // 2023-12-31 23:59:59
    let ts_2023 = Utc.from_utc_datetime(
        &NaiveDate::from_ymd_opt(2023, 12, 31).unwrap().and_hms_opt(23, 59, 59).unwrap()
    ).timestamp() as u64;
    
    // 2024-01-01 00:00:00
    let ts_2024 = Utc.from_utc_datetime(
        &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()
    ).timestamp() as u64;
    
    let p_2023 = PartitionId::from_timestamp(ts_2023, PartitionWindow::Daily);
    let p_2024 = PartitionId::from_timestamp(ts_2024, PartitionWindow::Daily);
    
    assert_eq!(p_2023.date.year(), 2023);
    assert_eq!(p_2024.date.year(), 2024);
    assert!(p_2023 < p_2024);
}

#[test]
fn test_partition_leap_year() {
    // 2024-02-29 (leap year)
    let leap_day = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
    let ts = Utc.from_utc_datetime(&leap_day.and_hms_opt(12, 0, 0).unwrap()).timestamp() as u64;
    
    let partition = PartitionId::from_timestamp(ts, PartitionWindow::Daily);
    
    assert_eq!(partition.date, leap_day);
    assert_eq!(partition.dir_name(), "2024-02-29");
}

#[test]
fn test_should_merge_partition() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy::default(),
    );
    
    let partition = PartitionId {
        date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        window: 0,
    };
    
    let dir = manager.ensure_partition_dir(&partition).unwrap();
    
    // Create 3 SST files
    for i in 0..3 {
        File::create(dir.join(format!("00_123456789{}_00000{}.sst", i, i))).unwrap();
    }
    
    // Should not merge with threshold of 5
    assert!(!manager.should_merge_partition(&partition, 5).unwrap());
    
    // Should merge with threshold of 2
    assert!(manager.should_merge_partition(&partition, 2).unwrap());
}

// =============================================================================
// Integration Tests
// =============================================================================

#[test]
fn test_full_partition_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let manager = PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::SixHour,
        RetentionPolicy {
            retention_period: Duration::from_secs(2 * 24 * 3600), // 2 days
            cleanup_interval: Duration::from_secs(3600),
            min_partitions: 2,
        },
    );
    
    // 1. Create partitions for multiple days
    let today = Utc::now().date_naive();
    let dates = [
        today - chrono::Duration::days(5), // Old - should be deleted
        today - chrono::Duration::days(4), // Old - should be deleted
        today - chrono::Duration::days(1), // Recent - keep
        today,                              // Today - keep
    ];
    
    for date in dates {
        for window in 0..4 {
            let partition = PartitionId { date, window };
            let dir = manager.ensure_partition_dir(&partition).unwrap();
            
            let prefix = match window {
                0 => "00", 1 => "06", 2 => "12", 3 => "18",
                _ => unreachable!(),
            };
            
            let mut file = File::create(dir.join(format!("{}_1234567890_000001.sst", prefix))).unwrap();
            file.write_all(&vec![0u8; 256]).unwrap();
        }
    }
    
    // 2. List all partitions
    let all_partitions = manager.list_partitions().unwrap();
    assert_eq!(all_partitions.len(), 16); // 4 dates * 4 windows
    
    // 3. Query partitions for a time range (yesterday to today)
    let yesterday_start = Utc.from_utc_datetime(
        &(today - chrono::Duration::days(1)).and_hms_opt(0, 0, 0).unwrap()
    ).timestamp() as u64;
    let today_end = Utc.from_utc_datetime(
        &today.and_hms_opt(23, 59, 59).unwrap()
    ).timestamp() as u64;
    
    let query_partitions = manager.partitions_for_range(yesterday_start, today_end);
    assert_eq!(query_partitions.len(), 8); // 2 days * 4 windows
    
    // 4. Enforce retention
    let result = manager.enforce_retention().unwrap();
    assert_eq!(result.partitions_deleted, 8); // 2 old days * 4 windows
    
    // 5. Verify remaining partitions
    let remaining = manager.list_partitions().unwrap();
    assert_eq!(remaining.len(), 8); // 2 recent days * 4 windows
    
    // All remaining should be recent
    for p in remaining {
        assert!(p.date >= today - chrono::Duration::days(1));
    }
}

#[test]
fn test_concurrent_partition_access() {
    use std::sync::Arc;
    use std::thread;
    
    let temp_dir = TempDir::new().unwrap();
    let manager = Arc::new(PartitionManager::new(
        temp_dir.path().to_path_buf(),
        PartitionWindow::Hourly,
        RetentionPolicy::default(),
    ));
    
    let mut handles = vec![];
    
    // Spawn multiple threads creating partitions
    for i in 0..10 {
        let manager_clone = Arc::clone(&manager);
        let handle = thread::spawn(move || {
            let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
            let partition = PartitionId { date, window: i };
            
            let dir = manager_clone.ensure_partition_dir(&partition).unwrap();
            
            // Create SST file
            let filename = format!("{:02}_1234567890_{:06}.sst", i, i);
            File::create(dir.join(filename)).unwrap();
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all partitions created
    let partitions = manager.list_partitions().unwrap();
    assert_eq!(partitions.len(), 10);
}
