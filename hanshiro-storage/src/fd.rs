//! # File Descriptor Management
//!
//! Manages file descriptors to prevent exhaustion under sustained load.
//! Provides:
//! - LRU pool for SSTable readers
//! - FD usage monitoring
//! - Backpressure when approaching limits

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use tracing::{debug, info, warn};

use hanshiro_core::error::{Error, Result};
use crate::sstable::SSTableReader;

/// FD pool configuration
#[derive(Debug, Clone)]
pub struct FdConfig {
    /// Max SSTable readers to keep open
    pub max_open_sstables: usize,
    /// Percentage of system limit to use as soft cap (0.0-1.0)
    pub soft_limit_ratio: f64,
    /// Enable backpressure when approaching limit
    pub enable_backpressure: bool,
}

impl Default for FdConfig {
    fn default() -> Self {
        Self {
            max_open_sstables: 256,
            soft_limit_ratio: 0.8,
            enable_backpressure: true,
        }
    }
}

/// File descriptor statistics
#[derive(Debug, Clone, Default)]
pub struct FdStats {
    pub open_sstables: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub system_limit: u64,
    pub estimated_used: u64,
}

/// LRU pool for SSTable readers
pub struct SSTablePool {
    config: FdConfig,
    cache: Mutex<LruCache<PathBuf, Arc<SSTableReader>>>,
    stats: FdPoolStats,
    system_fd_limit: u64,
}

struct FdPoolStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    current_open: AtomicUsize,
}

impl SSTablePool {
    pub fn new(config: FdConfig) -> Self {
        let system_fd_limit = get_fd_limit();
        let max_size = config.max_open_sstables.min(
            ((system_fd_limit as f64 * config.soft_limit_ratio) as usize).saturating_sub(64) // Reserve 64 for WAL, etc.
        );

        info!(
            "SSTable pool initialized: max={}, system_limit={}",
            max_size, system_fd_limit
        );

        Self {
            config,
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(max_size.max(1)).unwrap())),
            stats: FdPoolStats {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
                current_open: AtomicUsize::new(0),
            },
            system_fd_limit,
        }
    }

    /// Get or open an SSTable reader
    pub fn get(&self, path: &Path) -> Result<Arc<SSTableReader>> {
        let path_buf = path.to_path_buf();
        
        // Check cache first
        {
            let mut cache = self.cache.lock();
            if let Some(reader) = cache.get(&path_buf) {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::clone(reader));
            }
        }

        // Cache miss - need to open
        self.stats.misses.fetch_add(1, Ordering::Relaxed);

        // Check backpressure
        if self.config.enable_backpressure && self.should_backpressure() {
            return Err(Error::Internal {
                message: "FD limit approaching, backpressure active".to_string(),
            });
        }

        // Open new reader
        let reader = Arc::new(SSTableReader::open(path)?);
        
        // Insert into cache (may evict old entry)
        {
            let mut cache = self.cache.lock();
            let old_len = cache.len();
            cache.put(path_buf, Arc::clone(&reader));
            
            if cache.len() <= old_len && old_len > 0 {
                // An eviction occurred
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
            
            self.stats.current_open.store(cache.len(), Ordering::Relaxed);
        }

        Ok(reader)
    }

    /// Remove an SSTable from the pool (e.g., after compaction deletes it)
    pub fn remove(&self, path: &Path) {
        let mut cache = self.cache.lock();
        if cache.pop(&path.to_path_buf()).is_some() {
            self.stats.current_open.store(cache.len(), Ordering::Relaxed);
            debug!("Removed SSTable from pool: {:?}", path);
        }
    }

    /// Clear all cached readers
    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        cache.clear();
        self.stats.current_open.store(0, Ordering::Relaxed);
    }

    /// Get current statistics
    pub fn stats(&self) -> FdStats {
        FdStats {
            open_sstables: self.stats.current_open.load(Ordering::Relaxed),
            cache_hits: self.stats.hits.load(Ordering::Relaxed),
            cache_misses: self.stats.misses.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            system_limit: self.system_fd_limit,
            estimated_used: estimate_open_fds(),
        }
    }

    /// Check if we should apply backpressure
    fn should_backpressure(&self) -> bool {
        let current = estimate_open_fds();
        let threshold = (self.system_fd_limit as f64 * self.config.soft_limit_ratio) as u64;
        current >= threshold
    }
}

/// Global FD monitor for the entire process
pub struct FdMonitor {
    system_limit: u64,
    soft_limit_ratio: f64,
    check_interval_ms: u64,
}

impl FdMonitor {
    pub fn new(soft_limit_ratio: f64) -> Self {
        Self {
            system_limit: get_fd_limit(),
            soft_limit_ratio,
            check_interval_ms: 1000,
        }
    }

    /// Check current FD usage
    pub fn check(&self) -> FdStatus {
        let current = estimate_open_fds();
        let threshold = (self.system_limit as f64 * self.soft_limit_ratio) as u64;
        
        FdStatus {
            current,
            limit: self.system_limit,
            threshold,
            healthy: current < threshold,
            usage_ratio: current as f64 / self.system_limit as f64,
        }
    }

    /// Returns true if it's safe to open more files
    pub fn can_open(&self, count: usize) -> bool {
        let current = estimate_open_fds();
        let threshold = (self.system_limit as f64 * self.soft_limit_ratio) as u64;
        current + count as u64 <= threshold
    }
}

/// FD status report
#[derive(Debug, Clone)]
pub struct FdStatus {
    pub current: u64,
    pub limit: u64,
    pub threshold: u64,
    pub healthy: bool,
    pub usage_ratio: f64,
}

/// Get system file descriptor limit
#[cfg(unix)]
fn get_fd_limit() -> u64 {
    use std::io::{BufRead, BufReader};
    use std::fs::File;

    // Try /proc/self/limits first (Linux)
    if let Ok(file) = File::open("/proc/self/limits") {
        let reader = BufReader::new(file);
        for line in reader.lines().flatten() {
            if line.starts_with("Max open files") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 4 {
                    if let Ok(limit) = parts[3].parse::<u64>() {
                        return limit;
                    }
                }
            }
        }
    }

    // Fallback to getrlimit
    unsafe {
        let mut rlim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            return rlim.rlim_cur as u64;
        }
    }

    // Default fallback
    1024
}

#[cfg(not(unix))]
fn get_fd_limit() -> u64 {
    // Windows default
    8192
}

/// Estimate current open file descriptors
#[cfg(target_os = "linux")]
fn estimate_open_fds() -> u64 {
    std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn estimate_open_fds() -> u64 {
    // On macOS, use lsof equivalent via proc_pidinfo
    // For simplicity, use a rough estimate based on tracked resources
    // In production, could use libproc
    unsafe {
        let pid = libc::getpid();
        let mut rusage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut rusage) == 0 {
            // This doesn't give FD count directly, but we can track internally
        }
    }
    // Fallback: count /dev/fd entries
    std::fs::read_dir("/dev/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or(0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn estimate_open_fds() -> u64 {
    0 // Can't easily estimate on other platforms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fd_limit_detection() {
        let limit = get_fd_limit();
        assert!(limit > 0, "Should detect a positive FD limit");
        println!("Detected FD limit: {}", limit);
    }

    #[test]
    fn test_fd_monitor() {
        let monitor = FdMonitor::new(0.8);
        let status = monitor.check();
        
        assert!(status.limit > 0);
        assert!(status.usage_ratio >= 0.0 && status.usage_ratio <= 1.0);
        println!("FD status: {:?}", status);
    }

    #[test]
    fn test_sstable_pool_config() {
        let config = FdConfig {
            max_open_sstables: 128,
            soft_limit_ratio: 0.7,
            enable_backpressure: true,
        };
        let pool = SSTablePool::new(config);
        let stats = pool.stats();
        
        assert_eq!(stats.open_sstables, 0);
        assert_eq!(stats.cache_hits, 0);
    }
}
