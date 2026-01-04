//! # Block Cache
//!
//! LRU cache for frequently accessed SSTable blocks.

use std::sync::Arc;
use dashmap::DashMap;
use bytes::Bytes;

pub struct BlockCache {
    cache: Arc<DashMap<CacheKey, Bytes>>,
    max_size: usize,
    current_size: std::sync::atomic::AtomicUsize,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub file_id: u64,
    pub block_offset: u64,
}

impl BlockCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            max_size,
            current_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        self.cache.get(key).map(|entry| entry.value().clone())
    }
    
    pub fn insert(&self, key: CacheKey, value: Bytes) {
        // Simple implementation - in production would need proper eviction
        if self.current_size.load(std::sync::atomic::Ordering::Relaxed) < self.max_size {
            self.cache.insert(key, value.clone());
            self.current_size.fetch_add(value.len(), std::sync::atomic::Ordering::Relaxed);
        }
    }
}