//! # Compaction Strategy
//!
//! Handles merging and optimizing SSTables.

use std::path::PathBuf;
use hanshiro_core::Result;
use crate::sstable::{SSTableReader, SSTableWriter, SSTableConfig, SSTableInfo};

/// Compaction strategy trait
pub trait CompactionStrategy: Send + Sync {
    /// Select SSTables for compaction
    fn select_compaction(&self, sstables: &[SSTableInfo]) -> Vec<SSTableInfo>;
    
    /// Perform compaction
    fn compact(&self, sstables: Vec<SSTableInfo>, output_dir: &PathBuf) -> Result<Vec<SSTableInfo>>;
}

/// Level-based compaction strategy
pub struct LeveledCompaction {
    max_levels: u32,
    level_size_multiplier: u32,
}

impl LeveledCompaction {
    pub fn new() -> Self {
        Self {
            max_levels: 7,
            level_size_multiplier: 10,
        }
    }
}

impl CompactionStrategy for LeveledCompaction {
    fn select_compaction(&self, sstables: &[SSTableInfo]) -> Vec<SSTableInfo> {
        // Simple implementation - select oldest SSTables at level 0
        let mut level0: Vec<_> = sstables
            .iter()
            .filter(|s| s.level == 0)
            .cloned()
            .collect();
            
        level0.sort_by_key(|s| s.creation_time);
        level0.into_iter().take(4).collect()
    }
    
    fn compact(&self, sstables: Vec<SSTableInfo>, output_dir: &PathBuf) -> Result<Vec<SSTableInfo>> {
        // TODO: Implement actual compaction logic
        Ok(vec![])
    }
}