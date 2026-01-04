//! # Manifest - SSTable Metadata Management
//!
//! The manifest tracks all SSTables in the system and their metadata.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    pub sstables: Vec<SSTableManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableManifestEntry {
    pub id: u64,
    pub level: u32,
    pub path: PathBuf,
    pub size: u64,
    pub entry_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub creation_time: u64,
    pub compaction_score: f64,
}

impl Manifest {
    pub fn new() -> Self {
        Self {
            version: 0,
            sstables: Vec::new(),
        }
    }
}