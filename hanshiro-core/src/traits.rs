//! # Core Traits
//!
//! Defines the fundamental interfaces that all HanshiroDB components implement.
//!
//! ## Design Philosophy
//!
//! 1. **Async-First**: All I/O operations are async
//! 2. **Error Propagation**: All operations return Result
//! 3. **Extensibility**: Traits allow multiple implementations
//! 4. **Testability**: Traits enable easy mocking

use async_trait::async_trait;
use crate::{Result, Event, EventId, Vector, VectorId, types::QueryRequest};

/// Storage engine trait
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Write an event to storage
    async fn write(&self, event: Event) -> Result<EventId>;
    
    /// Read an event by ID
    async fn read(&self, id: EventId) -> Result<Option<Event>>;
    
    /// Batch write multiple events
    async fn write_batch(&self, events: Vec<Event>) -> Result<Vec<EventId>>;
    
    /// Scan events in time range
    async fn scan(&self, start: u64, end: u64) -> Result<Vec<Event>>;
    
    /// Get storage statistics
    async fn stats(&self) -> Result<StorageStats>;
    
    /// Flush pending writes to disk
    async fn flush(&self) -> Result<()>;
    
    /// Compact storage files
    async fn compact(&self) -> Result<CompactionResult>;
}

/// Vector index trait
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Add vector to index
    async fn add(&self, vector: Vector, metadata: Vec<u8>) -> Result<()>;
    
    /// Search for similar vectors
    async fn search(&self, query: &Vector, k: usize, threshold: f32) -> Result<Vec<SearchResult>>;
    
    /// Update vector in index
    async fn update(&self, id: VectorId, vector: Vector) -> Result<()>;
    
    /// Delete vector from index
    async fn delete(&self, id: VectorId) -> Result<()>;
    
    /// Get index statistics
    async fn stats(&self) -> Result<IndexStats>;
    
    /// Rebuild index from scratch
    async fn rebuild(&self) -> Result<()>;
}

/// Metadata index trait
#[async_trait]
pub trait MetadataIndex: Send + Sync {
    /// Add metadata entry
    async fn add(&self, key: &str, value: &str, doc_id: u64) -> Result<()>;
    
    /// Search by metadata
    async fn search(&self, key: &str, value: &str) -> Result<Vec<u64>>;
    
    /// Range query on metadata
    async fn range(&self, key: &str, start: &str, end: &str) -> Result<Vec<u64>>;
    
    /// Delete metadata entry
    async fn delete(&self, doc_id: u64) -> Result<()>;
    
    /// Get cardinality of a key
    async fn cardinality(&self, key: &str) -> Result<u64>;
}

/// Query executor trait
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Execute a query
    async fn execute(&self, query: QueryRequest) -> Result<QueryResult>;
    
    /// Explain query plan
    async fn explain(&self, query: QueryRequest) -> Result<QueryPlan>;
    
    /// Estimate query cost
    async fn estimate_cost(&self, query: QueryRequest) -> Result<QueryCost>;
}

/// Data ingestion trait
#[async_trait]
pub trait Ingester: Send + Sync {
    /// Ingest raw data
    async fn ingest(&self, data: &[u8], format: &str) -> Result<Vec<Event>>;
    
    /// Validate data format
    async fn validate(&self, data: &[u8], format: &str) -> Result<ValidationResult>;
    
    /// Get supported formats
    fn supported_formats(&self) -> Vec<String>;
}

/// Vectorizer trait
#[async_trait]
pub trait Vectorizer: Send + Sync {
    /// Convert text to vector
    async fn vectorize(&self, text: &str) -> Result<Vector>;
    
    /// Batch vectorization
    async fn vectorize_batch(&self, texts: Vec<String>) -> Result<Vec<Vector>>;
    
    /// Get vector dimension
    fn dimension(&self) -> u16;
    
    /// Get model info
    fn model_info(&self) -> ModelInfo;
}

// Supporting types

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_events: u64,
    pub total_bytes: u64,
    pub wal_size: u64,
    pub sstable_count: u32,
    pub memtable_size: u64,
    pub compaction_pending: bool,
}

/// Compaction result
#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub files_compacted: u32,
    pub bytes_reclaimed: u64,
    pub duration_ms: u64,
}

/// Vector search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: VectorId,
    pub score: f32,
    pub metadata: Vec<u8>,
}

/// Index statistics
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub vector_count: u64,
    pub index_size_bytes: u64,
    pub dimensions: u16,
    pub graph_degree: u32,
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub events: Vec<Event>,
    pub total_matches: u64,
    pub execution_time_ms: u64,
}

/// Query execution plan
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub steps: Vec<QueryStep>,
    pub estimated_cost: QueryCost,
}

/// Query execution step
#[derive(Debug, Clone)]
pub struct QueryStep {
    pub operation: String,
    pub estimated_rows: u64,
    pub cost: f64,
}

/// Query cost estimation
#[derive(Debug, Clone)]
pub struct QueryCost {
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub memory_bytes: u64,
}

/// Validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
}

/// Validation error
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
    pub line: Option<u32>,
}

/// Model information
#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub name: String,
    pub version: String,
    pub dimension: u16,
    pub max_tokens: u32,
}