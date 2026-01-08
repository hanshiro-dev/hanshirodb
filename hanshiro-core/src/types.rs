//! # Core Types
//!
//! Fundamental data structures used throughout HanshiroDB.
//!
//! ## Type Design Philosophy
//!
//! 1. **Zero-Copy**: Use rkyv for zero-copy deserialization
//! 2. **Type Safety**: Strong typing prevents runtime errors
//! 3. **Serialization**: All types support efficient rkyv serialization
//! 4. **Validation**: Types validate their invariants

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Unique identifier for events (128-bit UUID stored as two u64s for rkyv)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct EventId {
    pub hi: u64,
    pub lo: u64,
}

impl EventId {
    pub fn new() -> Self {
        let uuid = uuid::Uuid::new_v4();
        let (hi, lo) = uuid.as_u64_pair();
        Self { hi, lo }
    }
    
    pub fn to_uuid(&self) -> uuid::Uuid {
        uuid::Uuid::from_u64_pair(self.hi, self.lo)
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_uuid())
    }
}

/// Unique identifier for vectors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct VectorId {
    pub hi: u64,
    pub lo: u64,
}

impl VectorId {
    pub fn new() -> Self {
        let uuid = uuid::Uuid::new_v4();
        let (hi, lo) = uuid.as_u64_pair();
        Self { hi, lo }
    }
}

impl Default for VectorId {
    fn default() -> Self {
        Self::new()
    }
}

/// Vector data structure
#[derive(Debug, Clone, PartialEq)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct Vector {
    pub id: VectorId,
    pub data: Vec<f32>,
    pub dimension: u16,
    pub normalized: bool,
}

impl Vector {
    pub fn new(data: Vec<f32>) -> Result<Self, &'static str> {
        if data.is_empty() {
            return Err("Vector dimension must be positive");
        }
        if data.len() > 4096 {
            return Err("Vector dimension too large (max 4096)");
        }
        Ok(Self {
            id: VectorId::new(),
            dimension: data.len() as u16,
            data,
            normalized: false,
        })
    }
    
    pub fn normalize(&mut self) {
        let norm: f32 = self.data.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            self.data.iter_mut().for_each(|x| *x /= norm);
            self.normalized = true;
        }
    }
}

/// Event type enumeration
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum EventType {
    NetworkConnection,
    NetworkTraffic,
    DNSQuery,
    HTTPRequest,
    Authentication,
    Authorization,
    PasswordChange,
    FileCreate,
    FileModify,
    FileDelete,
    FileAccess,
    ProcessStart,
    ProcessStop,
    ProcessInject,
    MalwareDetected,
    AnomalyDetected,
    PolicyViolation,
    Custom(String),
}

/// Supported ingestion formats
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum IngestionFormat {
    OCSF,
    STIX,
    TAXII,
    Zeek,
    Suricata,
    PEHeader,
    CEF,
    LEEF,
    Raw,
    Custom(String),
}

/// Event source information
#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct EventSource {
    pub host: String,
    pub ip: Option<String>,  // IP as string for rkyv compatibility
    pub collector: String,
    pub format: IngestionFormat,
}

/// Core event structure - optimized for rkyv zero-copy serialization
#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct Event {
    pub id: EventId,
    pub timestamp_ms: u64,  // Milliseconds since epoch (faster than DateTime)
    pub event_type: EventType,
    pub source: EventSource,
    pub raw_data: Vec<u8>,
    pub metadata_json: String,  // JSON string for flexible metadata
    pub vector: Option<Vector>,
    pub merkle_prev: Option<String>,
    pub merkle_hash: Option<String>,
}

impl Event {
    pub fn new(event_type: EventType, source: EventSource, raw_data: Vec<u8>) -> Self {
        Self {
            id: EventId::new(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            event_type,
            source,
            raw_data,
            metadata_json: "{}".to_string(),
            vector: None,
            merkle_prev: None,
            merkle_hash: None,
        }
    }
    
    /// Add metadata field (merges into JSON)
    pub fn add_metadata<K: AsRef<str>>(&mut self, key: K, value: serde_json::Value) {
        let mut map: HashMap<String, serde_json::Value> = 
            serde_json::from_str(&self.metadata_json).unwrap_or_default();
        map.insert(key.as_ref().to_string(), value);
        self.metadata_json = serde_json::to_string(&map).unwrap_or_default();
    }
    
    /// Get metadata as HashMap
    pub fn metadata(&self) -> HashMap<String, serde_json::Value> {
        serde_json::from_str(&self.metadata_json).unwrap_or_default()
    }
    
    /// Get timestamp as chrono DateTime
    pub fn timestamp(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.timestamp_ms as i64)
            .unwrap_or_default()
    }
}

/// Query request structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub vector_query: Option<VectorQuery>,
    pub metadata_filters: Vec<MetadataFilter>,
    pub time_range: Option<TimeRange>,
    pub limit: usize,
    pub offset: usize,
}

/// Vector similarity query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorQuery {
    pub vector: Vector,
    pub threshold: f32,
    pub metric: SimilarityMetric,
}

/// Similarity metrics
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimilarityMetric {
    Cosine,
    Euclidean,
    DotProduct,
}

/// Metadata filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataFilter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: serde_json::Value,
}

/// Filter operators
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    In,
    NotIn,
    Contains,
    StartsWith,
    EndsWith,
}

/// Time range for queries (milliseconds since epoch)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ms: u64,
    pub end_ms: u64,
}

impl TimeRange {
    pub fn new(start_ms: u64, end_ms: u64) -> Result<Self, &'static str> {
        if start_ms > end_ms {
            Err("Start time must be before end time")
        } else {
            Ok(Self { start_ms, end_ms })
        }
    }
    
    pub fn contains(&self, timestamp_ms: u64) -> bool {
        timestamp_ms >= self.start_ms && timestamp_ms <= self.end_ms
    }
}