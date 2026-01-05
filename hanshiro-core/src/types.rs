//! # Core Types
//!
//! Fundamental data structures used throughout HanshiroDB.
//!
//! ## Type Design Philosophy
//!
//! 1. **Zero-Copy**: Use references where possible
//! 2. **Type Safety**: Strong typing prevents runtime errors
//! 3. **Serialization**: All types support efficient serialization
//! 4. **Validation**: Types validate their invariants

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

/// Timestamp type used throughout the system
pub type Timestamp = DateTime<Utc>;

/// Unique identifier for events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventId(pub Uuid);

impl EventId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for vectors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorId(pub Uuid);

impl VectorId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Vector dimension (must be positive)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorDimension(u16);

impl VectorDimension {
    pub fn new(dim: u16) -> Result<Self, &'static str> {
        if dim == 0 {
            Err("Vector dimension must be positive")
        } else if dim > 4096 {
            Err("Vector dimension too large (max 4096)")
        } else {
            Ok(Self(dim))
        }
    }
    
    pub fn value(&self) -> u16 {
        self.0
    }
}

/// Vector data structure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vector {
    pub id: VectorId,
    pub data: Vec<f32>,
    pub dimension: VectorDimension,
    pub normalized: bool,
}

impl Vector {
    pub fn new(data: Vec<f32>) -> Result<Self, &'static str> {
        let dimension = VectorDimension::new(data.len() as u16)?;
        Ok(Self {
            id: VectorId::new(),
            data,
            dimension,
            normalized: false,
        })
    }
    
    /// L2 normalize the vector
    pub fn normalize(&mut self) {
        let norm: f32 = self.data.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            self.data.iter_mut().for_each(|x| *x /= norm);
            self.normalized = true;
        }
    }
    
    /// Compute cosine similarity with another vector
    pub fn cosine_similarity(&self, other: &Vector) -> Result<f32, &'static str> {
        if self.dimension != other.dimension {
            return Err("Vector dimensions must match");
        }
        
        let dot_product: f32 = self.data.iter()
            .zip(other.data.iter())
            .map(|(a, b)| a * b)
            .sum();
            
        if self.normalized && other.normalized {
            Ok(dot_product)
        } else {
            let norm_a: f32 = self.data.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = other.data.iter().map(|x| x * x).sum::<f32>().sqrt();
            Ok(dot_product / (norm_a * norm_b))
        }
    }
}

/// Event type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    // Network Events
    NetworkConnection,
    NetworkTraffic,
    DNSQuery,
    HTTPRequest,
    
    // Authentication Events
    Authentication,
    Authorization,
    PasswordChange,
    
    // File Events
    FileCreate,
    FileModify,
    FileDelete,
    FileAccess,
    
    // Process Events
    ProcessStart,
    ProcessStop,
    ProcessInject,
    
    // Threat Events
    MalwareDetected,
    AnomalyDetected,
    PolicyViolation,
    
    // Custom
    Custom(String),
}

/// Event metadata as flexible key-value pairs
pub type EventMetadata = HashMap<String, serde_json::Value>;

/// Core event structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: EventId,
    pub timestamp: Timestamp,
    pub event_type: EventType,
    pub source: EventSource,
    pub raw_data: Vec<u8>,
    pub metadata: EventMetadata,
    pub vector: Option<Vector>,
    pub merkle_prev: Option<String>,
    pub merkle_hash: Option<String>,
}

impl Event {
    pub fn new(event_type: EventType, source: EventSource, raw_data: Vec<u8>) -> Self {
        Self {
            id: EventId::new(),
            timestamp: Utc::now(),
            event_type,
            source,
            raw_data,
            metadata: HashMap::new(),
            vector: None,
            merkle_prev: None,
            merkle_hash: None,
        }
    }
    
    /// Add metadata field
    pub fn add_metadata<K, V>(&mut self, key: K, value: V) 
    where
        K: Into<String>,
        V: Into<serde_json::Value>,
    {
        self.metadata.insert(key.into(), value.into());
    }
    
    /// Get metadata field
    pub fn get_metadata(&self, key: &str) -> Option<&serde_json::Value> {
        self.metadata.get(key)
    }
}

/// Event source information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSource {
    pub host: String,
    pub ip: Option<std::net::IpAddr>,
    pub collector: String,
    pub format: IngestionFormat,
}

/// Supported ingestion formats
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestionFormat {
    OCSF,       // Open Cybersecurity Schema Framework
    STIX,       // Structured Threat Information Expression
    TAXII,      // Trusted Automated Exchange of Intelligence Information
    Zeek,       // Zeek (formerly Bro) logs
    Suricata,   // Suricata IDS logs
    PEHeader,   // Windows PE executable headers
    CEF,        // Common Event Format
    LEEF,       // Log Event Extended Format
    Raw,        // Raw unstructured logs
    Custom(String),
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

/// Time range for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TimeRange {
    pub fn new(start: Timestamp, end: Timestamp) -> Result<Self, &'static str> {
        if start > end {
            Err("Start time must be before end time")
        } else {
            Ok(Self { start, end })
        }
    }
    
    pub fn contains(&self, timestamp: &Timestamp) -> bool {
        timestamp >= &self.start && timestamp <= &self.end
    }
}