//! # HanshiroDB Client
//!
//! High-level client for interacting with HanshiroDB storage engine.

use std::path::Path;
use std::sync::Arc;

use hanshiro_core::{
    error::Result,
    Event, EventId, EventType, EventSource, IngestionFormat,
    HanshiroValue, CodeArtifact, IndexEntry, Vector,
    value::FileType,
};
use hanshiro_storage::{
    StorageEngine,
    engine::StorageConfig,
    Correlation, CorrelationType,
};

/// High-level client for HanshiroDB operations
pub struct HanshiroClient {
    engine: Arc<StorageEngine>,
}

impl HanshiroClient {
    /// Open or create a database at the given path
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let config = StorageConfig {
            data_dir: path.as_ref().to_path_buf(),
            ..Default::default()
        };
        let engine = StorageEngine::new(config).await?;
        Ok(Self { engine: Arc::new(engine) })
    }

    /// Open with custom configuration
    pub async fn open_with_config(config: StorageConfig) -> Result<Self> {
        let engine = StorageEngine::new(config).await?;
        Ok(Self { engine: Arc::new(engine) })
    }

    // ========== Event/Log Operations ==========

    /// Ingest a security event
    pub async fn ingest_event(&self, event: Event) -> Result<EventId> {
        self.engine.write_value(HanshiroValue::Log(event)).await
    }

    /// Get event by ID
    pub async fn get_event(&self, id: EventId) -> Result<Option<Event>> {
        Ok(self.engine.read_value(id).await?.and_then(|v| v.as_log().cloned()))
    }

    /// Scan all logs
    pub async fn scan_logs(&self) -> Result<Vec<Event>> {
        self.engine.scan_logs().await
    }

    // ========== Code Artifact Operations ==========

    /// Store a code/binary analysis artifact
    pub async fn store_code(&self, artifact: CodeArtifact) -> Result<EventId> {
        self.engine.write_code(artifact).await
    }

    /// Get code artifact by ID
    pub async fn get_code(&self, id: EventId) -> Result<Option<CodeArtifact>> {
        self.engine.read_code(id).await
    }

    /// Scan all code artifacts
    pub async fn scan_code(&self) -> Result<Vec<CodeArtifact>> {
        self.engine.scan_code().await
    }

    // ========== Correlation Operations ==========

    /// Run auto-correlation between all logs and code artifacts
    pub async fn correlate_all(&self) -> Result<Vec<Correlation>> {
        self.engine.correlate_all().await
    }

    /// Find entities correlated to a given ID
    pub async fn find_correlated(&self, id: EventId) -> Result<Vec<EventId>> {
        self.engine.find_correlated(id).await
    }

    /// Find similar entities by vector
    pub async fn find_similar(&self, query: &[f32], top_k: usize) -> Result<Vec<(EventId, f32)>> {
        self.engine.find_similar(query, top_k).await
    }

    // ========== Utility ==========

    /// Force flush all data to disk
    pub async fn flush(&self) -> Result<()> {
        self.engine.force_flush().await
    }

    /// Get underlying engine for advanced operations
    pub fn engine(&self) -> &StorageEngine {
        &self.engine
    }
}

// ========== Builder Helpers ==========

/// Builder for creating security events
pub struct EventBuilder {
    event_type: EventType,
    source: EventSource,
    raw_data: Vec<u8>,
    metadata: serde_json::Map<String, serde_json::Value>,
    vector: Option<Vector>,
}

impl EventBuilder {
    pub fn new(event_type: EventType) -> Self {
        Self {
            event_type,
            source: EventSource {
                host: "unknown".to_string(),
                ip: None,
                collector: "hanshiro".to_string(),
                format: IngestionFormat::Raw,
            },
            raw_data: Vec::new(),
            metadata: serde_json::Map::new(),
            vector: None,
        }
    }

    pub fn source(mut self, host: &str, collector: &str, format: IngestionFormat) -> Self {
        self.source = EventSource {
            host: host.to_string(),
            ip: None,
            collector: collector.to_string(),
            format,
        };
        self
    }

    pub fn source_ip(mut self, ip: &str) -> Self {
        self.source.ip = Some(ip.to_string());
        self
    }

    pub fn raw_data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.raw_data = data.into();
        self
    }

    pub fn metadata(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.metadata.insert(key.to_string(), value.into());
        self
    }

    pub fn vector(mut self, data: Vec<f32>) -> Self {
        self.vector = Vector::new(data).ok();
        self
    }

    pub fn build(self) -> Event {
        let mut event = Event::new(self.event_type, self.source, self.raw_data);
        event.metadata_json = serde_json::to_string(&self.metadata).unwrap_or_default();
        event.vector = self.vector;
        event
    }
}

/// Builder for code artifacts
pub struct CodeArtifactBuilder {
    hash: [u8; 32],
    file_type: FileType,
    file_size: u64,
    imports: Vec<String>,
    strings: Vec<String>,
    vector: Option<Vector>,
}

impl CodeArtifactBuilder {
    pub fn new(sha256_hash: [u8; 32], file_type: FileType, file_size: u64) -> Self {
        Self {
            hash: sha256_hash,
            file_type,
            file_size,
            imports: Vec::new(),
            strings: Vec::new(),
            vector: None,
        }
    }

    pub fn imports(mut self, imports: Vec<String>) -> Self {
        self.imports = imports;
        self
    }

    pub fn strings(mut self, strings: Vec<String>) -> Self {
        self.strings = strings;
        self
    }

    pub fn vector(mut self, data: Vec<f32>) -> Self {
        self.vector = Vector::new(data).ok();
        self
    }

    pub fn build(self) -> CodeArtifact {
        let mut artifact = CodeArtifact::new(self.hash, self.file_type, self.file_size);
        artifact.imports = self.imports;
        artifact.strings = self.strings;
        artifact.vector = self.vector;
        artifact
    }
}
