//! # HTTP Server Implementation
//!
//! REST API for HanshiroDB.

use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
    body::Bytes,
};
use serde::{Deserialize, Serialize};

use hanshiro_core::{Event, EventId, EventType, EventSource, IngestionFormat, Vector, VectorId, HanshiroValue};
use hanshiro_core::value::{CodeArtifact, FileType};
use hanshiro_storage::{StorageEngine, engine::StorageConfig};

// ========== API Types ==========

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiEventId {
    pub hi: u64,
    pub lo: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiEvent {
    #[serde(default)]
    pub id: Option<ApiEventId>,
    pub event_type: String,
    pub source_host: String,
    #[serde(default)]
    pub source_ip: Option<String>,
    pub collector: String,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub raw_data: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub vector: Vec<f32>,
}

fn default_format() -> String { "Raw".to_string() }

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiCodeArtifact {
    #[serde(default)]
    pub id: Option<ApiEventId>,
    pub hash_sha256: String, // hex
    pub file_type: String,
    pub file_size: u64,
    #[serde(default)]
    pub imports: Vec<String>,
    #[serde(default)]
    pub strings: Vec<String>,
    #[serde(default)]
    pub vector: Vec<f32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiCorrelation {
    pub source_id: ApiEventId,
    pub target_id: ApiEventId,
    pub correlation_type: String,
    pub score: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FindSimilarRequest {
    pub vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

fn default_top_k() -> usize { 10 }

#[derive(Debug, Serialize, Deserialize)]
pub struct SimilarResult {
    pub id: ApiEventId,
    pub score: f32,
}

// ========== Server State ==========

pub struct AppState {
    pub engine: Arc<StorageEngine>,
}

impl AppState {
    pub async fn new(data_dir: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let config = StorageConfig {
            data_dir,
            ..Default::default()
        };
        let engine = StorageEngine::new(config).await?;
        Ok(Self { engine: Arc::new(engine) })
    }
}

// ========== Routes ==========

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        // JSON endpoints
        .route("/events", post(ingest_event))
        .route("/events/:hi/:lo", get(get_event))
        .route("/events", get(scan_logs))
        .route("/code", post(store_code))
        .route("/code/:hi/:lo", get(get_code))
        .route("/similar", post(find_similar))
        .route("/correlate", post(correlate))
        // Binary endpoints (high-performance)
        .route("/bin/events", post(ingest_event_binary))
        .route("/bin/events/:hi/:lo", get(get_event_binary))
        .route("/bin/code", post(store_code_binary))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Json(event): Json<ApiEvent>,
) -> Result<Json<ApiEventId>, (StatusCode, String)> {
    let core_event = api_to_event(event).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    
    let id = state.engine
        .write_value(HanshiroValue::Log(core_event))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Json(ApiEventId { hi: id.hi, lo: id.lo }))
}

async fn get_event(
    State(state): State<Arc<AppState>>,
    Path((hi, lo)): Path<(u64, u64)>,
) -> Result<Json<Option<ApiEvent>>, (StatusCode, String)> {
    let id = EventId { hi, lo };
    
    let event = state.engine
        .read_value(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .and_then(|v| v.as_log().cloned())
        .map(event_to_api);
    
    Ok(Json(event))
}

async fn scan_logs(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ApiEvent>>, (StatusCode, String)> {
    let logs = state.engine
        .scan_logs()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Json(logs.into_iter().map(event_to_api).collect()))
}

async fn store_code(
    State(state): State<Arc<AppState>>,
    Json(artifact): Json<ApiCodeArtifact>,
) -> Result<Json<ApiEventId>, (StatusCode, String)> {
    let core_artifact = api_to_code_artifact(artifact).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    
    let id = state.engine
        .write_code(core_artifact)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Json(ApiEventId { hi: id.hi, lo: id.lo }))
}

async fn get_code(
    State(state): State<Arc<AppState>>,
    Path((hi, lo)): Path<(u64, u64)>,
) -> Result<Json<Option<ApiCodeArtifact>>, (StatusCode, String)> {
    let id = EventId { hi, lo };
    
    let artifact = state.engine
        .read_code(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map(code_artifact_to_api);
    
    Ok(Json(artifact))
}

async fn find_similar(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FindSimilarRequest>,
) -> Result<Json<Vec<SimilarResult>>, (StatusCode, String)> {
    let results = state.engine
        .find_similar(&req.vector, req.top_k)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Json(results.into_iter().map(|(id, score)| SimilarResult {
        id: ApiEventId { hi: id.hi, lo: id.lo },
        score,
    }).collect()))
}

async fn correlate(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ApiCorrelation>>, (StatusCode, String)> {
    let correlations = state.engine
        .correlate_all()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Json(correlations.into_iter().map(|c| ApiCorrelation {
        source_id: ApiEventId { hi: c.source_id.hi, lo: c.source_id.lo },
        target_id: ApiEventId { hi: c.target_id.hi, lo: c.target_id.lo },
        correlation_type: format!("{:?}", c.correlation_type),
        score: c.score,
    }).collect()))
}

// ========== Conversions ==========

fn api_to_event(e: ApiEvent) -> Result<Event, String> {
    let event_type = match e.event_type.as_str() {
        "ProcessStart" => EventType::ProcessStart,
        "NetworkConnection" => EventType::NetworkConnection,
        "Authentication" => EventType::Authentication,
        "MalwareDetected" => EventType::MalwareDetected,
        "FileCreate" => EventType::FileCreate,
        other => EventType::Custom(other.to_string()),
    };
    
    let format = match e.format.as_str() {
        "OCSF" => IngestionFormat::OCSF,
        "Zeek" => IngestionFormat::Zeek,
        "Suricata" => IngestionFormat::Suricata,
        _ => IngestionFormat::Raw,
    };
    
    let id = e.id.map(|i| EventId { hi: i.hi, lo: i.lo }).unwrap_or_else(EventId::new);
    
    let vector = if e.vector.is_empty() {
        None
    } else {
        Some(Vector {
            id: VectorId::new(),
            dimension: e.vector.len() as u16,
            data: e.vector,
            normalized: false,
        })
    };
    
    Ok(Event {
        id,
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type,
        source: EventSource {
            host: e.source_host,
            ip: e.source_ip,
            collector: e.collector,
            format,
        },
        raw_data: e.raw_data.into_bytes(),
        metadata_json: serde_json::to_string(&e.metadata).unwrap_or_default(),
        vector,
        merkle_prev: None,
        merkle_hash: None,
    })
}

fn event_to_api(e: Event) -> ApiEvent {
    ApiEvent {
        id: Some(ApiEventId { hi: e.id.hi, lo: e.id.lo }),
        event_type: format!("{:?}", e.event_type),
        source_host: e.source.host,
        source_ip: e.source.ip,
        collector: e.source.collector,
        format: format!("{:?}", e.source.format),
        raw_data: String::from_utf8_lossy(&e.raw_data).to_string(),
        metadata: serde_json::from_str(&e.metadata_json).unwrap_or_default(),
        vector: e.vector.map(|v| v.data).unwrap_or_default(),
    }
}

fn api_to_code_artifact(a: ApiCodeArtifact) -> Result<CodeArtifact, String> {
    let hash_bytes = hex::decode(&a.hash_sha256)
        .map_err(|_| "invalid hex hash")?;
    let hash: [u8; 32] = hash_bytes.try_into()
        .map_err(|_| "hash must be 32 bytes")?;
    
    let file_type = match a.file_type.as_str() {
        "PE" => FileType::PE,
        "ELF" => FileType::ELF,
        "Script" => FileType::Script,
        _ => FileType::Unknown,
    };
    
    let id = a.id.map(|i| EventId { hi: i.hi, lo: i.lo }).unwrap_or_else(EventId::new);
    
    let vector = if a.vector.is_empty() {
        None
    } else {
        Some(Vector {
            id: VectorId::new(),
            dimension: a.vector.len() as u16,
            data: a.vector,
            normalized: false,
        })
    };
    
    Ok(CodeArtifact {
        id,
        hash_sha256: hash,
        file_type,
        file_size: a.file_size,
        capabilities: Default::default(),
        imports: a.imports,
        strings: a.strings,
        vector,
        related_logs: Vec::new(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    })
}

fn code_artifact_to_api(a: CodeArtifact) -> ApiCodeArtifact {
    ApiCodeArtifact {
        id: Some(ApiEventId { hi: a.id.hi, lo: a.id.lo }),
        hash_sha256: hex::encode(a.hash_sha256),
        file_type: format!("{:?}", a.file_type),
        file_size: a.file_size,
        imports: a.imports,
        strings: a.strings,
        vector: a.vector.map(|v| v.data).unwrap_or_default(),
    }
}


// ========== Binary Endpoints (high-performance) ==========

async fn ingest_event_binary(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<Bytes, (StatusCode, String)> {
    let event = rkyv::from_bytes::<Event>(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid binary: {}", e)))?;
    
    let id = state.engine
        .write_value(HanshiroValue::Log(event))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    let id_bytes = rkyv::to_bytes::<_, 32>(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Bytes::from(id_bytes.to_vec()))
}

async fn get_event_binary(
    State(state): State<Arc<AppState>>,
    Path((hi, lo)): Path<(u64, u64)>,
) -> Result<Bytes, (StatusCode, String)> {
    let id = EventId { hi, lo };
    
    let event = state.engine
        .read_value(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .and_then(|v| v.as_log().cloned());
    
    match event {
        Some(e) => {
            let bytes = rkyv::to_bytes::<_, 4096>(&e)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            Ok(Bytes::from(bytes.to_vec()))
        }
        None => Err((StatusCode::NOT_FOUND, "Event not found".to_string())),
    }
}

async fn store_code_binary(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<Bytes, (StatusCode, String)> {
    let artifact = rkyv::from_bytes::<CodeArtifact>(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid rkyv: {}", e)))?;
    
    let id = state.engine
        .write_code(artifact)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    let id_bytes = rkyv::to_bytes::<_, 32>(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Bytes::from(id_bytes.to_vec()))
}
