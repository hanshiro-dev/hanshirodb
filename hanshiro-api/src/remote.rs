//! # Remote Client
//!
//! Connect to HanshiroDB server via HTTP (JSON or rkyv binary).

use hanshiro_core::{EventId, Event, error::{Error, Result}};
use hanshiro_core::value::CodeArtifact;

use crate::server::{ApiEvent, ApiEventId, ApiCodeArtifact, ApiCorrelation, FindSimilarRequest, SimilarResult};

/// Remote client for connecting to HanshiroDB server
pub struct RemoteClient {
    base_url: String,
    client: reqwest::Client,
}

impl RemoteClient {
    /// Connect to a HanshiroDB server
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let base_url = endpoint.into().trim_end_matches('/').to_string();
        let client = reqwest::Client::new();
        
        client.get(format!("{}/health", base_url))
            .send()
            .await
            .map_err(|e| Error::Internal { message: format!("Connection failed: {}", e) })?;
        
        Ok(Self { base_url, client })
    }

    // ========== JSON API ==========

    /// Ingest event (JSON)
    pub async fn ingest_event(&self, event: ApiEvent) -> Result<EventId> {
        let resp: ApiEventId = self.client
            .post(format!("{}/events", self.base_url))
            .json(&event)
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .json()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?;
        
        Ok(EventId { hi: resp.hi, lo: resp.lo })
    }

    /// Store code artifact (JSON)
    pub async fn store_code(&self, artifact: ApiCodeArtifact) -> Result<EventId> {
        let resp: ApiEventId = self.client
            .post(format!("{}/code", self.base_url))
            .json(&artifact)
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .json()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?;
        
        Ok(EventId { hi: resp.hi, lo: resp.lo })
    }

    /// Get event by ID (JSON)
    pub async fn get_event(&self, id: EventId) -> Result<Option<ApiEvent>> {
        self.client
            .get(format!("{}/events/{}/{}", self.base_url, id.hi, id.lo))
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .json()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })
    }

    /// Scan all logs (JSON)
    pub async fn scan_logs(&self) -> Result<Vec<ApiEvent>> {
        self.client
            .get(format!("{}/events", self.base_url))
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .json()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })
    }

    /// Find similar by vector
    pub async fn find_similar(&self, vector: &[f32], top_k: usize) -> Result<Vec<SimilarResult>> {
        let req = FindSimilarRequest { vector: vector.to_vec(), top_k };
        
        self.client
            .post(format!("{}/similar", self.base_url))
            .json(&req)
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .json()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })
    }

    /// Run correlation
    pub async fn correlate_all(&self) -> Result<Vec<ApiCorrelation>> {
        self.client
            .post(format!("{}/correlate", self.base_url))
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .json()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })
    }

    // ========== Binary API (high-performance) ==========

    /// Ingest event using binary format (faster)
    pub async fn ingest_event_binary(&self, event: &Event) -> Result<EventId> {
        let bytes = rkyv::to_bytes::<_, 4096>(event)
            .map_err(|e| Error::Internal { message: format!("Serialize failed: {}", e) })?;
        
        let resp = self.client
            .post(format!("{}/bin/events", self.base_url))
            .header("Content-Type", "application/octet-stream")
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .bytes()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?;
        
        rkyv::from_bytes::<EventId>(&resp)
            .map_err(|e| Error::Internal { message: format!("Deserialize failed: {}", e) })
    }

    /// Get event using binary format (faster)
    pub async fn get_event_binary(&self, id: EventId) -> Result<Option<Event>> {
        let resp = self.client
            .get(format!("{}/bin/events/{}/{}", self.base_url, id.hi, id.lo))
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?;
        
        if resp.status() == 404 {
            return Ok(None);
        }
        
        let bytes = resp.bytes()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?;
        
        let event = rkyv::from_bytes::<Event>(&bytes)
            .map_err(|e| Error::Internal { message: format!("Deserialize failed: {}", e) })?;
        
        Ok(Some(event))
    }

    /// Store code artifact using binary format (faster)
    pub async fn store_code_binary(&self, artifact: &CodeArtifact) -> Result<EventId> {
        let bytes = rkyv::to_bytes::<_, 4096>(artifact)
            .map_err(|e| Error::Internal { message: format!("Serialize failed: {}", e) })?;
        
        let resp = self.client
            .post(format!("{}/bin/code", self.base_url))
            .header("Content-Type", "application/octet-stream")
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?
            .bytes()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })?;
        
        rkyv::from_bytes::<EventId>(&resp)
            .map_err(|e| Error::Internal { message: format!("Deserialize failed: {}", e) })
    }
}
