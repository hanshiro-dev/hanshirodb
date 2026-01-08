//! Fast serialization using rkyv for zero-copy deserialization.

use crate::{Event, error::{Error, Result}};

/// Serialize an event to bytes using rkyv
#[inline]
pub fn serialize_event(event: &Event) -> Result<Vec<u8>> {
    rkyv::to_bytes::<_, 1024>(event)
        .map(|v| v.to_vec())
        .map_err(|e| Error::Internal {
            message: format!("Serialization failed: {}", e),
        })
}

/// Deserialize an event from bytes using rkyv (with validation)
#[inline]
pub fn deserialize_event(bytes: &[u8]) -> Result<Event> {
    let archived = rkyv::check_archived_root::<Event>(bytes)
        .map_err(|e| Error::Internal {
            message: format!("Validation failed: {}", e),
        })?;
    
    use rkyv::Deserialize;
    archived.deserialize(&mut rkyv::Infallible)
        .map_err(|_: std::convert::Infallible| Error::Internal {
            message: "Deserialization failed".to_string(),
        })
}

/// Zero-copy access to archived event (fastest read path)
#[inline]
pub fn archived_event(bytes: &[u8]) -> Result<&rkyv::Archived<Event>> {
    rkyv::check_archived_root::<Event>(bytes)
        .map_err(|e| Error::Internal {
            message: format!("Validation failed: {}", e),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventId, EventType, EventSource, IngestionFormat};

    fn create_test_event() -> Event {
        Event {
            id: EventId::new(),
            timestamp_ms: 1704067200000,
            event_type: EventType::NetworkConnection,
            source: EventSource {
                host: "test-host".to_string(),
                ip: None,
                collector: "test".to_string(),
                format: IngestionFormat::Raw,
            },
            raw_data: b"test data".to_vec(),
            metadata_json: "{}".to_string(),
            vector: None,
            merkle_prev: None,
            merkle_hash: None,
        }
    }

    #[test]
    fn test_roundtrip() {
        let event = create_test_event();
        let bytes = serialize_event(&event).unwrap();
        let recovered = deserialize_event(&bytes).unwrap();
        assert_eq!(event.id.hi, recovered.id.hi);
        assert_eq!(event.id.lo, recovered.id.lo);
        assert_eq!(event.timestamp_ms, recovered.timestamp_ms);
    }
    
    #[test]
    fn test_zero_copy_access() {
        let event = create_test_event();
        let bytes = serialize_event(&event).unwrap();
        let archived = archived_event(&bytes).unwrap();
        // Access fields without full deserialization
        assert_eq!(archived.timestamp_ms, event.timestamp_ms);
    }
}
