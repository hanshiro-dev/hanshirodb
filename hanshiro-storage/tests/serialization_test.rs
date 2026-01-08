//! Test rkyv serialization for Event

use hanshiro_core::types::*;

#[test]
fn test_event_serialization() {
    println!("\n=== Testing rkyv Serialization ===\n");
    
    // Create event with metadata
    let mut event = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: "test-host".to_string(),
            ip: Some("127.0.0.1".to_string()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; 512],
    );
    
    // Add various metadata types
    event.add_metadata("string_value", serde_json::json!("test"));
    event.add_metadata("int_value", serde_json::json!(42));
    event.add_metadata("float_value", serde_json::json!(3.14));
    event.add_metadata("bool_value", serde_json::json!(true));
    event.add_metadata("array_value", serde_json::json!([1, 2, 3]));
    
    // Test rkyv serialization
    let data = hanshiro_core::serialize_event(&event).unwrap();
    println!("Serialized size: {} bytes", data.len());
    
    let recovered = hanshiro_core::deserialize_event(&data).unwrap();
    assert_eq!(recovered.id.hi, event.id.hi);
    assert_eq!(recovered.id.lo, event.id.lo);
    assert_eq!(recovered.timestamp_ms, event.timestamp_ms);
    assert_eq!(recovered.raw_data, event.raw_data);
    
    // Verify metadata roundtrip
    let meta = recovered.metadata();
    assert_eq!(meta.get("int_value"), Some(&serde_json::json!(42)));
    
    println!("✓ Serialization roundtrip successful");
}

#[test]
fn test_event_without_metadata() {
    let event = Event::new(
        EventType::FileCreate,
        EventSource {
            host: "test".to_string(),
            ip: None,
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![1, 2, 3],
    );
    
    let data = hanshiro_core::serialize_event(&event).unwrap();
    let recovered = hanshiro_core::deserialize_event(&data).unwrap();
    
    assert_eq!(recovered.raw_data, vec![1, 2, 3]);
    assert!(recovered.metadata().is_empty());
}
