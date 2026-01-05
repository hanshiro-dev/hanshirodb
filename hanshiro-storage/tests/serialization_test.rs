//! Test different serialization formats for EventMetadata

use hanshiro_core::types::*;
use std::collections::HashMap;

#[test]
fn test_event_serialization_formats() {
    println!("\n=== Testing Serialization Formats ===\n");
    
    // Create event with metadata
    let mut event = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: "test-host".to_string(),
            ip: Some("127.0.0.1".parse().unwrap()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; 512],
    );
    
    // Add various metadata types
    event.add_metadata("string_value", "test");
    event.add_metadata("int_value", 42);
    event.add_metadata("float_value", 3.14);
    event.add_metadata("bool_value", true);
    event.add_metadata("array_value", vec![1, 2, 3]);
    
    // Test bincode
    println!("Testing bincode:");
    match bincode::serialize(&event) {
        Ok(data) => {
            println!("  ✓ Serialization successful, size: {} bytes", data.len());
            match bincode::deserialize::<Event>(&data) {
                Ok(_) => println!("  ✓ Deserialization successful"),
                Err(e) => println!("  ✗ Deserialization failed: {:?}", e),
            }
        }
        Err(e) => println!("  ✗ Serialization failed: {:?}", e),
    }
    
    // Test MessagePack
    println!("\nTesting MessagePack (rmp-serde):");
    match rmp_serde::to_vec(&event) {
        Ok(data) => {
            println!("  ✓ Serialization successful, size: {} bytes", data.len());
            match rmp_serde::from_slice::<Event>(&data) {
                Ok(_) => println!("  ✓ Deserialization successful"),
                Err(e) => println!("  ✗ Deserialization failed: {:?}", e),
            }
        }
        Err(e) => println!("  ✗ Serialization failed: {:?}", e),
    }
    
    // Test without metadata
    println!("\nTesting bincode without metadata:");
    let event_no_meta = Event::new(
        EventType::NetworkConnection,
        EventSource {
            host: "test-host".to_string(),
            ip: Some("127.0.0.1".parse().unwrap()),
            collector: "test".to_string(),
            format: IngestionFormat::Raw,
        },
        vec![b'x'; 512],
    );
    
    match bincode::serialize(&event_no_meta) {
        Ok(data) => {
            println!("  ✓ Serialization successful, size: {} bytes", data.len());
            match bincode::deserialize::<Event>(&data) {
                Ok(_) => println!("  ✓ Deserialization successful"),
                Err(e) => println!("  ✗ Deserialization failed: {:?}", e),
            }
        }
        Err(e) => println!("  ✗ Serialization failed: {:?}", e),
    }
}