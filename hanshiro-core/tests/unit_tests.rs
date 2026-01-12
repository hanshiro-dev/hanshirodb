//! Unit tests for hanshiro-core

use hanshiro_core::{
    Event, EventId, EventType, EventSource, IngestionFormat, Vector,
    HanshiroValue, KeyPrefix, StorageKey, CodeArtifact, FileType, Capabilities, IndexEntry,
    serialization::{serialize_event, deserialize_event, serialize_value, deserialize_value},
};

mod value_tests {
    use super::*;

    #[test]
    fn test_key_prefix_roundtrip() {
        for prefix in [KeyPrefix::Log, KeyPrefix::Code, KeyPrefix::Index] {
            let byte = prefix.as_byte();
            let recovered = KeyPrefix::from_byte(byte).unwrap();
            assert_eq!(prefix, recovered);
        }
    }

    #[test]
    fn test_storage_key_bytes() {
        let id = EventId { hi: 123, lo: 456 };
        let key = StorageKey {
            prefix: KeyPrefix::Log,
            timestamp_ns: 1000000,
            id,
        };
        let bytes = key.to_bytes();
        let recovered = StorageKey::from_bytes(&bytes).unwrap();
        assert_eq!(key, recovered);
    }

    #[test]
    fn test_key_ordering() {
        let log_key = StorageKey::new_log(EventId::new());
        let code_key = StorageKey::new_code(EventId::new());
        assert!(log_key < code_key);
    }

    #[test]
    fn test_capabilities() {
        let mut caps = Capabilities::default();
        assert!(!caps.has(Capabilities::NETWORK));
        caps.set(Capabilities::NETWORK);
        caps.set(Capabilities::CRYPTO);
        assert!(caps.has(Capabilities::NETWORK));
        assert!(caps.has(Capabilities::CRYPTO));
        assert!(!caps.has(Capabilities::KEYLOGGER));
    }

    #[test]
    fn test_code_artifact_hash_hex() {
        let hash = [0xab; 32];
        let artifact = CodeArtifact::new(hash, FileType::PE, 1024);
        let hex = artifact.hash_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_index_entry_add_ref() {
        let mut idx = IndexEntry::new("hash", "abc123");
        let id1 = EventId::new();
        let id2 = EventId::new();
        
        idx.add_ref(id1);
        idx.add_ref(id2);
        idx.add_ref(id1); // Duplicate
        
        assert_eq!(idx.refs.len(), 2);
    }
}

mod serialization_tests {
    use super::*;

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
    fn test_event_roundtrip() {
        let event = create_test_event();
        let bytes = serialize_event(&event).unwrap();
        let recovered = deserialize_event(&bytes).unwrap();
        assert_eq!(event.id.hi, recovered.id.hi);
        assert_eq!(event.id.lo, recovered.id.lo);
        assert_eq!(event.timestamp_ms, recovered.timestamp_ms);
    }

    #[test]
    fn test_value_log_roundtrip() {
        let event = create_test_event();
        let value = HanshiroValue::Log(event.clone());
        let bytes = serialize_value(&value).unwrap();
        let recovered = deserialize_value(&bytes).unwrap();
        let log = recovered.as_log().unwrap();
        assert_eq!(log.id.hi, event.id.hi);
    }

    #[test]
    fn test_value_code_roundtrip() {
        let code = CodeArtifact::new([0u8; 32], FileType::PE, 1024);
        let value = HanshiroValue::Code(code);
        let bytes = serialize_value(&value).unwrap();
        let recovered = deserialize_value(&bytes).unwrap();
        assert!(recovered.as_code().is_some());
    }

    #[test]
    fn test_value_index_roundtrip() {
        let mut idx = IndexEntry::new("hash", "abc123");
        idx.add_ref(EventId::new());
        let value = HanshiroValue::Index(idx);
        let bytes = serialize_value(&value).unwrap();
        let recovered = deserialize_value(&bytes).unwrap();
        let index = recovered.as_index().unwrap();
        assert_eq!(index.field, "hash");
        assert_eq!(index.refs.len(), 1);
    }

    #[test]
    fn test_event_with_vector() {
        let mut event = create_test_event();
        event.vector = Some(Vector::new(vec![0.1, 0.2, 0.3, 0.4]).unwrap());
        
        let bytes = serialize_event(&event).unwrap();
        let recovered = deserialize_event(&bytes).unwrap();
        
        assert!(recovered.vector.is_some());
        assert_eq!(recovered.vector.unwrap().data.len(), 4);
    }
}

mod utils_tests {
    use hanshiro_core::utils::{next_power_of_two, is_power_of_two, align_to, format_bytes};

    #[test]
    fn test_power_of_two() {
        assert_eq!(next_power_of_two(0), 1);
        assert_eq!(next_power_of_two(1), 1);
        assert_eq!(next_power_of_two(2), 2);
        assert_eq!(next_power_of_two(3), 4);
        assert_eq!(next_power_of_two(5), 8);

        assert!(is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(is_power_of_two(4));
        assert!(!is_power_of_two(3));
        assert!(!is_power_of_two(0));
    }

    #[test]
    fn test_align() {
        assert_eq!(align_to(0, 8), 0);
        assert_eq!(align_to(1, 8), 8);
        assert_eq!(align_to(7, 8), 8);
        assert_eq!(align_to(8, 8), 8);
        assert_eq!(align_to(9, 8), 16);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }
}

mod crypto_tests {
    use hanshiro_core::crypto::MerkleChain;

    #[test]
    fn test_merkle_chain() {
        let mut chain = MerkleChain::new();
        
        let node1 = chain.add(b"data1");
        let node2 = chain.add(b"data2");
        let node3 = chain.add(b"data3");
        
        // Each hash should be different
        assert_ne!(node1.hash, node2.hash);
        assert_ne!(node2.hash, node3.hash);
        
        // Chain should link properly
        assert!(node1.prev_hash.is_none());
        assert_eq!(node2.prev_hash, Some(node1.hash.clone()));
        assert_eq!(node3.prev_hash, Some(node2.hash.clone()));
    }
}
