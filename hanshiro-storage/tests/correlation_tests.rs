//! Tests for the correlation engine

use hanshiro_core::{
    Event, EventId, EventType, EventSource, IngestionFormat, Vector,
    CodeArtifact, FileType,
};
use hanshiro_storage::correlation::{
    CorrelationEngine, CorrelationConfig, CorrelationType,
};

fn make_event(raw: &str) -> Event {
    Event {
        id: EventId::new(),
        timestamp_ms: 0,
        event_type: EventType::ProcessStart,
        source: EventSource {
            host: "test".into(),
            ip: None,
            collector: "test".into(),
            format: IngestionFormat::Raw,
        },
        raw_data: raw.as_bytes().to_vec(),
        metadata_json: "{}".into(),
        vector: None,
        merkle_prev: None,
        merkle_hash: None,
    }
}

fn make_event_with_vector(raw: &str, vec: Vec<f32>) -> Event {
    let mut event = make_event(raw);
    event.vector = Some(Vector::new(vec).unwrap());
    event
}

fn make_artifact(hash: [u8; 32], strings: Vec<String>) -> CodeArtifact {
    let mut a = CodeArtifact::new(hash, FileType::PE, 1024);
    a.strings = strings;
    a
}

fn make_artifact_with_vector(hash: [u8; 32], vec: Vec<f32>) -> CodeArtifact {
    let mut a = CodeArtifact::new(hash, FileType::PE, 1024);
    a.vector = Some(Vector::new(vec).unwrap());
    a
}

#[test]
fn test_hash_correlation() {
    let engine = CorrelationEngine::new(CorrelationConfig::default());

    let hash = [0xab; 32];
    let artifact = make_artifact(hash, vec![]);
    let hash_hex = artifact.hash_hex();

    // Log that mentions the hash
    let log = make_event(&format!("Process started with hash {}", hash_hex));

    let correlations = engine.correlate_log_to_code(&log, &[artifact]);
    assert_eq!(correlations.len(), 1);
    assert_eq!(correlations[0].correlation_type, CorrelationType::HashMatch);
    assert_eq!(correlations[0].score, 1.0);
}

#[test]
fn test_partial_hash_correlation() {
    let engine = CorrelationEngine::new(CorrelationConfig::default());

    let hash = [0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
                0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb];
    let artifact = make_artifact(hash, vec![]);
    let hash_hex = artifact.hash_hex();
    let hash_short = &hash_hex[..16]; // First 16 chars

    // Log that mentions partial hash
    let log = make_event(&format!("Detected malware: {}", hash_short));

    let correlations = engine.correlate_log_to_code(&log, &[artifact]);
    assert_eq!(correlations.len(), 1);
    assert_eq!(correlations[0].correlation_type, CorrelationType::HashMatch);
}

#[test]
fn test_filename_correlation() {
    let engine = CorrelationEngine::new(CorrelationConfig::default());

    let artifact = make_artifact([0; 32], vec!["malware.exe".into(), "config.dat".into()]);
    let log = make_event("Suspicious process malware.exe detected");

    let correlations = engine.correlate_log_to_code(&log, &[artifact]);
    assert_eq!(correlations.len(), 1);
    assert_eq!(correlations[0].correlation_type, CorrelationType::FilenameMatch);
}

#[test]
fn test_vector_similarity_correlation() {
    let config = CorrelationConfig {
        vector_similarity_threshold: 0.9,
        ..Default::default()
    };
    let engine = CorrelationEngine::new(config);

    // Very similar vectors
    let log = make_event_with_vector("test", vec![1.0, 0.0, 0.0, 0.0]);
    let artifact = make_artifact_with_vector([0; 32], vec![0.99, 0.01, 0.0, 0.0]);

    let correlations = engine.correlate_log_to_code(&log, &[artifact]);
    assert_eq!(correlations.len(), 1);
    assert_eq!(correlations[0].correlation_type, CorrelationType::VectorSimilarity);
    assert!(correlations[0].score > 0.9);
}

#[test]
fn test_no_correlation_for_dissimilar_vectors() {
    let config = CorrelationConfig {
        vector_similarity_threshold: 0.9,
        hash_matching: false,
        filename_matching: false,
        max_correlations_per_entity: 100,
    };
    let engine = CorrelationEngine::new(config);

    // Orthogonal vectors
    let log = make_event_with_vector("test", vec![1.0, 0.0, 0.0, 0.0]);
    let artifact = make_artifact_with_vector([0; 32], vec![0.0, 1.0, 0.0, 0.0]);

    let correlations = engine.correlate_log_to_code(&log, &[artifact]);
    assert!(correlations.is_empty());
}

#[test]
fn test_multiple_correlations() {
    let engine = CorrelationEngine::new(CorrelationConfig::default());

    let hash = [0xab; 32];
    let artifact = make_artifact(hash, vec!["evil.exe".into()]);
    let hash_hex = artifact.hash_hex();

    // Log that mentions both hash and filename
    let log = make_event(&format!("Process evil.exe started with hash {}", hash_hex));

    let correlations = engine.correlate_log_to_code(&log, &[artifact]);
    // Should have both hash and filename correlations, but deduplicated by (source, target)
    assert!(correlations.len() >= 1);
}

#[test]
fn test_correlate_code_to_logs() {
    let engine = CorrelationEngine::new(CorrelationConfig::default());

    let hash = [0xcd; 32];
    let artifact = make_artifact(hash, vec![]);
    let hash_hex = artifact.hash_hex();

    let logs = vec![
        make_event("Normal log entry"),
        make_event(&format!("Malware detected: {}", hash_hex)),
        make_event("Another normal log"),
    ];

    let correlations = engine.correlate_code_to_logs(&artifact, &logs);
    assert_eq!(correlations.len(), 1);
    // Direction should be code -> log
    assert_eq!(correlations[0].source_id, artifact.id);
}

#[test]
fn test_find_similar_by_vector() {
    let engine = CorrelationEngine::new(CorrelationConfig {
        vector_similarity_threshold: 0.8,
        ..Default::default()
    });

    let query = vec![1.0, 0.0, 0.0];
    let candidates = vec![
        (EventId::new(), vec![0.9, 0.1, 0.0]),  // Similar
        (EventId::new(), vec![0.0, 1.0, 0.0]),  // Orthogonal
        (EventId::new(), vec![0.95, 0.05, 0.0]), // Very similar
    ];

    let results = engine.find_similar_by_vector(&query, &candidates, 10);
    
    // Should find 2 similar vectors (above 0.8 threshold)
    assert_eq!(results.len(), 2);
    // Should be sorted by score descending
    assert!(results[0].1 >= results[1].1);
}

#[test]
fn test_build_index_entries() {
    use hanshiro_storage::correlation::Correlation;
    
    let engine = CorrelationEngine::new(CorrelationConfig::default());
    
    let target_id = EventId::new();
    let source1 = EventId::new();
    let source2 = EventId::new();
    
    let correlations = vec![
        Correlation {
            source_id: source1,
            target_id,
            correlation_type: CorrelationType::HashMatch,
            score: 1.0,
        },
        Correlation {
            source_id: source2,
            target_id,
            correlation_type: CorrelationType::FilenameMatch,
            score: 0.9,
        },
    ];
    
    let indexes = engine.build_index_entries(&correlations);
    assert_eq!(indexes.len(), 1); // Grouped by target
    assert_eq!(indexes[0].refs.len(), 2);
}

#[test]
fn test_correlation_config_defaults() {
    let config = CorrelationConfig::default();
    assert_eq!(config.vector_similarity_threshold, 0.85);
    assert!(config.hash_matching);
    assert!(config.filename_matching);
    assert_eq!(config.max_correlations_per_entity, 100);
}
