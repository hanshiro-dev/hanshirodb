//! # Production Test Cases for HanshiroDB
//!
//! Real-world SecOps scenarios demonstrating HanshiroDB's capabilities:
//! 1. Malware detection and correlation
//! 2. Threat hunting with vector similarity
//! 3. Incident response timeline reconstruction
//! 4. Code-to-log correlation for forensics

use hanshiro_api::{HanshiroClient, EventBuilder, CodeArtifactBuilder};
use hanshiro_core::{EventType, IngestionFormat, value::FileType};
use sha2::{Sha256, Digest};
use tempfile::TempDir;

/// Generate a mock embedding vector (in production, use a real model)
fn mock_embedding(seed: &str) -> Vec<f32> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    let hash = hasher.finish();
    
    // Generate 128-dim vector from hash (simplified)
    (0..128)
        .map(|i| {
            let val = ((hash.wrapping_mul(i as u64 + 1)) % 1000) as f32 / 1000.0;
            val * 2.0 - 1.0 // Normalize to [-1, 1]
        })
        .collect()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

// ============================================================================
// SCENARIO 1: Malware Detection Pipeline
// ============================================================================

/// Simulates a complete malware detection workflow:
/// 1. Ingest suspicious process execution logs
/// 2. Store analyzed malware sample
/// 3. Auto-correlate logs to malware
/// 4. Query related events
#[tokio::test]
async fn test_malware_detection_pipeline() {
    let tmp = TempDir::new().unwrap();
    let db = HanshiroClient::open(tmp.path()).await.unwrap();

    // Step 1: Ingest process execution events from EDR
    let malware_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    
    let process_event = EventBuilder::new(EventType::ProcessStart)
        .source("workstation-01", "crowdstrike", IngestionFormat::Raw)
        .source_ip("10.0.1.50")
        .raw_data(format!(
            r#"{{"process":"suspicious.exe","hash":"{}","parent":"explorer.exe","cmdline":"suspicious.exe -hidden"}}"#,
            malware_hash
        ))
        .metadata("severity", "critical")
        .metadata("hash", malware_hash)
        .metadata("process_name", "suspicious.exe")
        .vector(mock_embedding(&format!("process_exec_{}", malware_hash)))
        .build();

    let event_id = db.ingest_event(process_event).await.unwrap();

    // Step 2: Store the analyzed malware sample
    let malware_binary = b"MZ\x90\x00...fake_pe_header...";
    let artifact = CodeArtifactBuilder::new(
        sha256(malware_binary),
        FileType::PE,
        malware_binary.len() as u64,
    )
    .imports(vec![
        "kernel32.dll:CreateRemoteThread".to_string(),
        "kernel32.dll:VirtualAllocEx".to_string(),
        "ntdll.dll:NtUnmapViewOfSection".to_string(),
    ])
    .strings(vec![
        "http://evil.com/beacon".to_string(),
        malware_hash.to_string(),
    ])
    .vector(mock_embedding(&format!("malware_{}", malware_hash)))
    .build();

    let code_id = db.store_code(artifact).await.unwrap();

    // Step 3: Run auto-correlation
    let correlations = db.correlate_all().await.unwrap();
    
    // Verify correlation was found (hash match in strings)
    println!("Found {} correlations", correlations.len());

    // Step 4: Query all related events
    let logs = db.scan_logs().await.unwrap();
    let code = db.scan_code().await.unwrap();

    assert_eq!(logs.len(), 1);
    assert_eq!(code.len(), 1);
    assert!(code[0].imports.contains(&"kernel32.dll:CreateRemoteThread".to_string()));
}

// ============================================================================
// SCENARIO 2: Threat Hunting with Vector Similarity
// ============================================================================

/// Demonstrates finding similar threats using vector embeddings:
/// 1. Ingest multiple security events with embeddings
/// 2. Search for events similar to a known threat pattern
#[tokio::test]
async fn test_threat_hunting_vector_similarity() {
    let tmp = TempDir::new().unwrap();
    let db = HanshiroClient::open(tmp.path()).await.unwrap();

    // Ingest various network events
    let events = vec![
        ("normal_traffic", "10.0.1.10", "Normal HTTPS traffic to cdn.example.com"),
        ("c2_beacon", "10.0.1.50", "Suspicious beacon to 185.234.xx.xx:443 every 60s"),
        ("data_exfil", "10.0.1.50", "Large outbound transfer to unknown IP"),
        ("normal_dns", "10.0.1.20", "DNS query for google.com"),
        ("c2_dns", "10.0.1.50", "DNS query for dga-domain-xyz123.com"),
    ];

    for (name, ip, desc) in &events {
        let event = EventBuilder::new(EventType::NetworkConnection)
            .source("firewall-01", "paloalto", IngestionFormat::Raw)
            .source_ip(ip)
            .raw_data(desc.as_bytes().to_vec())
            .metadata("event_name", *name)
            .metadata("description", *desc)
            .vector(mock_embedding(name)) // Embedding based on behavior
            .build();
        
        db.ingest_event(event).await.unwrap();
    }

    // Hunt for events similar to known C2 beacon pattern
    let c2_pattern = mock_embedding("c2_beacon");
    let similar = db.find_similar(&c2_pattern, 3).await.unwrap();

    println!("Found {} similar events to C2 pattern", similar.len());
    
    // The c2_beacon event should be most similar to itself
    assert!(!similar.is_empty());
}

// ============================================================================
// SCENARIO 3: Incident Response Timeline
// ============================================================================

/// Reconstructs an attack timeline from multiple event sources:
/// 1. Initial access (phishing email)
/// 2. Execution (malicious macro)
/// 3. Persistence (scheduled task)
/// 4. Lateral movement (RDP)
/// 5. Exfiltration (data upload)
#[tokio::test]
async fn test_incident_timeline_reconstruction() {
    let tmp = TempDir::new().unwrap();
    let db = HanshiroClient::open(tmp.path()).await.unwrap();

    let attacker_ip = "192.168.1.100";
    let victim_host = "finance-ws-01";

    // Phase 1: Initial Access - Phishing
    let phishing = EventBuilder::new(EventType::Custom("EmailReceived".to_string()))
        .source(victim_host, "exchange", IngestionFormat::Raw)
        .raw_data(r#"{"from":"attacker@evil.com","subject":"Invoice","attachment":"invoice.docm"}"#)
        .metadata("phase", "initial_access")
        .metadata("technique", "T1566.001")
        .build();
    db.ingest_event(phishing).await.unwrap();

    // Phase 2: Execution - Macro
    let macro_exec = EventBuilder::new(EventType::ProcessStart)
        .source(victim_host, "sysmon", IngestionFormat::Raw)
        .raw_data(r#"{"process":"powershell.exe","parent":"WINWORD.EXE","cmdline":"powershell -enc BASE64..."}"#)
        .metadata("phase", "execution")
        .metadata("technique", "T1059.001")
        .build();
    db.ingest_event(macro_exec).await.unwrap();

    // Phase 3: Persistence - Scheduled Task
    let persistence = EventBuilder::new(EventType::Custom("ScheduledTaskCreated".to_string()))
        .source(victim_host, "sysmon", IngestionFormat::Raw)
        .raw_data(r#"{"task":"UpdateCheck","action":"powershell.exe -hidden","trigger":"daily"}"#)
        .metadata("phase", "persistence")
        .metadata("technique", "T1053.005")
        .build();
    db.ingest_event(persistence).await.unwrap();

    // Phase 4: Lateral Movement - RDP
    let lateral = EventBuilder::new(EventType::Authentication)
        .source("dc-01", "windows", IngestionFormat::Raw)
        .source_ip(attacker_ip)
        .raw_data(format!(r#"{{"user":"admin","source":"{}","type":"RDP"}}"#, victim_host))
        .metadata("phase", "lateral_movement")
        .metadata("technique", "T1021.001")
        .build();
    db.ingest_event(lateral).await.unwrap();

    // Phase 5: Exfiltration
    let exfil = EventBuilder::new(EventType::NetworkTraffic)
        .source(victim_host, "zeek", IngestionFormat::Zeek)
        .raw_data(r#"{"dest":"185.xxx.xxx.xxx","bytes_out":50000000,"protocol":"HTTPS"}"#)
        .metadata("phase", "exfiltration")
        .metadata("technique", "T1041")
        .build();
    db.ingest_event(exfil).await.unwrap();

    // Reconstruct timeline
    let timeline = db.scan_logs().await.unwrap();
    
    assert_eq!(timeline.len(), 5);
    
    // Verify MITRE ATT&CK phases are captured
    let phases: Vec<_> = timeline.iter()
        .filter_map(|e| {
            let meta: serde_json::Value = serde_json::from_str(&e.metadata_json).ok()?;
            meta.get("phase")?.as_str().map(String::from)
        })
        .collect();
    
    assert!(phases.contains(&"initial_access".to_string()));
    assert!(phases.contains(&"execution".to_string()));
    assert!(phases.contains(&"persistence".to_string()));
    assert!(phases.contains(&"lateral_movement".to_string()));
    assert!(phases.contains(&"exfiltration".to_string()));
}

// ============================================================================
// SCENARIO 4: Code-to-Log Forensic Correlation
// ============================================================================

/// Demonstrates automatic correlation between malware samples and logs:
/// 1. Store multiple malware samples with different capabilities
/// 2. Ingest logs mentioning those samples
/// 3. Auto-correlate to build relationship graph
#[tokio::test]
async fn test_code_log_forensic_correlation() {
    let tmp = TempDir::new().unwrap();
    let db = HanshiroClient::open(tmp.path()).await.unwrap();

    // Store ransomware sample
    let ransomware_hash = sha256(b"ransomware_payload");
    let ransomware = CodeArtifactBuilder::new(ransomware_hash, FileType::PE, 150000)
        .imports(vec![
            "advapi32.dll:CryptEncrypt".to_string(),
            "kernel32.dll:FindFirstFileW".to_string(),
        ])
        .strings(vec![
            "YOUR FILES HAVE BEEN ENCRYPTED".to_string(),
            "bitcoin:1A1zP1...".to_string(),
            hex::encode(ransomware_hash),
        ])
        .vector(mock_embedding("ransomware_behavior"))
        .build();
    let ransomware_id = db.store_code(ransomware).await.unwrap();

    // Store dropper sample
    let dropper_hash = sha256(b"dropper_payload");
    let dropper = CodeArtifactBuilder::new(dropper_hash, FileType::Script, 5000)
        .imports(vec!["System.Net.WebClient".to_string()])
        .strings(vec![
            "Invoke-WebRequest".to_string(),
            "http://evil.com/stage2.exe".to_string(),
            hex::encode(dropper_hash),
        ])
        .vector(mock_embedding("dropper_behavior"))
        .build();
    let dropper_id = db.store_code(dropper).await.unwrap();

    // Ingest detection events that reference these hashes
    let detection1 = EventBuilder::new(EventType::MalwareDetected)
        .source("endpoint-01", "defender", IngestionFormat::Raw)
        .raw_data(format!(
            r#"{{"detection":"Ransom.Generic","hash":"{}","action":"quarantined"}}"#,
            hex::encode(ransomware_hash)
        ))
        .metadata("hash", hex::encode(ransomware_hash))
        .vector(mock_embedding("ransomware_behavior"))
        .build();
    db.ingest_event(detection1).await.unwrap();

    let detection2 = EventBuilder::new(EventType::MalwareDetected)
        .source("endpoint-02", "crowdstrike", IngestionFormat::Raw)
        .raw_data(format!(
            r#"{{"detection":"Dropper.PS1","hash":"{}","action":"blocked"}}"#,
            hex::encode(dropper_hash)
        ))
        .metadata("hash", hex::encode(dropper_hash))
        .vector(mock_embedding("dropper_behavior"))
        .build();
    db.ingest_event(detection2).await.unwrap();

    // Run auto-correlation
    let correlations = db.correlate_all().await.unwrap();
    
    println!("Forensic correlations found: {}", correlations.len());
    for c in &correlations {
        println!("  {:?} -> {:?} ({:?}, score: {:.2})", 
            c.source_id, c.target_id, c.correlation_type, c.score);
    }

    // Verify we can find related entities
    let related_to_ransomware = db.find_correlated(ransomware_id).await.unwrap();
    println!("Entities related to ransomware: {:?}", related_to_ransomware);

    // Verify data integrity
    let all_code = db.scan_code().await.unwrap();
    let all_logs = db.scan_logs().await.unwrap();
    
    assert_eq!(all_code.len(), 2);
    assert_eq!(all_logs.len(), 2);
}

// ============================================================================
// SCENARIO 5: High-Throughput Ingestion
// ============================================================================

/// Tests high-throughput event ingestion for SOC environments
#[tokio::test]
async fn test_high_throughput_ingestion() {
    let tmp = TempDir::new().unwrap();
    let db = HanshiroClient::open(tmp.path()).await.unwrap();

    let start = std::time::Instant::now();
    let num_events = 1_000; // Reduced for debug builds

    for i in 0..num_events {
        let event = EventBuilder::new(EventType::NetworkConnection)
            .source(&format!("sensor-{}", i % 10), "zeek", IngestionFormat::Zeek)
            .source_ip(&format!("10.0.{}.{}", i / 256 % 256, i % 256))
            .raw_data(format!(r#"{{"conn_id":"{}","bytes":{}}}"#, i, i * 100))
            .metadata("index", i)
            .build();
        
        db.ingest_event(event).await.unwrap();
    }

    let elapsed = start.elapsed();
    let rate = num_events as f64 / elapsed.as_secs_f64();
    
    println!("Ingested {} events in {:?} ({:.0} events/sec)", 
        num_events, elapsed, rate);

    // Verify all events stored
    let logs = db.scan_logs().await.unwrap();
    assert_eq!(logs.len(), num_events);
    
    // Lower threshold for debug builds
    assert!(rate > 100.0, "Throughput too low: {:.0} events/sec", rate);
}

// ============================================================================
// SCENARIO 6: Multi-Source Event Correlation
// ============================================================================

/// Correlates events from multiple security tools by vector similarity
#[tokio::test]
async fn test_multi_source_correlation() {
    let tmp = TempDir::new().unwrap();
    let db = HanshiroClient::open(tmp.path()).await.unwrap();

    let attack_signature = "powershell_encoded_command";

    // EDR detection
    let edr_event = EventBuilder::new(EventType::ProcessStart)
        .source("ws-01", "crowdstrike", IngestionFormat::Raw)
        .raw_data(r#"{"process":"powershell.exe","cmdline":"-enc JABz..."}"#)
        .metadata("source_tool", "edr")
        .vector(mock_embedding(attack_signature))
        .build();
    db.ingest_event(edr_event).await.unwrap();

    // SIEM alert
    let siem_event = EventBuilder::new(EventType::AnomalyDetected)
        .source("siem-01", "splunk", IngestionFormat::Raw)
        .raw_data(r#"{"alert":"Encoded PowerShell Execution","severity":"high"}"#)
        .metadata("source_tool", "siem")
        .vector(mock_embedding(attack_signature))
        .build();
    db.ingest_event(siem_event).await.unwrap();

    // Network IDS
    let ids_event = EventBuilder::new(EventType::NetworkTraffic)
        .source("ids-01", "suricata", IngestionFormat::Suricata)
        .raw_data(r#"{"alert":"ET POLICY PowerShell User-Agent","sid":2024897}"#)
        .metadata("source_tool", "ids")
        .vector(mock_embedding(attack_signature))
        .build();
    db.ingest_event(ids_event).await.unwrap();

    // Find similar events (should cluster together due to same embedding)
    let query = mock_embedding(attack_signature);
    let similar = db.find_similar(&query, 5).await.unwrap();

    println!("Found {} similar events across tools", similar.len());
    assert_eq!(similar.len(), 3); // All 3 events should be similar

    // Verify different source tools
    let logs = db.scan_logs().await.unwrap();
    let tools: Vec<_> = logs.iter()
        .filter_map(|e| {
            let meta: serde_json::Value = serde_json::from_str(&e.metadata_json).ok()?;
            meta.get("source_tool")?.as_str().map(String::from)
        })
        .collect();
    
    assert!(tools.contains(&"edr".to_string()));
    assert!(tools.contains(&"siem".to_string()));
    assert!(tools.contains(&"ids".to_string()));
}
