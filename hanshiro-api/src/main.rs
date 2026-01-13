//! # HanshiroDB Server
//!
//! Run with: `cargo run --bin hanshiro-server -- --data-dir ./data --port 3000`

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::{info, Level};

use hanshiro_api::server::{AppState, create_router};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    let args: Vec<String> = std::env::args().collect();
    
    let data_dir = args.iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data"));
    
    let port: u16 = args.iter()
        .position(|a| a == "--port")
        .and_then(|i| args.get(i + 1))
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    let addr = format!("0.0.0.0:{}", port);

    info!("Starting HanshiroDB server on {}", addr);
    info!("Data directory: {:?}", data_dir);

    let state = Arc::new(AppState::new(data_dir).await
        .map_err(|e| anyhow::anyhow!("{}", e))?);
    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
