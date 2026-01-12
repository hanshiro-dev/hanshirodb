//! # Embedding Trait
//!
//! Trait for integrating embedding models with HanshiroDB.
//!
//! ## Production Options
//!
//! 1. **Local models** (via candle, ort, or tract):
//!    - all-MiniLM-L6-v2 for logs (384 dims)
//!    - CodeBERT for code/binaries (768 dims)
//!
//! 2. **Remote APIs**:
//!    - OpenAI text-embedding-3-small
//!    - AWS Bedrock Titan Embeddings
//!    - Cohere embed-v3
//!
//! 3. **Self-hosted**:
//!    - TEI (Text Embeddings Inference)
//!    - vLLM with embedding models

use async_trait::async_trait;
use hanshiro_core::error::Result;

/// Trait for embedding text into vectors
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Embed a single text string
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;
    
    /// Embed multiple texts (batch for efficiency)
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }
    
    /// Vector dimension
    fn dimension(&self) -> usize;
}

/// No-op embedder for testing or when embeddings are pre-computed
pub struct NoOpEmbedder {
    pub dimension: usize,
}

impl NoOpEmbedder {
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }
}

#[async_trait]
impl Embedder for NoOpEmbedder {
    async fn embed(&self, _text: &str) -> Result<Vec<f32>> {
        Ok(vec![0.0; self.dimension])
    }
    
    fn dimension(&self) -> usize {
        self.dimension
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_embedder() {
        let embedder = NoOpEmbedder::new(384);
        let vec = embedder.embed("test").await.unwrap();
        assert_eq!(vec.len(), 384);
    }
}
