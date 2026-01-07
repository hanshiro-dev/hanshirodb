//! Bloom filter implementation for SSTable

use std::hash::Hash;

/// Bloom filter for probabilistic membership testing
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hash_functions: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with specified false positive rate and expected items
    pub fn new(bits_per_key: usize, num_keys: usize) -> Self {
        // Calculate optimal number of bits and hash functions
        let num_bits = std::cmp::max(64, bits_per_key * num_keys);
        let num_bytes = (num_bits + 7) / 8;
        let num_hash_functions = std::cmp::max(1, (bits_per_key as f64 * 0.69) as usize);
        
        Self {
            bits: vec![0; num_bytes],
            num_bits,
            num_hash_functions,
        }
    }
    
    /// Create bloom filter with a target false positive rate
    pub fn with_rate(false_positive_rate: f64, expected_items: usize) -> Self {
        // Calculate optimal bits per key for target false positive rate
        let bits_per_key = (-false_positive_rate.ln() / (2.0_f64.ln().powi(2))) * 1.44;
        Self::new(bits_per_key.ceil() as usize, expected_items)
    }
    
    /// Insert a key into the bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        let hash = self.hash(key);
        for i in 0..self.num_hash_functions {
            let bit_pos = self.get_bit_position(hash, i);
            let byte_idx = bit_pos / 8;
            let bit_idx = bit_pos % 8;
            if byte_idx < self.bits.len() {
                self.bits[byte_idx] |= 1 << bit_idx;
            }
        }
    }
    
    /// Check if a key might be in the set (false positives possible)
    pub fn contains(&self, key: &[u8]) -> bool {
        let hash = self.hash(key);
        for i in 0..self.num_hash_functions {
            let bit_pos = self.get_bit_position(hash, i);
            let byte_idx = bit_pos / 8;
            let bit_idx = bit_pos % 8;
            if byte_idx >= self.bits.len() || (self.bits[byte_idx] & (1 << bit_idx)) == 0 {
                return false;
            }
        }
        true
    }
    
    /// Get the raw bits for serialization
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
    
    /// Create bloom filter from raw bits
    pub fn from_bytes(bits: Vec<u8>, bits_per_key: usize) -> Self {
        let num_bits = bits.len() * 8;
        let num_hash_functions = std::cmp::max(1, (bits_per_key as f64 * 0.69) as usize);
        
        Self {
            bits,
            num_bits,
            num_hash_functions,
        }
    }
    
    /// Get metadata for serialization
    pub fn metadata(&self) -> (usize, usize) {
        (self.num_hash_functions, self.num_bits)
    }
    
    fn hash(&self, key: &[u8]) -> u64 {
        // Use a deterministic hash function (FNV-1a)
        let mut hash = 0xcbf29ce484222325u64; // FNV-1a offset basis
        for byte in key {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3); // FNV-1a prime
        }
        hash
    }
    
    fn get_bit_position(&self, hash: u64, i: usize) -> usize {
        // Use double hashing for multiple hash functions
        let h1 = hash as usize;
        let h2 = (hash >> 32) as usize;
        (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(10, 100);
        
        // Insert some keys
        bloom.insert(b"key1");
        bloom.insert(b"key2");
        bloom.insert(b"key3");
        
        // Check they exist
        assert!(bloom.contains(b"key1"));
        assert!(bloom.contains(b"key2"));
        assert!(bloom.contains(b"key3"));
        
        // Check non-existent keys (might have false positives)
        // This is probabilistic, so we just verify the method works
        let _ = bloom.contains(b"key4");
        let _ = bloom.contains(b"key5");
    }
}