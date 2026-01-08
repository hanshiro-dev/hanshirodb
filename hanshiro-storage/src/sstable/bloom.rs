//! Bloom filter implementation for SSTable using gxhash for speed

/// Bloom filter for probabilistic membership testing
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hash_functions: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with specified bits per key and expected items
    pub fn new(bits_per_key: usize, num_keys: usize) -> Self {
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
        let bits_per_key = (-false_positive_rate.ln() / (2.0_f64.ln().powi(2))) * 1.44;
        Self::new(bits_per_key.ceil() as usize, expected_items)
    }
    
    /// Insert a key into the bloom filter
    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        let h = gxhash::gxhash64(key, 0);
        let h1 = h as usize;
        let h2 = (h >> 32) as usize;
        
        for i in 0..self.num_hash_functions {
            let bit_pos = h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }
    
    /// Check if a key might be in the set (false positives possible)
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        let h = gxhash::gxhash64(key, 0);
        let h1 = h as usize;
        let h2 = (h >> 32) as usize;
        
        for i in 0..self.num_hash_functions {
            let bit_pos = h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits;
            if (self.bits[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
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
        Self { bits, num_bits, num_hash_functions }
    }
    
    /// Get metadata for serialization
    pub fn metadata(&self) -> (usize, usize) {
        (self.num_hash_functions, self.num_bits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(10, 100);
        
        bloom.insert(b"key1");
        bloom.insert(b"key2");
        bloom.insert(b"key3");
        
        assert!(bloom.contains(b"key1"));
        assert!(bloom.contains(b"key2"));
        assert!(bloom.contains(b"key3"));
    }
    
    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut bloom = BloomFilter::with_rate(0.01, 1000);
        
        // Insert 1000 keys
        for i in 0..1000u32 {
            bloom.insert(&i.to_le_bytes());
        }
        
        // Check false positive rate on non-existent keys
        let mut false_positives = 0;
        for i in 1000..2000u32 {
            if bloom.contains(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }
        
        // Should be around 1% (10 out of 1000), allow some variance
        assert!(false_positives < 30, "Too many false positives: {}", false_positives);
    }
}