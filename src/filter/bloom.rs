use super::FilterPolicy;

/// Bloom filter policy
/// Uses k hash functions to reduce false positive rate
/// Default: 10 bits per key gives ~1% false positive rate
pub struct BloomFilterPolicy {
    bits_per_key: usize,
}

impl BloomFilterPolicy {
    /// Create a new Bloom filter policy
    /// bits_per_key: Number of bits to use per key (10 gives ~1% false positive rate)
    pub fn new(bits_per_key: usize) -> Self {
        BloomFilterPolicy { bits_per_key }
    }

    /// Calculate number of hash functions to use
    /// k = (m/n) * ln(2) where m = bits, n = keys
    /// We use a simplified formula: k = bits_per_key * 0.69
    fn num_hash_functions(bits_per_key: usize) -> usize {
        let k = (bits_per_key as f64 * 0.69) as usize;
        k.max(1).min(30) // At least 1, at most 30
    }

    /// Bloom hash: simple hash function for bloom filters
    fn bloom_hash(data: &[u8]) -> u32 {
        let mut h = 0xbc9f1d34u32;
        for &b in data {
            h = h.wrapping_mul(0x9e3779b9).wrapping_add(b as u32);
        }
        h
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &str {
        "leveldb.BuiltinBloomFilter2"
    }

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        if keys.is_empty() {
            // Empty filter
            return vec![0]; // k = 0
        }

        // Calculate filter size in bits and bytes
        let mut bits = keys.len() * self.bits_per_key;

        // Minimum size to avoid false positives with small key sets
        if bits < 64 {
            bits = 64;
        }

        let bytes = (bits + 7) / 8; // Round up to nearest byte
        let bits = bytes * 8; // Actual bits after rounding

        // Initialize filter with zeros
        let mut filter = vec![0u8; bytes + 1]; // +1 for k value at the end

        // Store number of hash functions in the last byte
        let k = Self::num_hash_functions(self.bits_per_key);
        filter[bytes] = k as u8;

        // Add all keys to filter
        for key in keys {
            let h = Self::bloom_hash(key);
            let delta = (h >> 17) | (h << 15); // Rotate right 17 bits

            for i in 0..k {
                let bit_pos = h.wrapping_add((i as u32).wrapping_mul(delta)) as usize % bits;
                filter[bit_pos / 8] |= 1 << (bit_pos % 8);
            }
        }

        filter
    }

    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool {
        if filter.len() < 2 {
            return false;
        }

        let bytes = filter.len() - 1;
        let bits = bytes * 8;
        let k = filter[bytes] as usize;

        if k > 30 {
            // Invalid k value, consider it a match to be safe
            return true;
        }

        let h = Self::bloom_hash(key);
        let delta = (h >> 17) | (h << 15); // Rotate right 17 bits

        for i in 0..k {
            let bit_pos = h.wrapping_add((i as u32).wrapping_mul(delta)) as usize % bits;
            if (filter[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
                return false; // Definitely not in set
            }
        }

        true // Might be in set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_empty() {
        let policy = BloomFilterPolicy::new(10);
        let filter = policy.create_filter(&[]);

        assert_eq!(filter.len(), 1);
        assert_eq!(filter[0], 0); // k = 0
    }

    #[test]
    fn test_bloom_filter_single_key() {
        let policy = BloomFilterPolicy::new(10);
        let keys = vec![b"hello".to_vec()];
        let filter = policy.create_filter(&keys);

        assert!(policy.may_contain(&filter, b"hello"));
        assert!(!policy.may_contain(&filter, b"world"));
    }

    #[test]
    fn test_bloom_filter_multiple_keys() {
        let policy = BloomFilterPolicy::new(10);
        let keys = vec![
            b"foo".to_vec(),
            b"bar".to_vec(),
            b"baz".to_vec(),
        ];
        let filter = policy.create_filter(&keys);

        assert!(policy.may_contain(&filter, b"foo"));
        assert!(policy.may_contain(&filter, b"bar"));
        assert!(policy.may_contain(&filter, b"baz"));
        assert!(!policy.may_contain(&filter, b"nothere"));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let policy = BloomFilterPolicy::new(10);

        // Insert 100 keys
        let mut keys = Vec::new();
        for i in 0..100 {
            keys.push(format!("key{:04}", i).into_bytes());
        }
        let filter = policy.create_filter(&keys);

        // Test that all inserted keys are found
        for i in 0..100 {
            let key = format!("key{:04}", i);
            assert!(policy.may_contain(&filter, key.as_bytes()));
        }

        // Test false positive rate on non-existent keys
        let mut false_positives = 0;
        for i in 100..1000 {
            let key = format!("key{:04}", i);
            if policy.may_contain(&filter, key.as_bytes()) {
                false_positives += 1;
            }
        }

        // With 10 bits per key, false positive rate should be around 1%
        // For 900 tests, we expect around 9 false positives
        // Allow some margin: should be less than 5% (45 false positives)
        assert!(false_positives < 45, "False positive rate too high: {}/900", false_positives);
        println!("False positive rate: {:.2}% ({}/900)",
                 false_positives as f64 / 9.0, false_positives);
    }

    #[test]
    fn test_bloom_filter_different_bits_per_key() {
        // Test with fewer bits (higher false positive rate)
        let policy_low = BloomFilterPolicy::new(5);
        let mut keys = Vec::new();
        for i in 0..50 {
            keys.push(format!("key{}", i).into_bytes());
        }
        let filter_low = policy_low.create_filter(&keys);

        // Test with more bits (lower false positive rate)
        let policy_high = BloomFilterPolicy::new(20);
        let filter_high = policy_high.create_filter(&keys);

        // Higher bits should produce larger filter
        assert!(filter_high.len() > filter_low.len());
    }

    #[test]
    fn test_bloom_filter_hash_function() {
        // Test that hash function is deterministic
        let h1 = BloomFilterPolicy::bloom_hash(b"test");
        let h2 = BloomFilterPolicy::bloom_hash(b"test");
        assert_eq!(h1, h2);

        // Test that different inputs produce different hashes
        let h3 = BloomFilterPolicy::bloom_hash(b"test2");
        assert_ne!(h1, h3);
    }
}
