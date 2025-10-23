pub mod bloom;

pub use bloom::BloomFilterPolicy;

/// Filter policy trait for determining if a key might exist
pub trait FilterPolicy: Send + Sync {
    /// Returns the name of this filter policy
    fn name(&self) -> &str;

    /// Create a filter for the given keys
    /// Keys are concatenated with their lengths
    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;

    /// Test if the key may exist in the filter
    /// Returns false if definitely not in filter, true if might be in filter
    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;
}
