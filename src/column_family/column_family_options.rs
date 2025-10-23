use crate::table::format::CompressionType;

/// Options for a specific Column Family
///
/// Each column family can have different configuration for:
/// - Write buffer size (MemTable size before flush)
/// - Compression type (for SSTables)
/// - Bloom filter configuration
/// - Block cache size
///
/// # Example
///
/// ```ignore
/// use rucksdb::{ColumnFamilyOptions, CompressionType};
///
/// let options = ColumnFamilyOptions {
///     write_buffer_size: 8 * 1024 * 1024,  // 8MB
///     compression_type: CompressionType::Lz4,
///     filter_bits_per_key: Some(10),
///     block_cache_size: 2000,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ColumnFamilyOptions {
    /// Size of write buffer (MemTable) in bytes before flushing to disk
    /// Default: 4MB
    pub write_buffer_size: usize,

    /// Compression type for SSTable blocks
    /// Default: Snappy
    pub compression_type: CompressionType,

    /// Bits per key for bloom filter (None = no bloom filter)
    /// Default: Some(10) ~1% false positive rate
    pub filter_bits_per_key: Option<usize>,

    /// Number of blocks to cache (block_size = 4KB by default)
    /// Default: 1000 blocks (~4MB)
    pub block_cache_size: usize,
}

impl Default for ColumnFamilyOptions {
    fn default() -> Self {
        ColumnFamilyOptions {
            write_buffer_size: 4 * 1024 * 1024, // 4MB
            compression_type: CompressionType::Snappy,
            filter_bits_per_key: Some(10),
            block_cache_size: 1000,
        }
    }
}
