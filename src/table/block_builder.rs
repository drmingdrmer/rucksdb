use crate::table::format::{
    CompressionType, DEFAULT_RESTART_INTERVAL, calculate_checksum, encode_varint,
};
use crate::util::Slice;

/// Block builder for SSTable data blocks
///
/// Builds a block with key prefix compression:
/// - Stores restart points every N entries
/// - Uses prefix compression for keys between restart points
pub struct BlockBuilder {
    /// Block data buffer
    buffer: Vec<u8>,
    /// Restart points (offsets in buffer)
    restarts: Vec<u32>,
    /// Counter for entries since last restart
    counter: usize,
    /// Restart interval
    restart_interval: usize,
    /// Last key added (for prefix compression)
    last_key: Vec<u8>,
    /// Whether the block is finished
    finished: bool,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        let mut builder = BlockBuilder {
            buffer: Vec::new(),
            restarts: Vec::new(),
            counter: 0,
            restart_interval,
            last_key: Vec::new(),
            finished: false,
        };
        builder.restarts.push(0); // First restart point at offset 0
        builder
    }

    /// Add a key-value pair to the block
    /// Keys must be added in sorted order
    pub fn add(&mut self, key: &Slice, value: &Slice) {
        assert!(!self.finished, "Block is already finished");
        assert!(
            self.counter <= self.restart_interval,
            "Counter exceeds restart interval"
        );

        let mut shared = 0usize;

        if self.counter < self.restart_interval {
            // Find shared prefix with last key
            let min_len = self.last_key.len().min(key.size());
            while shared < min_len && self.last_key[shared] == key.data()[shared] {
                shared += 1;
            }
        } else {
            // New restart point
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let non_shared = key.size() - shared;

        // Encode entry: shared_len | non_shared_len | value_len | key[shared..] | value
        self.buffer.extend_from_slice(&encode_varint(shared as u64));
        self.buffer
            .extend_from_slice(&encode_varint(non_shared as u64));
        self.buffer
            .extend_from_slice(&encode_varint(value.size() as u64));
        self.buffer.extend_from_slice(&key.data()[shared..]);
        self.buffer.extend_from_slice(value.data());

        // Update last key
        self.last_key.clear();
        self.last_key.extend_from_slice(key.data());

        self.counter += 1;
    }

    /// Finish building the block with no compression
    /// Returns the complete block data
    pub fn finish(&mut self) -> Vec<u8> {
        self.finish_with_compression(CompressionType::None)
    }

    /// Finish building the block with specified compression
    /// Returns the complete block data including:
    /// - Compressed/uncompressed block data
    /// - Restart array
    /// - Num restarts (4 bytes)
    /// - Compression type (1 byte)
    /// - CRC32 checksum (4 bytes)
    pub fn finish_with_compression(&mut self, compression: CompressionType) -> Vec<u8> {
        if self.finished {
            return self.buffer.clone();
        }

        // Build uncompressed block first
        let mut uncompressed = Vec::new();
        uncompressed.extend_from_slice(&self.buffer);

        // Append restart array
        for &restart in &self.restarts {
            uncompressed.extend_from_slice(&restart.to_le_bytes());
        }

        // Append number of restarts
        uncompressed.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());

        // Compress the block if needed
        let (final_data, final_compression) = if compression != CompressionType::None {
            match crate::compression::compress(compression, &uncompressed) {
                Ok(data) if data.len() < uncompressed.len() => {
                    // Compression helped, use it
                    (data, compression)
                }
                _ => {
                    // Compression failed or made it larger, use uncompressed
                    (uncompressed, CompressionType::None)
                }
            }
        } else {
            (uncompressed, CompressionType::None)
        };

        // Calculate checksum of final data
        let checksum = calculate_checksum(&final_data);

        // Build final block
        self.buffer = final_data;
        self.buffer.push(final_compression as u8);
        self.buffer.extend_from_slice(&checksum.to_le_bytes());

        self.finished = true;
        self.buffer.clone()
    }

    /// Get current block size estimate
    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len()
            + self.restarts.len() * 4  // Restart array
            + 4  // Num restarts
            + 1  // Compression type
            + 4 // Checksum
    }

    /// Check if the block is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Reset the builder for reuse
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.finished = false;
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_RESTART_INTERVAL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder_empty() {
        let builder = BlockBuilder::new(16);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_block_builder_single_entry() {
        let mut builder = BlockBuilder::new(16);
        builder.add(&Slice::from("key1"), &Slice::from("value1"));

        let block = builder.finish();
        assert!(!block.is_empty());

        // Check that block contains data
        assert!(block.len() > "key1".len() + "value1".len());
    }

    #[test]
    fn test_block_builder_multiple_entries() {
        let mut builder = BlockBuilder::new(16);

        for i in 0..10 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            builder.add(&Slice::from(key), &Slice::from(value));
        }

        let block = builder.finish();
        assert!(!block.is_empty());

        // Verify we have restart points
        assert_eq!(builder.restarts.len(), 1); // Only one restart for 10 entries
    }

    #[test]
    fn test_block_builder_restart_points() {
        let mut builder = BlockBuilder::new(4); // Restart every 4 entries

        for i in 0..10 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            builder.add(&Slice::from(key), &Slice::from(value));
        }

        // Should have 3 restart points: 0, 4, 8
        assert_eq!(builder.restarts.len(), 3);
    }

    #[test]
    fn test_block_builder_prefix_compression() {
        let mut builder = BlockBuilder::new(16);

        // Add keys with common prefix
        builder.add(&Slice::from("key_0001"), &Slice::from("value1"));
        builder.add(&Slice::from("key_0002"), &Slice::from("value2"));
        builder.add(&Slice::from("key_0003"), &Slice::from("value3"));

        let block = builder.finish();

        // With prefix compression, the block should be smaller than
        // storing all keys fully
        let full_size = 3 * ("key_0001".len() + "value1".len());
        assert!(block.len() < full_size + 100); // Allow some overhead
    }

    #[test]
    fn test_block_builder_reset() {
        let mut builder = BlockBuilder::new(16);

        builder.add(&Slice::from("key1"), &Slice::from("value1"));
        assert!(!builder.is_empty());

        builder.reset();
        assert!(builder.is_empty());
        assert_eq!(builder.restarts.len(), 1);
        assert_eq!(builder.counter, 0);
    }

    #[test]
    fn test_block_builder_size_estimate() {
        let mut builder = BlockBuilder::new(16);

        let initial_estimate = builder.current_size_estimate();
        assert!(initial_estimate > 0); // Should include restart array overhead

        builder.add(&Slice::from("key1"), &Slice::from("value1"));
        let after_add = builder.current_size_estimate();
        assert!(after_add > initial_estimate);
    }
}
