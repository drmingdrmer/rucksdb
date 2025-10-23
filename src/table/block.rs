use crate::table::format::{calculate_checksum, decode_varint, CompressionType};
use crate::util::{Result, Slice, Status};

/// Block reader for SSTable data blocks
///
/// Reads entries from a block with support for:
/// - Prefix-compressed keys
/// - Restart points for efficient seeking
/// - Checksum verification
#[derive(Debug)]
pub struct Block {
    data: Vec<u8>,
    restart_offset: usize,
    num_restarts: u32,
}

impl Block {
    /// Create a block from raw data
    /// Data format: [entries...][restarts...][num_restarts:4][compression:1][checksum:4]
    pub fn new(data: Vec<u8>) -> Result<Self> {
        if data.len() < 9 {
            // Minimum: num_restarts(4) + compression(1) + checksum(4)
            return Err(Status::corruption("Block too small"));
        }

        let len = data.len();

        // Extract and verify checksum
        let stored_checksum = u32::from_le_bytes([
            data[len - 4],
            data[len - 3],
            data[len - 2],
            data[len - 1],
        ]);
        let actual_checksum = calculate_checksum(&data[..len - 5]); // Exclude compression and checksum
        if stored_checksum != actual_checksum {
            return Err(Status::corruption(format!(
                "Block checksum mismatch: expected {}, got {}",
                actual_checksum, stored_checksum
            )));
        }

        // Extract compression type
        let compression = CompressionType::from_u8(data[len - 5])
            .ok_or_else(|| Status::corruption("Invalid compression type"))?;
        if compression != CompressionType::None {
            return Err(Status::not_supported("Compression not yet supported"));
        }

        // Extract number of restarts
        let num_restarts = u32::from_le_bytes([
            data[len - 9],
            data[len - 8],
            data[len - 7],
            data[len - 6],
        ]);

        let restart_offset = len - 9 - (num_restarts as usize * 4);

        Ok(Block {
            data,
            restart_offset,
            num_restarts,
        })
    }

    /// Get number of restart points
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    /// Get restart point offset by index
    fn get_restart_point(&self, index: u32) -> Option<u32> {
        if index >= self.num_restarts {
            return None;
        }
        let offset = self.restart_offset + (index as usize * 4);
        Some(u32::from_le_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
        ]))
    }

    /// Create an iterator over the block
    pub fn iter(&self) -> BlockIterator {
        BlockIterator::new(self)
    }

    /// Decode entry at given offset
    /// Returns: (key, value, next_offset)
    fn decode_entry(&self, offset: usize, prev_key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, usize)> {
        if offset >= self.restart_offset {
            return Err(Status::corruption("Offset beyond block data"));
        }

        let mut pos = offset;

        // Decode shared length
        let (shared, n1) = decode_varint(&self.data[pos..])
            .ok_or_else(|| Status::corruption("Failed to decode shared length"))?;
        pos += n1;

        // Decode non-shared length
        let (non_shared, n2) = decode_varint(&self.data[pos..])
            .ok_or_else(|| Status::corruption("Failed to decode non-shared length"))?;
        pos += n2;

        // Decode value length
        let (value_len, n3) = decode_varint(&self.data[pos..])
            .ok_or_else(|| Status::corruption("Failed to decode value length"))?;
        pos += n3;

        // Extract key
        let mut key = Vec::with_capacity(shared as usize + non_shared as usize);
        if shared > 0 {
            if shared as usize > prev_key.len() {
                return Err(Status::corruption("Shared length exceeds previous key"));
            }
            key.extend_from_slice(&prev_key[..shared as usize]);
        }

        if pos + non_shared as usize > self.restart_offset {
            return Err(Status::corruption("Key data extends beyond block"));
        }
        key.extend_from_slice(&self.data[pos..pos + non_shared as usize]);
        pos += non_shared as usize;

        // Extract value
        if pos + value_len as usize > self.restart_offset {
            return Err(Status::corruption("Value data extends beyond block"));
        }
        let value = self.data[pos..pos + value_len as usize].to_vec();
        pos += value_len as usize;

        Ok((key, value, pos))
    }
}

/// Iterator over entries in a block
pub struct BlockIterator<'a> {
    block: &'a Block,
    current_offset: usize,
    current_key: Vec<u8>,
    current_value: Vec<u8>,
    restart_index: u32,
}

impl<'a> BlockIterator<'a> {
    fn new(block: &'a Block) -> Self {
        BlockIterator {
            block,
            current_offset: 0,
            current_key: Vec::new(),
            current_value: Vec::new(),
            restart_index: 0,
        }
    }

    /// Seek to the first entry
    pub fn seek_to_first(&mut self) -> Result<bool> {
        self.current_offset = 0;
        self.current_key.clear();
        self.restart_index = 0;
        self.next()
    }

    /// Move to the next entry
    pub fn next(&mut self) -> Result<bool> {
        if self.current_offset >= self.block.restart_offset {
            return Ok(false);
        }

        let prev_key = if self.current_offset == 0 {
            Vec::new()
        } else {
            self.current_key.clone()
        };

        match self.block.decode_entry(self.current_offset, &prev_key) {
            Ok((key, value, next_offset)) => {
                self.current_key = key;
                self.current_value = value;
                self.current_offset = next_offset;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Get current key
    pub fn key(&self) -> Slice {
        Slice::from(self.current_key.clone())
    }

    /// Get current value
    pub fn value(&self) -> Slice {
        Slice::from(self.current_value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::block_builder::BlockBuilder;

    fn build_test_block(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = BlockBuilder::new(16);
        for (key, value) in entries {
            builder.add(&Slice::from(*key), &Slice::from(*value));
        }
        builder.finish()
    }

    #[test]
    fn test_block_single_entry() {
        let data = build_test_block(&[("key1", "value1")]);
        let block = Block::new(data).unwrap();

        let mut iter = block.iter();
        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), Slice::from("key1"));
        assert_eq!(iter.value(), Slice::from("value1"));
        assert!(!iter.next().unwrap());
    }

    #[test]
    fn test_block_multiple_entries() {
        let entries = vec![
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ];
        let data = build_test_block(&entries);
        let block = Block::new(data).unwrap();

        let mut iter = block.iter();
        assert!(iter.seek_to_first().unwrap());

        for (expected_key, expected_value) in entries {
            assert_eq!(iter.key(), Slice::from(expected_key));
            assert_eq!(iter.value(), Slice::from(expected_value));
            if expected_key != "key3" {
                assert!(iter.next().unwrap());
            }
        }
    }

    #[test]
    fn test_block_iteration() {
        let entries = vec![
            ("aaa", "value1"),
            ("bbb", "value2"),
            ("ccc", "value3"),
            ("ddd", "value4"),
        ];
        let data = build_test_block(&entries);
        let block = Block::new(data).unwrap();

        let mut iter = block.iter();
        assert!(iter.seek_to_first().unwrap());

        for (expected_key, expected_value) in entries {
            assert_eq!(iter.key(), Slice::from(expected_key));
            assert_eq!(iter.value(), Slice::from(expected_value));
            let has_next = iter.next().unwrap();
            if expected_key != "ddd" {
                assert!(has_next);
            }
        }
    }

    #[test]
    fn test_block_prefix_compression() {
        let entries = vec![
            ("key_0001", "value1"),
            ("key_0002", "value2"),
            ("key_0003", "value3"),
        ];
        let data = build_test_block(&entries);
        let block = Block::new(data).unwrap();

        let mut iter = block.iter();
        assert!(iter.seek_to_first().unwrap());

        for (expected_key, expected_value) in entries {
            assert_eq!(iter.key(), Slice::from(expected_key));
            assert_eq!(iter.value(), Slice::from(expected_value));
            if expected_key != "key_0003" {
                assert!(iter.next().unwrap());
            }
        }
    }

    #[test]
    fn test_block_invalid_checksum() {
        let mut data = build_test_block(&[("key", "value")]);
        // Corrupt the checksum
        let len = data.len();
        data[len - 1] ^= 0xFF;

        let result = Block::new(data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .unwrap()
            .contains("checksum mismatch"));
    }
}
