/// SSTable file format
///
/// An SSTable file consists of:
/// 1. Data blocks (variable size)
/// 2. Meta blocks (optional, e.g., filter blocks)
/// 3. Meta index block (points to meta blocks)
/// 4. Index block (points to data blocks)
/// 5. Footer (fixed 48 bytes at the end)
///
/// File layout (text diagram):
/// - Data Block 1
/// - Data Block 2
/// - ...
/// - Data Block N
/// - Meta Block 1 (optional)
/// - Meta Index (points to meta blocks)
/// - Index Block (points to data blocks)
/// - Footer (48 bytes)
///
/// Data Block format:
/// - Entry 1
/// - Entry 2
/// - ...
/// - Restart[0] (4 bytes)
/// - Restart[1] (4 bytes)
/// - ...
/// - Num Restarts (4 bytes)
/// - Compression Type (1 byte)
/// - CRC32 (4 bytes)
///
/// Entry format (with key prefix compression):
/// - Shared Key Len (varint)
/// - Unshared Key Len (varint)
/// - Value Len (varint)
/// - Unshared Key (bytes)
/// - Value (bytes)
///
/// Footer format (48 bytes):
/// - Meta Index Block Handle (offset: 8 bytes, size: 8 bytes)
/// - Index Block Handle (offset: 8 bytes, size: 8 bytes)
/// - Padding (24 bytes, reserved for future use)
/// - Magic Number (8 bytes: 0x88e3f3fb2af1ecd7)
use crc32fast::Hasher;

/// Block size for SSTable (default 4KB)
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Restart interval (number of entries between restart points)
pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Footer size (48 bytes)
pub const FOOTER_SIZE: usize = 48;

/// Magic number for SSTable files
pub const MAGIC_NUMBER: u64 = 0x88e3f3fb2af1ecd7;

/// Compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Snappy = 1,
    Lz4 = 2,
}

impl CompressionType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Snappy),
            2 => Some(CompressionType::Lz4),
            _ => None,
        }
    }
}

/// Block handle (offset and size of a block)
#[derive(Debug, Clone, Copy)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    /// Create a new BlockHandle pointing to a block at given offset and size.
    ///
    /// This is a const fn, allowing it to be evaluated at compile time for
    /// constant block handles.
    pub const fn new(offset: u64, size: u64) -> Self {
        BlockHandle { offset, size }
    }

    #[inline]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.size.to_le_bytes());
        buf
    }

    #[inline]
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }
        let offset = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let size = u64::from_le_bytes(data[8..16].try_into().ok()?);
        Some(BlockHandle { offset, size })
    }
}

/// Footer of SSTable file
#[derive(Debug, Clone)]
pub struct Footer {
    pub meta_index_handle: BlockHandle,
    pub index_handle: BlockHandle,
}

impl Footer {
    pub fn new(meta_index_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Footer {
            meta_index_handle,
            index_handle,
        }
    }

    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];

        // Meta index handle (16 bytes)
        let meta_encoded = self.meta_index_handle.encode();
        buf[0..16].copy_from_slice(&meta_encoded);

        // Index handle (16 bytes)
        let index_encoded = self.index_handle.encode();
        buf[16..32].copy_from_slice(&index_encoded);

        // Padding (24 bytes) - reserved for future use
        // Already zeroed

        // Magic number (8 bytes)
        buf[40..48].copy_from_slice(&MAGIC_NUMBER.to_le_bytes());

        buf
    }

    pub fn decode(data: &[u8; FOOTER_SIZE]) -> Option<Self> {
        // Verify magic number
        let magic = u64::from_le_bytes(data[40..48].try_into().ok()?);
        if magic != MAGIC_NUMBER {
            return None;
        }

        let meta_index_handle = BlockHandle::decode(&data[0..16])?;
        let index_handle = BlockHandle::decode(&data[16..32])?;

        Some(Footer {
            meta_index_handle,
            index_handle,
        })
    }
}

/// Calculate CRC32 checksum
#[inline]
pub fn calculate_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Encode varint (variable-length integer)
#[inline]
pub fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    while value >= 0x80 {
        buf.push((value & 0x7F | 0x80) as u8);
        value >>= 7;
    }
    buf.push(value as u8);
    buf
}

/// Decode varint
#[inline]
pub fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 64 {
            return None;
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte < 0x80 {
            return Some((value, i + 1));
        }
        shift += 7;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_type() {
        assert_eq!(CompressionType::from_u8(0), Some(CompressionType::None));
        assert_eq!(CompressionType::from_u8(1), Some(CompressionType::Snappy));
        assert_eq!(CompressionType::from_u8(2), Some(CompressionType::Lz4));
        assert_eq!(CompressionType::from_u8(3), None);
    }

    #[test]
    fn test_block_handle_encode_decode() {
        let handle = BlockHandle::new(1234, 5678);
        let encoded = handle.encode();
        let decoded = BlockHandle::decode(&encoded).unwrap();
        assert_eq!(decoded.offset, 1234);
        assert_eq!(decoded.size, 5678);
    }

    #[test]
    fn test_footer_encode_decode() {
        let meta_handle = BlockHandle::new(100, 200);
        let index_handle = BlockHandle::new(300, 400);
        let footer = Footer::new(meta_handle, index_handle);

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.meta_index_handle.offset, 100);
        assert_eq!(decoded.meta_index_handle.size, 200);
        assert_eq!(decoded.index_handle.offset, 300);
        assert_eq!(decoded.index_handle.size, 400);
    }

    #[test]
    fn test_footer_invalid_magic() {
        let mut data = [0u8; FOOTER_SIZE];
        data[40..48].copy_from_slice(&0x1234567890abcdefu64.to_le_bytes());
        assert!(Footer::decode(&data).is_none());
    }

    #[test]
    fn test_varint_encode_decode() {
        let test_cases = vec![0, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX];

        for value in test_cases {
            let encoded = encode_varint(value);
            let (decoded, len) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(len, encoded.len());
        }
    }

    #[test]
    fn test_checksum() {
        let data = b"hello world";
        let checksum1 = calculate_checksum(data);
        let checksum2 = calculate_checksum(data);
        assert_eq!(checksum1, checksum2);

        let data2 = b"hello world!";
        let checksum3 = calculate_checksum(data2);
        assert_ne!(checksum1, checksum3);
    }
}
