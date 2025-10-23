/// WAL (Write Ahead Log) file format
///
/// A WAL file consists of a sequence of 32KB blocks. Each block contains
/// multiple records. A record may span across multiple blocks.
///
/// Block format:
/// +---------+---------+---------+-----+
/// | Record1 | Record2 | Record3 | ... |
/// +---------+---------+---------+-----+
///
/// Record format:
/// +----------+--------+------+--------+
/// | Checksum | Length | Type | Data   |
/// +----------+--------+------+--------+
/// | 4 bytes  | 2 bytes| 1 byte| N bytes|
/// +----------+--------+------+--------+
///
/// Record types:
/// - Full: Complete record fits in one block
/// - First: First fragment of a record
/// - Middle: Middle fragment of a record
/// - Last: Last fragment of a record
use crc32fast::Hasher;

/// Block size is 32KB
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Header size: checksum(4) + length(2) + type(1) = 7 bytes
pub const HEADER_SIZE: usize = 7;

/// Record type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Complete record
    Full = 1,
    /// First fragment of a record
    First = 2,
    /// Middle fragment of a record
    Middle = 3,
    /// Last fragment of a record
    Last = 4,
}

impl RecordType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(RecordType::Full),
            2 => Some(RecordType::First),
            3 => Some(RecordType::Middle),
            4 => Some(RecordType::Last),
            _ => None,
        }
    }
}

/// Calculate CRC32 checksum for record
pub fn calculate_checksum(record_type: RecordType, data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&[record_type as u8]);
    hasher.update(data);
    hasher.finalize()
}

/// Encode record header
/// Returns: [checksum(4), length(2), type(1)]
pub fn encode_header(checksum: u32, length: u16, record_type: RecordType) -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&checksum.to_le_bytes());
    header[4..6].copy_from_slice(&length.to_le_bytes());
    header[6] = record_type as u8;
    header
}

/// Decode record header
/// Returns: (checksum, length, record_type)
pub fn decode_header(header: &[u8; HEADER_SIZE]) -> Option<(u32, u16, RecordType)> {
    let checksum = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let length = u16::from_le_bytes([header[4], header[5]]);
    let record_type = RecordType::from_u8(header[6])?;
    Some((checksum, length, record_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_type_conversion() {
        assert_eq!(RecordType::from_u8(1), Some(RecordType::Full));
        assert_eq!(RecordType::from_u8(2), Some(RecordType::First));
        assert_eq!(RecordType::from_u8(3), Some(RecordType::Middle));
        assert_eq!(RecordType::from_u8(4), Some(RecordType::Last));
        assert_eq!(RecordType::from_u8(5), None);
    }

    #[test]
    fn test_checksum_calculation() {
        let data = b"hello world";
        let checksum = calculate_checksum(RecordType::Full, data);
        assert_ne!(checksum, 0);

        let checksum2 = calculate_checksum(RecordType::Full, data);
        assert_eq!(checksum, checksum2);

        let checksum3 = calculate_checksum(RecordType::First, data);
        assert_ne!(checksum, checksum3);
    }

    #[test]
    fn test_header_encode_decode() {
        let checksum = 0x12345678;
        let length = 1024;
        let record_type = RecordType::Full;

        let header = encode_header(checksum, length, record_type);
        assert_eq!(header.len(), HEADER_SIZE);

        let (decoded_checksum, decoded_length, decoded_type) = decode_header(&header).unwrap();
        assert_eq!(decoded_checksum, checksum);
        assert_eq!(decoded_length, length);
        assert_eq!(decoded_type, record_type);
    }

    #[test]
    fn test_block_size() {
        assert_eq!(BLOCK_SIZE, 32768);
        assert_eq!(HEADER_SIZE, 7);
        const _: () = assert!(HEADER_SIZE < BLOCK_SIZE);
    }
}
