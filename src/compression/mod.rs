use crate::table::format::CompressionType;
use crate::util::{Result, Status};

/// Compress data using the specified compression type
pub fn compress(compression: CompressionType, data: &[u8]) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => compress_snappy(data),
        CompressionType::Lz4 => compress_lz4(data),
    }
}

/// Decompress data using the specified compression type
pub fn decompress(compression: CompressionType, data: &[u8]) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => decompress_snappy(data),
        CompressionType::Lz4 => decompress_lz4(data),
    }
}

/// Compress data using Snappy
fn compress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = snap::raw::Encoder::new();
    encoder
        .compress_vec(data)
        .map_err(|e| Status::io_error(format!("Snappy compression failed: {}", e)))
}

/// Decompress data using Snappy
fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = snap::raw::Decoder::new();
    decoder
        .decompress_vec(data)
        .map_err(|e| Status::corruption(format!("Snappy decompression failed: {}", e)))
}

/// Compress data using LZ4
fn compress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(data))
}

/// Decompress data using LZ4
fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| Status::corruption(format!("LZ4 decompression failed: {:?}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let data = b"Hello, World!";
        let compressed = compress(CompressionType::None, data).unwrap();
        assert_eq!(compressed, data);

        let decompressed = decompress(CompressionType::None, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compression() {
        let data = b"Hello, World! This is a test of Snappy compression. ".repeat(10);
        let compressed = compress(CompressionType::Snappy, &data).unwrap();

        // Compressed should be smaller for repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = decompress(CompressionType::Snappy, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_compression() {
        let data = b"Hello, World! This is a test of LZ4 compression. ".repeat(10);
        let compressed = compress(CompressionType::Lz4, &data).unwrap();

        // Compressed should be smaller for repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = decompress(CompressionType::Lz4, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_ratio() {
        // Test with highly compressible data
        let data = vec![b'A'; 1000];

        let snappy_compressed = compress(CompressionType::Snappy, &data).unwrap();
        let lz4_compressed = compress(CompressionType::Lz4, &data).unwrap();

        println!(
            "Original: {} bytes, Snappy: {} bytes ({:.1}%), LZ4: {} bytes ({:.1}%)",
            data.len(),
            snappy_compressed.len(),
            snappy_compressed.len() as f64 / data.len() as f64 * 100.0,
            lz4_compressed.len(),
            lz4_compressed.len() as f64 / data.len() as f64 * 100.0
        );

        // Both should compress significantly
        assert!(snappy_compressed.len() < data.len() / 10);
        assert!(lz4_compressed.len() < data.len() / 10);
    }

    #[test]
    fn test_random_data_compression() {
        // Random data should not compress well
        let data: Vec<u8> = (0..1000).map(|i| (i * 97 % 256) as u8).collect();

        let snappy_compressed = compress(CompressionType::Snappy, &data).unwrap();
        let lz4_compressed = compress(CompressionType::Lz4, &data).unwrap();

        // Random data typically expands slightly due to compression overhead
        println!(
            "Random data - Original: {} bytes, Snappy: {} bytes, LZ4: {} bytes",
            data.len(),
            snappy_compressed.len(),
            lz4_compressed.len()
        );

        // Verify decompression still works
        let snappy_decompressed = decompress(CompressionType::Snappy, &snappy_compressed).unwrap();
        let lz4_decompressed = decompress(CompressionType::Lz4, &lz4_compressed).unwrap();

        assert_eq!(snappy_decompressed, data);
        assert_eq!(lz4_decompressed, data);
    }
}
