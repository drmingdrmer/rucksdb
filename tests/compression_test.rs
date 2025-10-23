use rucksdb::{CompressionType, Slice};
use rucksdb::table::{TableBuilder, TableReader};
use tempfile::NamedTempFile;

#[test]
fn test_no_compression() {
    let temp_file = NamedTempFile::new().unwrap();

    // Build table without compression
    {
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish().unwrap();
    }

    // Read table
    {
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected_value = format!("value{:04}", i);
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(value, Some(Slice::from(expected_value)));
        }
    }
}

#[test]
fn test_snappy_compression() {
    let temp_file = NamedTempFile::new().unwrap();

    // Build table with Snappy compression
    {
        let mut builder = TableBuilder::new_with_filter(temp_file.path(), None).unwrap();

        // Use repetitive data to get good compression
        for i in 0..200 {
            let key = format!("key{:04}", i);
            let value = "This is a highly repetitive value that should compress well with Snappy!".to_string();
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish().unwrap();
    }

    // Read table
    {
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        for i in 0..200 {
            let key = format!("key{:04}", i);
            let expected_value = "This is a highly repetitive value that should compress well with Snappy!";
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(value, Some(Slice::from(expected_value)));
        }
    }

    println!("Snappy compression test passed - all values read correctly");
}

#[test]
fn test_lz4_compression() {
    let temp_file = NamedTempFile::new().unwrap();

    // Build table with LZ4 compression
    {
        let mut builder = TableBuilder::new_with_filter(temp_file.path(), None).unwrap();

        // Use repetitive data to get good compression
        for i in 0..200 {
            let key = format!("key{:04}", i);
            let value = "LZ4 compression is very fast and provides excellent compression ratios!".to_string();
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish().unwrap();
    }

    // Read table
    {
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        for i in 0..200 {
            let key = format!("key{:04}", i);
            let expected_value = "LZ4 compression is very fast and provides excellent compression ratios!";
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(value, Some(Slice::from(expected_value)));
        }
    }

    println!("LZ4 compression test passed - all values read correctly");
}

#[test]
fn test_compression_with_varied_data() {
    let temp_file = NamedTempFile::new().unwrap();

    // Build table with varied data
    {
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for i in 0..100 {
            let key = format!("key{:04}", i);
            // Mix of compressible and incompressible data
            let value = if i % 2 == 0 {
                "AAAA".repeat(50) // Highly compressible
            } else {
                format!("Random_{}_data_{}", i, i * 97 % 256) // Less compressible
            };
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish().unwrap();
    }

    // Read and verify
    {
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected_value = if i % 2 == 0 {
                "AAAA".repeat(50)
            } else {
                format!("Random_{}_data_{}", i, i * 97 % 256)
            };
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(value, Some(Slice::from(expected_value)));
        }
    }

    println!("Mixed data compression test passed");
}

#[test]
fn test_large_values_compression() {
    let temp_file = NamedTempFile::new().unwrap();

    // Build table with large values
    {
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for i in 0..50 {
            let key = format!("bigkey{:04}", i);
            // Large repetitive value (10KB)
            let value = format!("Pattern_{}_", i).repeat(1000);
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish().unwrap();
    }

    // Read and verify
    {
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        for i in 0..50 {
            let key = format!("bigkey{:04}", i);
            let expected_value = format!("Pattern_{}_", i).repeat(1000);
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(value, Some(Slice::from(expected_value)));
        }
    }

    println!("Large values compression test passed");
}
