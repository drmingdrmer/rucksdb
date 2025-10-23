use std::sync::Arc;

use rucksdb::{
    BloomFilterPolicy, Slice,
    table::{CompressionType, TableBuilder, TableReader},
};
use tempfile::NamedTempFile;

#[test]
fn test_bloom_filter_with_sstable() {
    let temp_file = NamedTempFile::new().unwrap();
    let filter_policy = Arc::new(BloomFilterPolicy::new(10));

    // Build table with filter
    {
        let mut builder =
            TableBuilder::new_with_filter(temp_file.path(), Some(filter_policy.clone())).unwrap();

        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish(CompressionType::None).unwrap();
    }

    // Read table with filter
    {
        let mut reader =
            TableReader::open_with_filter(temp_file.path(), 1, None, Some(filter_policy)).unwrap();

        // Keys that exist should be found
        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert!(value.is_some(), "Key {key} should exist");
        }

        // Keys that don't exist should mostly be filtered out
        // (may have false positives, but we count them)
        let mut false_positives = 0;
        for i in 100..1000 {
            let key = format!("key{i:04}");
            if reader.get(&Slice::from(key.as_str())).unwrap().is_some() {
                false_positives += 1;
            }
        }

        // With 10 bits per key, false positive rate should be around 1%
        // For 900 tests, expect < 5% false positives (45)
        assert!(
            false_positives < 45,
            "Too many false positives: {}/900 ({:.2}%)",
            false_positives,
            false_positives as f64 / 9.0
        );
        println!(
            "Bloom filter false positive rate: {:.2}% ({}/900)",
            false_positives as f64 / 9.0,
            false_positives
        );
    }
}

#[test]
fn test_sstable_without_filter() {
    let temp_file = NamedTempFile::new().unwrap();

    // Build table without filter
    {
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for i in 0..50 {
            let key = format!("test{i:03}");
            let value = format!("value{i:03}");
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish(CompressionType::None).unwrap();
    }

    // Read table without filter
    {
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        // Should still work without filter
        for i in 0..50 {
            let key = format!("test{i:03}");
            let expected_value = format!("value{i:03}");
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(value, Some(Slice::from(expected_value)));
        }

        // Non-existent keys
        assert_eq!(reader.get(&Slice::from("nonexistent")).unwrap(), None);
    }
}

#[test]
fn test_filter_effectiveness() {
    let temp_file = NamedTempFile::new().unwrap();
    let filter_policy = Arc::new(BloomFilterPolicy::new(20)); // Higher bits = lower false positive rate

    // Build table with many keys
    {
        let mut builder =
            TableBuilder::new_with_filter(temp_file.path(), Some(filter_policy.clone())).unwrap();

        for i in 0..1000 {
            let key = format!("present{i:05}");
            let value = format!("val{i:05}");
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish(CompressionType::None).unwrap();
    }

    // Test filter effectiveness
    {
        let mut reader =
            TableReader::open_with_filter(temp_file.path(), 1, None, Some(filter_policy)).unwrap();

        // All present keys should be found
        for i in 0..1000 {
            let key = format!("present{i:05}");
            let value = reader.get(&Slice::from(key.as_str())).unwrap();
            assert!(value.is_some());
        }

        // Count false positives for absent keys
        let mut false_positives = 0;
        for i in 0..5000 {
            let key = format!("absent{i:05}");
            if reader.get(&Slice::from(key.as_str())).unwrap().is_some() {
                false_positives += 1;
            }
        }

        // With 20 bits per key, false positive rate should be around 0.05%
        // For 5000 tests, expect < 1% false positives (50)
        assert!(
            false_positives < 50,
            "Too many false positives: {}/5000 ({:.3}%)",
            false_positives,
            false_positives as f64 / 50.0
        );
        println!(
            "Filter effectiveness - FP rate: {:.3}% ({}/5000)",
            false_positives as f64 / 50.0,
            false_positives
        );
    }
}
