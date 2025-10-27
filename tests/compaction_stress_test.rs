use std::{sync::Arc, thread, time::Duration};

use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

#[test]
fn test_compaction_under_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Small write buffer to trigger frequent flushes
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 2 * 1024, // 2KB - triggers many flushes
        block_cache_size: 1000,
        table_cache_size: 100,
        enable_background_compaction: false, // Disable automatic compaction for controlled testing
        ..Default::default()
    };

    let db = Arc::new(DB::open(db_path.to_str().unwrap(), options).unwrap());

    // Spawn multiple writer threads
    let mut handles = vec![];
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("key_t{}_i{:04}", thread_id, i);
                let value = format!("value_thread{}_iteration{}_padding_data", thread_id, i);
                db_clone
                    .put(
                        &WriteOptions::default(),
                        Slice::from(key),
                        Slice::from(value),
                    )
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all writers
    for handle in handles {
        handle.join().unwrap();
    }

    // Trigger compaction after all writes complete
    db.maybe_compact().unwrap();

    // Verify all keys are present
    for thread_id in 0..4 {
        for i in 0..100 {
            let key = format!("key_t{}_i{:04}", thread_id, i);
            let expected_value = format!("value_thread{}_iteration{}_padding_data", thread_id, i);
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value.as_ref().map(|v| v.to_string()),
                Some(expected_value),
                "Missing or incorrect value for key: {}",
                key
            );
        }
    }
}

#[test]
fn test_large_dataset_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 4 * 1024, // 4KB
        block_cache_size: 2000,
        table_cache_size: 100,
        enable_background_compaction: false, // Disable for controlled testing
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write large dataset to create multiple levels
    for batch in 0..10 {
        for i in 0..200 {
            let key = format!("key_{:06}", batch * 1000 + i);
            let value = format!("value_batch{}_i{:04}_with_more_padding", batch, i);
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // Trigger compaction after each batch
        db.maybe_compact().unwrap();
    }

    // Verify all 2000 keys
    for batch in 0..10 {
        for i in 0..200 {
            let key = format!("key_{:06}", batch * 1000 + i);
            let expected_value = format!("value_batch{}_i{:04}_with_more_padding", batch, i);
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert!(
                value.is_some(),
                "Key not found after large dataset compaction: {}",
                key
            );
            assert_eq!(value.unwrap().to_string(), expected_value);
        }
    }
}

#[test]
fn test_compaction_with_many_overwrites() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 2 * 1024,
        block_cache_size: 1000,
        enable_background_compaction: false, // Disable for controlled testing
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write same keys multiple times
    for version in 0..5 {
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_version{}_{:04}_padding", version, i);
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // Compact after each version
        db.maybe_compact().unwrap();
    }

    // Verify only latest version is visible
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let expected_value = format!("value_version4_{:04}_padding", i); // version 4 is latest
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(
            value.as_ref().map(|v| v.to_string()),
            Some(expected_value),
            "Incorrect value for key: {}",
            key
        );
    }
}

#[test]
fn test_compaction_priority_with_unbalanced_levels() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024, // 1KB - very small to create many files
        block_cache_size: 1000,
        enable_background_compaction: false, // Disable for controlled testing
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Create unbalanced scenario: lots of data at level 0
    for i in 0..500 {
        let key = format!("key_{:06}", i);
        let value = format!("value_{:06}_with_padding_data", i);
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Multiple compactions should balance the levels
    for _ in 0..5 {
        db.maybe_compact().unwrap();
        thread::sleep(Duration::from_millis(10));
    }

    // Verify all keys are still accessible
    for i in 0..500 {
        let key = format!("key_{:06}", i);
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert!(
            value.is_some(),
            "Key lost during compaction balancing: {}",
            key
        );
    }
}

#[test]
fn test_concurrent_reads_during_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 2 * 1024,
        block_cache_size: 1000,
        enable_background_compaction: false, // Disable for controlled testing
        ..Default::default()
    };

    let db = Arc::new(DB::open(db_path.to_str().unwrap(), options).unwrap());

    // Write initial data
    for i in 0..200 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}_padding", i);
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    let db_clone = Arc::clone(&db);
    let compaction_handle = thread::spawn(move || {
        for _ in 0..10 {
            let _ = db_clone.maybe_compact();
            thread::sleep(Duration::from_millis(10));
        }
    });

    // Concurrent readers
    let mut reader_handles = vec![];
    for thread_id in 0..3 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            // Use simple LCG for random-ish access pattern
            let mut x = 123456789u64 + thread_id as u64;
            for _ in 0..50 {
                x = x.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (x as usize) % 200;
                let key = format!("key_{:04}", key_idx);
                let expected_value = format!("value_{:04}_padding", key_idx);
                let value = db_clone
                    .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap();
                assert_eq!(value.as_ref().map(|v| v.to_string()), Some(expected_value));
                thread::sleep(Duration::from_millis(1));
            }
        });
        reader_handles.push(handle);
    }

    // Wait for all operations
    compaction_handle.join().unwrap();
    for handle in reader_handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_compaction_with_mixed_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 2 * 1024,
        block_cache_size: 1000,
        enable_background_compaction: false, // Disable for controlled testing
        ..Default::default()
    };

    let db = Arc::new(DB::open(db_path.to_str().unwrap(), options).unwrap());

    // Initial data
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Mix of operations during compaction
    let db_clone = Arc::clone(&db);
    let writer_handle = thread::spawn(move || {
        // Overwrites
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let value = format!("updated_{:04}", i);
            db_clone
                .put(
                    &WriteOptions::default(),
                    Slice::from(key),
                    Slice::from(value),
                )
                .unwrap();
        }

        // Deletes
        for i in 50..75 {
            let key = format!("key_{:04}", i);
            db_clone
                .delete(&WriteOptions::default(), Slice::from(key))
                .unwrap();
        }

        // New keys
        for i in 100..150 {
            let key = format!("key_{:04}", i);
            let value = format!("new_{:04}", i);
            db_clone
                .put(
                    &WriteOptions::default(),
                    Slice::from(key),
                    Slice::from(value),
                )
                .unwrap();
        }
    });

    // Trigger compactions
    for _ in 0..5 {
        db.maybe_compact().unwrap();
        thread::sleep(Duration::from_millis(10));
    }

    writer_handle.join().unwrap();

    // Final compaction
    db.maybe_compact().unwrap();

    // Verify final state
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(
            value.as_ref().map(|v| v.to_string()),
            Some(format!("updated_{:04}", i))
        );
    }

    for i in 50..75 {
        let key = format!("key_{:04}", i);
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert!(value.is_none(), "Deleted key still exists: {}", key);
    }

    for i in 75..100 {
        let key = format!("key_{:04}", i);
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(
            value.as_ref().map(|v| v.to_string()),
            Some(format!("value_{:04}", i))
        );
    }

    for i in 100..150 {
        let key = format!("key_{:04}", i);
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(
            value.as_ref().map(|v| v.to_string()),
            Some(format!("new_{:04}", i))
        );
    }
}
