use std::{
    sync::{Arc, Barrier},
    thread,
};

use rucksdb::{ColumnFamilyOptions, DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

#[test]
fn test_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = Arc::new(DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap());

    let num_threads = 8;
    let writes_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait(); // Synchronize start

            for i in 0..writes_per_thread {
                let key = format!("thread{}_key{}", thread_id, i);
                let value = format!("thread{}_value{}", thread_id, i);

                db_clone
                    .put(
                        &WriteOptions::default(),
                        Slice::from(key.clone()),
                        Slice::from(value.clone()),
                    )
                    .unwrap();

                // Verify immediately
                let result = db_clone
                    .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap();
                assert_eq!(result, Some(Slice::from(value.as_str())));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes persisted
    for thread_id in 0..num_threads {
        for i in 0..writes_per_thread {
            let key = format!("thread{}_key{}", thread_id, i);
            let expected_value = format!("thread{}_value{}", thread_id, i);

            let result = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(result, Some(Slice::from(expected_value.as_str())));
        }
    }
}

#[test]
fn test_concurrent_reads_and_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = Arc::new(DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap());

    // Pre-populate with some data
    for i in 0..1000 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    let num_readers = 4;
    let num_writers = 4;
    let reads_per_thread = 5000;
    let writes_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));

    let mut handles = vec![];

    // Spawn reader threads
    for _ in 0..num_readers {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for j in 0..reads_per_thread {
                let key_id = j % 2000; // Deterministic access pattern
                let key = format!("key{}", key_id);
                let _result = db_clone
                    .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap();
            }
        });

        handles.push(handle);
    }

    // Spawn writer threads
    for thread_id in 0..num_writers {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..writes_per_thread {
                let key = format!("key{}", 1000 + thread_id * writes_per_thread + i);
                let value = format!("value{}", 1000 + thread_id * writes_per_thread + i);

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

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify new writes
    for thread_id in 0..num_writers {
        for i in 0..writes_per_thread {
            let key = format!("key{}", 1000 + thread_id * writes_per_thread + i);
            let expected_value = format!("value{}", 1000 + thread_id * writes_per_thread + i);

            let result = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(result, Some(Slice::from(expected_value.as_str())));
        }
    }
}

#[test]
fn test_multi_cf_concurrent_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = Arc::new(DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap());

    // Create multiple column families
    let cf1 = db
        .create_column_family("cf1", ColumnFamilyOptions::default())
        .unwrap();
    let cf2 = db
        .create_column_family("cf2", ColumnFamilyOptions::default())
        .unwrap();
    let cf3 = db
        .create_column_family("cf3", ColumnFamilyOptions::default())
        .unwrap();

    let cfs = [cf1, cf2, cf3];
    let num_threads = 6;
    let operations_per_thread = 500;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let cf_clone = cfs[thread_id % 3].clone();
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..operations_per_thread {
                let key = format!("thread{}_key{}", thread_id, i);
                let value = format!("thread{}_value{}", thread_id, i);

                // Write
                db_clone
                    .put_cf(
                        &WriteOptions::default(),
                        &cf_clone,
                        Slice::from(key.clone()),
                        Slice::from(value.clone()),
                    )
                    .unwrap();

                // Read
                let result = db_clone
                    .get_cf(
                        &ReadOptions::default(),
                        &cf_clone,
                        &Slice::from(key.as_str()),
                    )
                    .unwrap();
                assert_eq!(result, Some(Slice::from(value.as_str())));

                // Delete some entries
                if i % 10 == 0 {
                    db_clone
                        .delete_cf(
                            &WriteOptions::default(),
                            &cf_clone,
                            Slice::from(key.clone()),
                        )
                        .unwrap();

                    let result = db_clone
                        .get_cf(
                            &ReadOptions::default(),
                            &cf_clone,
                            &Slice::from(key.as_str()),
                        )
                        .unwrap();
                    assert_eq!(result, None);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify data in each CF
    for (cf_idx, cf) in cfs.iter().enumerate() {
        let mut count = 0;
        for thread_id in (0..num_threads).filter(|&t| t % 3 == cf_idx) {
            for i in 0..operations_per_thread {
                let key = format!("thread{}_key{}", thread_id, i);

                if i % 10 == 0 {
                    // Should be deleted
                    let result = db
                        .get_cf(&ReadOptions::default(), cf, &Slice::from(key.as_str()))
                        .unwrap();
                    assert_eq!(result, None);
                } else {
                    // Should exist
                    let expected_value = format!("thread{}_value{}", thread_id, i);
                    let result = db
                        .get_cf(&ReadOptions::default(), cf, &Slice::from(key.as_str()))
                        .unwrap();
                    assert_eq!(result, Some(Slice::from(expected_value.as_str())));
                    count += 1;
                }
            }
        }
        assert!(count > 0);
    }
}

#[test]
fn test_large_values() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    // Test various large value sizes
    let sizes = vec![1024, 10 * 1024, 100 * 1024, 1024 * 1024]; // 1KB, 10KB, 100KB, 1MB

    for size in sizes {
        let key = format!("large_key_{}", size);
        let value = "x".repeat(size);

        db.put(
            &WriteOptions::default(),
            Slice::from(key.clone()),
            Slice::from(value.clone()),
        )
        .unwrap();

        let result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(result, Some(Slice::from(value.as_str())));
    }
}

#[test]
fn test_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    // Empty value
    db.put(
        &WriteOptions::default(),
        Slice::from("empty_key"),
        Slice::from(""),
    )
    .unwrap();
    let result = db
        .get(&ReadOptions::default(), &Slice::from("empty_key"))
        .unwrap();
    assert_eq!(result, Some(Slice::from("")));

    // Single character key and value
    db.put(&WriteOptions::default(), Slice::from("k"), Slice::from("v"))
        .unwrap();
    let result = db.get(&ReadOptions::default(), &Slice::from("k")).unwrap();
    assert_eq!(result, Some(Slice::from("v")));

    // Very long key
    let long_key = "k".repeat(1000);
    let long_value = "v".repeat(1000);
    db.put(
        &WriteOptions::default(),
        Slice::from(long_key.clone()),
        Slice::from(long_value.clone()),
    )
    .unwrap();
    let result = db
        .get(&ReadOptions::default(), &Slice::from(long_key.as_str()))
        .unwrap();
    assert_eq!(result, Some(Slice::from(long_value.as_str())));

    // Special characters (using byte string)
    let special_key = b"special\x00\x7F\x01";
    db.put(
        &WriteOptions::default(),
        Slice::from(&special_key[..]),
        Slice::from("value"),
    )
    .unwrap();
    let result = db
        .get(&ReadOptions::default(), &Slice::from(&special_key[..]))
        .unwrap();
    assert_eq!(result, Some(Slice::from("value")));
}

#[test]
fn test_repeated_overwrites() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    let key = "overwrite_key";
    let num_overwrites = 10000;

    for i in 0..num_overwrites {
        let value = format!("value{}", i);
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value.clone()),
        )
        .unwrap();

        let result = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(result, Some(Slice::from(value.as_str())));
    }

    // Final value should be the last one
    let result = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
    assert_eq!(
        result,
        Some(Slice::from(format!("value{}", num_overwrites - 1).as_str()))
    );
}

#[test]
fn test_sequential_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    let num_keys = 1000;

    // Write keys
    for i in 0..num_keys {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Delete all keys
    for i in 0..num_keys {
        let key = format!("key{}", i);
        db.delete(&WriteOptions::default(), Slice::from(key.clone()))
            .unwrap();

        let result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(result, None);
    }

    // Verify all deleted
    for i in 0..num_keys {
        let key = format!("key{}", i);
        let result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(result, None);
    }
}

#[test]
fn test_alternating_write_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    let num_iterations = 100;

    for iteration in 0..num_iterations {
        // Write 100 keys
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("iter{}_value{}", iteration, i);
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // Delete half of them
        for i in 0..50 {
            let key = format!("key{}", i);
            db.delete(&WriteOptions::default(), Slice::from(key))
                .unwrap();
        }

        // Verify state
        for i in 0..100 {
            let key = format!("key{}", i);
            let result = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();

            if i < 50 {
                assert_eq!(result, None);
            } else {
                let expected_value = format!("iter{}_value{}", iteration, i);
                assert_eq!(result, Some(Slice::from(expected_value.as_str())));
            }
        }
    }
}
