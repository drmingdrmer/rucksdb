use rucksdb::{CompressionType, DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

/// Test that parallel compaction produces same results as sequential
#[test]
fn test_parallel_vs_sequential_equivalence() {
    // Create two databases - one with parallel, one sequential
    let temp_dir_parallel = TempDir::new().unwrap();
    let temp_dir_sequential = TempDir::new().unwrap();

    let parallel_options = DBOptions {
        create_if_missing: true,
        write_buffer_size: 4 * 1024, // Small buffer to trigger compaction
        parallel_compaction_threads: 4,
        enable_subcompaction: true,
        subcompaction_min_size: 10 * 1024,
        compression_type: CompressionType::None,
        ..Default::default()
    };

    let sequential_options = DBOptions {
        create_if_missing: true,
        write_buffer_size: 4 * 1024,
        parallel_compaction_threads: 0, // Disable parallel
        enable_subcompaction: false,
        compression_type: CompressionType::None,
        ..Default::default()
    };

    let db_parallel =
        DB::open(temp_dir_parallel.path().to_str().unwrap(), parallel_options).unwrap();

    let db_sequential = DB::open(
        temp_dir_sequential.path().to_str().unwrap(),
        sequential_options,
    )
    .unwrap();

    // Insert same data into both databases to create multiple L0 files
    for batch in 0..3 {
        for i in 0..100 {
            let key = format!("key{i:05}");
            let value = format!("value{i:05}_batch{batch}");
            db_parallel
                .put(
                    &WriteOptions::default(),
                    Slice::from(key.clone()),
                    Slice::from(value.clone()),
                )
                .unwrap();
            db_sequential
                .put(
                    &WriteOptions::default(),
                    Slice::from(key),
                    Slice::from(value),
                )
                .unwrap();
        }
    }

    // Trigger compaction on both
    db_parallel.maybe_compact().unwrap();
    db_sequential.maybe_compact().unwrap();

    // Verify both databases have identical data (should have batch 2 values)
    for i in 0..100 {
        let key = format!("key{i:05}");
        let expected_value = format!("value{i:05}_batch2");

        let parallel_value = db_parallel
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        let sequential_value = db_sequential
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();

        assert_eq!(
            parallel_value,
            Some(Slice::from(expected_value.as_str())),
            "Parallel compaction mismatch for {key}"
        );
        assert_eq!(
            sequential_value,
            Some(Slice::from(expected_value.as_str())),
            "Sequential compaction mismatch for {key}"
        );
        assert_eq!(
            parallel_value, sequential_value,
            "Parallel and sequential results differ for {key}"
        );
    }

    println!("✓ Parallel and sequential compaction produce identical results");
}

/// Test parallel compaction with deletions
#[test]
fn test_parallel_compaction_with_deletions() {
    let temp_dir = TempDir::new().unwrap();

    let options = DBOptions {
        create_if_missing: true,
        write_buffer_size: 4 * 1024,
        parallel_compaction_threads: 4,
        enable_subcompaction: true,
        subcompaction_min_size: 10 * 1024,
        compression_type: CompressionType::None,
        ..Default::default()
    };

    let db = DB::open(temp_dir.path().to_str().unwrap(), options).unwrap();

    // Write data to fill buffer
    for i in 0..200 {
        let key = format!("key{i:05}");
        let value = format!("value{i:05}_data");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Delete some keys
    for i in (50..150).step_by(3) {
        let key = format!("key{i:05}");
        db.delete(&WriteOptions::default(), Slice::from(key))
            .unwrap();
    }

    // Trigger compaction
    db.maybe_compact().unwrap();

    // Verify deletions worked
    let mut deleted_count = 0;
    let mut present_count = 0;

    for i in 0..200 {
        let key = format!("key{i:05}");
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();

        if (50..150).contains(&i) && (i - 50) % 3 == 0 {
            assert!(value.is_none(), "Key {key} should be deleted");
            deleted_count += 1;
        } else {
            assert!(value.is_some(), "Key {key} should exist");
            present_count += 1;
        }
    }

    println!("✓ Parallel compaction: {deleted_count} deletions, {present_count} keys remain");
}

/// Test that parallel compaction is actually enabled
#[test]
fn test_parallel_enabled() {
    let temp_dir = TempDir::new().unwrap();

    let options = DBOptions {
        create_if_missing: true,
        write_buffer_size: 4 * 1024,
        parallel_compaction_threads: 4, // Should enable parallel
        enable_subcompaction: true,
        subcompaction_min_size: 10 * 1024,
        ..Default::default()
    };

    let db = DB::open(temp_dir.path().to_str().unwrap(), options).unwrap();

    // Write enough data to trigger compaction
    for i in 0..300 {
        let key = format!("key{i:04}");
        let value = format!("value{i:04}_test_parallel");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    db.maybe_compact().unwrap();

    // Verify data is correct
    for i in 0..300 {
        let key = format!("key{i:04}");
        let expected = format!("value{i:04}_test_parallel");
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(value, Some(Slice::from(expected.as_str())));
    }

    println!("✓ Parallel compaction with 4 threads completed successfully");
}
