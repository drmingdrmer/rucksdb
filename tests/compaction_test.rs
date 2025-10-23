use rucksdb::{DBOptions, ReadOptions, Slice, WriteOptions, DB};
use tempfile::TempDir;

#[test]
fn test_manual_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create DB with small write buffer to trigger multiple flushes
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024, // 1KB to trigger flush quickly
        block_cache_size: 1000,
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write multiple batches to create several Level 0 files
    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("key{:04}", batch * 100 + i);
            let value = format!("value{i:04}_batch{batch}_padding");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Manually trigger compaction
    db.maybe_compact().unwrap();

    // Verify all keys are still readable
    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("key{:04}", batch * 100 + i);
            let expected_value = format!("value{i:04}_batch{batch}_padding");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value,
                Some(Slice::from(expected_value)),
                "Failed for key: {key}"
            );
        }
    }
}

#[test]
fn test_compaction_with_overwrites() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024,
        block_cache_size: 1000,
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write initial values
    for i in 0..50 {
        let key = format!("key{i:04}");
        let value = format!("old_value{i:04}_with_padding");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Overwrite some keys
    for i in 0..25 {
        let key = format!("key{i:04}");
        let value = format!("new_value{i:04}_updated");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Compact
    db.maybe_compact().unwrap();

    // Verify latest values are visible
    for i in 0..25 {
        let key = format!("key{i:04}");
        let expected_value = format!("new_value{i:04}_updated");
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(
            value,
            Some(Slice::from(expected_value)),
            "Failed for key: {key}"
        );
    }

    for i in 25..50 {
        let key = format!("key{i:04}");
        let expected_value = format!("old_value{i:04}_with_padding");
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(
            value,
            Some(Slice::from(expected_value)),
            "Failed for key: {key}"
        );
    }
}

#[test]
fn test_compaction_with_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024,
        block_cache_size: 1000,
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write values
    for i in 0..50 {
        let key = format!("key{i:04}");
        let value = format!("value{i:04}_with_padding_text");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Delete some keys
    for i in 0..20 {
        let key = format!("key{i:04}");
        db.delete(&WriteOptions::default(), Slice::from(key))
            .unwrap();
    }

    // Compact
    db.maybe_compact().unwrap();

    // Verify deleted keys are gone
    for i in 0..20 {
        let key = format!("key{i:04}");
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(value, None, "Key {key} should be deleted");
    }

    // Verify non-deleted keys exist
    for i in 20..50 {
        let key = format!("key{i:04}");
        let expected_value = format!("value{i:04}_with_padding_text");
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(
            value,
            Some(Slice::from(expected_value)),
            "Failed for key: {key}"
        );
    }
}
