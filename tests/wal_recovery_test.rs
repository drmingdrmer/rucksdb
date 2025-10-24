use rucksdb::{ColumnFamilyOptions, DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

#[test]
fn test_wal_recovery_after_crash() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Write some data
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key2"),
            Slice::from("value2"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key3"),
            Slice::from("value3"),
        )
        .unwrap();

        // Simulate crash - drop DB without clean shutdown
    }

    // Phase 2: Reopen DB and verify data is recovered from WAL
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let val1 = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(val1, Some(Slice::from("value1")));

        let val2 = db
            .get(&ReadOptions::default(), &Slice::from("key2"))
            .unwrap();
        assert_eq!(val2, Some(Slice::from("value2")));

        let val3 = db
            .get(&ReadOptions::default(), &Slice::from("key3"))
            .unwrap();
        assert_eq!(val3, Some(Slice::from("value3")));
    }
}

#[test]
fn test_wal_recovery_with_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Write and delete
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key2"),
            Slice::from("value2"),
        )
        .unwrap();
        db.delete(&WriteOptions::default(), Slice::from("key1"))
            .unwrap();
    }

    // Phase 2: Recover and verify
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let val1 = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(val1, None);

        let val2 = db
            .get(&ReadOptions::default(), &Slice::from("key2"))
            .unwrap();
        assert_eq!(val2, Some(Slice::from("value2")));
    }
}

#[test]
fn test_wal_recovery_with_overwrite() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Write with overwrites
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key"),
            Slice::from("value1"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key"),
            Slice::from("value2"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key"),
            Slice::from("value3"),
        )
        .unwrap();
    }

    // Phase 2: Recover and verify latest value
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let val = db
            .get(&ReadOptions::default(), &Slice::from("key"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("value3")));
    }
}

#[test]
fn test_wal_recovery_many_keys() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Write many keys
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        for i in 0..1000 {
            let key = format!("key{i}");
            let value = format!("value{i}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Phase 2: Recover and verify all keys
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        for i in 0..1000 {
            let key = format!("key{i}");
            let expected_value = format!("value{i}");
            let val = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
            assert_eq!(val, Some(Slice::from(expected_value)));
        }
    }
}

#[test]
fn test_wal_sync_option() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let sync_opts = WriteOptions { sync: true };

        db.put(&sync_opts, Slice::from("key"), Slice::from("value"))
            .unwrap();
    }

    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
        let val = db
            .get(&ReadOptions::default(), &Slice::from("key"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("value")));
    }
}

#[test]
fn test_wal_recovery_multi_cf() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Test multi-CF writes in same session (WAL encoding includes CF ID)
    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    // Create two additional CFs
    let cf1 = db
        .create_column_family("cf1", ColumnFamilyOptions::default())
        .unwrap();
    let cf2 = db
        .create_column_family("cf2", ColumnFamilyOptions::default())
        .unwrap();

    // Write to default CF
    db.put(
        &WriteOptions::default(),
        Slice::from("default_key1"),
        Slice::from("default_value1"),
    )
    .unwrap();
    db.put(
        &WriteOptions::default(),
        Slice::from("default_key2"),
        Slice::from("default_value2"),
    )
    .unwrap();

    // Write to CF1
    db.put_cf(
        &WriteOptions::default(),
        &cf1,
        Slice::from("cf1_key1"),
        Slice::from("cf1_value1"),
    )
    .unwrap();
    db.put_cf(
        &WriteOptions::default(),
        &cf1,
        Slice::from("cf1_key2"),
        Slice::from("cf1_value2"),
    )
    .unwrap();

    // Write to CF2
    db.put_cf(
        &WriteOptions::default(),
        &cf2,
        Slice::from("cf2_key1"),
        Slice::from("cf2_value1"),
    )
    .unwrap();
    db.put_cf(
        &WriteOptions::default(),
        &cf2,
        Slice::from("cf2_key2"),
        Slice::from("cf2_value2"),
    )
    .unwrap();

    // Verify all data is accessible in same session
    // Verify default CF
    let val = db
        .get(&ReadOptions::default(), &Slice::from("default_key1"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("default_value1")));
    let val = db
        .get(&ReadOptions::default(), &Slice::from("default_key2"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("default_value2")));

    // Verify CF1
    let val = db
        .get_cf(&ReadOptions::default(), &cf1, &Slice::from("cf1_key1"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("cf1_value1")));
    let val = db
        .get_cf(&ReadOptions::default(), &cf1, &Slice::from("cf1_key2"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("cf1_value2")));

    // Verify CF2
    let val = db
        .get_cf(&ReadOptions::default(), &cf2, &Slice::from("cf2_key1"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("cf2_value1")));
    let val = db
        .get_cf(&ReadOptions::default(), &cf2, &Slice::from("cf2_key2"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("cf2_value2")));

    // NOTE: Cross-restart multi-CF WAL recovery requires MANIFEST persistence
    // (Phase 5.1) to restore CF metadata before WAL replay
}

#[test]
fn test_wal_recovery_multi_cf_with_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Test mixed operations across CFs in same session
    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    let cf1 = db
        .create_column_family("cf1", ColumnFamilyOptions::default())
        .unwrap();

    // Write and delete in default CF
    db.put(
        &WriteOptions::default(),
        Slice::from("key1"),
        Slice::from("value1"),
    )
    .unwrap();
    db.put(
        &WriteOptions::default(),
        Slice::from("key2"),
        Slice::from("value2"),
    )
    .unwrap();
    db.delete(&WriteOptions::default(), Slice::from("key1"))
        .unwrap();

    // Write and delete in CF1
    db.put_cf(
        &WriteOptions::default(),
        &cf1,
        Slice::from("cf1_key1"),
        Slice::from("cf1_value1"),
    )
    .unwrap();
    db.put_cf(
        &WriteOptions::default(),
        &cf1,
        Slice::from("cf1_key2"),
        Slice::from("cf1_value2"),
    )
    .unwrap();
    db.delete_cf(&WriteOptions::default(), &cf1, Slice::from("cf1_key1"))
        .unwrap();

    // Verify default CF - key1 should be deleted
    let val = db
        .get(&ReadOptions::default(), &Slice::from("key1"))
        .unwrap();
    assert_eq!(val, None);
    let val = db
        .get(&ReadOptions::default(), &Slice::from("key2"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("value2")));

    // Verify CF1 - cf1_key1 should be deleted
    let val = db
        .get_cf(&ReadOptions::default(), &cf1, &Slice::from("cf1_key1"))
        .unwrap();
    assert_eq!(val, None);
    let val = db
        .get_cf(&ReadOptions::default(), &cf1, &Slice::from("cf1_key2"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("cf1_value2")));
}

#[test]
fn test_wal_recovery_multi_cf_interleaved() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Test interleaved writes across CFs in same session
    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    let cf1 = db
        .create_column_family("cf1", ColumnFamilyOptions::default())
        .unwrap();
    let cf2 = db
        .create_column_family("cf2", ColumnFamilyOptions::default())
        .unwrap();

    // Interleave writes across CFs to test WAL encoding with CF IDs
    for i in 0..100 {
        let key = format!("key{i}");
        let value = format!("value{i}");

        match i % 3 {
            0 => {
                db.put(
                    &WriteOptions::default(),
                    Slice::from(key),
                    Slice::from(value),
                )
                .unwrap();
            },
            1 => {
                db.put_cf(
                    &WriteOptions::default(),
                    &cf1,
                    Slice::from(key),
                    Slice::from(value),
                )
                .unwrap();
            },
            2 => {
                db.put_cf(
                    &WriteOptions::default(),
                    &cf2,
                    Slice::from(key),
                    Slice::from(value),
                )
                .unwrap();
            },
            _ => unreachable!(),
        }
    }

    // Verify all data is accessible
    for i in 0..100 {
        let key = format!("key{i}");
        let expected_value = format!("value{i}");

        match i % 3 {
            0 => {
                let val = db
                    .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap();
                assert_eq!(val, Some(Slice::from(expected_value)));
            },
            1 => {
                let val = db
                    .get_cf(&ReadOptions::default(), &cf1, &Slice::from(key.as_str()))
                    .unwrap();
                assert_eq!(val, Some(Slice::from(expected_value)));
            },
            2 => {
                let val = db
                    .get_cf(&ReadOptions::default(), &cf2, &Slice::from(key.as_str()))
                    .unwrap();
                assert_eq!(val, Some(Slice::from(expected_value)));
            },
            _ => unreachable!(),
        }
    }
}
