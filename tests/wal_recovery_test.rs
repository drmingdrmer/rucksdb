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

    // NOTE: Cross-restart test below verifies this works with MANIFEST
    // persistence
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

// ============================================================================
// Cross-restart multi-CF tests (MANIFEST CF persistence + WAL recovery)
// ============================================================================

#[test]
fn test_cross_restart_multi_cf_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Create CFs and write data
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Create two CFs
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

        // Write to CF1
        db.put_cf(
            &WriteOptions::default(),
            &cf1,
            Slice::from("cf1_key1"),
            Slice::from("cf1_value1"),
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

        // Simulate crash - drop DB without clean shutdown
    }

    // Phase 2: Reopen DB and verify CFs and data are recovered
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Verify CFs exist
        let cfs = db.list_column_families();
        assert_eq!(cfs.len(), 3);

        let cf_names: Vec<&str> = cfs.iter().map(|h| h.name()).collect();
        assert!(cf_names.contains(&"default"));
        assert!(cf_names.contains(&"cf1"));
        assert!(cf_names.contains(&"cf2"));

        // Get CF handles
        let cf1 = cfs.iter().find(|h| h.name() == "cf1").unwrap();
        let cf2 = cfs.iter().find(|h| h.name() == "cf2").unwrap();

        // Verify data in default CF
        let val = db
            .get(&ReadOptions::default(), &Slice::from("default_key1"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("default_value1")));

        // Verify data in CF1
        let val = db
            .get_cf(&ReadOptions::default(), cf1, &Slice::from("cf1_key1"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("cf1_value1")));

        // Verify data in CF2
        let val = db
            .get_cf(&ReadOptions::default(), cf2, &Slice::from("cf2_key1"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("cf2_value1")));
    }
}

#[test]
fn test_cross_restart_multi_cf_with_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Create CFs, write and delete data
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let cf1 = db
            .create_column_family("cf1", ColumnFamilyOptions::default())
            .unwrap();

        // Write to default CF
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
        db.delete_cf(&WriteOptions::default(), &cf1, Slice::from("cf1_key1"))
            .unwrap();
    }

    // Phase 2: Reopen and verify deletions are preserved
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Get CF1 handle
        let cfs = db.list_column_families();
        let cf1 = cfs.iter().find(|h| h.name() == "cf1").unwrap();

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
            .get_cf(&ReadOptions::default(), cf1, &Slice::from("cf1_key1"))
            .unwrap();
        assert_eq!(val, None);
        let val = db
            .get_cf(&ReadOptions::default(), cf1, &Slice::from("cf1_key2"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("cf1_value2")));
    }
}

#[test]
fn test_cross_restart_multi_cf_interleaved() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Create CFs and interleave writes
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let cf1 = db
            .create_column_family("cf1", ColumnFamilyOptions::default())
            .unwrap();
        let cf2 = db
            .create_column_family("cf2", ColumnFamilyOptions::default())
            .unwrap();

        // Interleave writes across CFs
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
    }

    // Phase 2: Reopen and verify all data
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Get CF handles
        let cfs = db.list_column_families();
        let cf1 = cfs.iter().find(|h| h.name() == "cf1").unwrap();
        let cf2 = cfs.iter().find(|h| h.name() == "cf2").unwrap();

        // Verify all data
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
                        .get_cf(&ReadOptions::default(), cf1, &Slice::from(key.as_str()))
                        .unwrap();
                    assert_eq!(val, Some(Slice::from(expected_value)));
                },
                2 => {
                    let val = db
                        .get_cf(&ReadOptions::default(), cf2, &Slice::from(key.as_str()))
                        .unwrap();
                    assert_eq!(val, Some(Slice::from(expected_value)));
                },
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn test_cross_restart_cf_drop_and_create() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Create CF1, write data, drop it, create CF2
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Create CF1 and write data
        let cf1 = db
            .create_column_family("cf1", ColumnFamilyOptions::default())
            .unwrap();
        db.put_cf(
            &WriteOptions::default(),
            &cf1,
            Slice::from("cf1_key"),
            Slice::from("cf1_value"),
        )
        .unwrap();

        // Drop CF1
        db.drop_column_family(&cf1).unwrap();

        // Create CF2
        let cf2 = db
            .create_column_family("cf2", ColumnFamilyOptions::default())
            .unwrap();
        db.put_cf(
            &WriteOptions::default(),
            &cf2,
            Slice::from("cf2_key"),
            Slice::from("cf2_value"),
        )
        .unwrap();
    }

    // Phase 2: Reopen and verify CF1 is gone, CF2 exists
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let cfs = db.list_column_families();
        assert_eq!(cfs.len(), 2); // default + cf2

        let cf_names: Vec<&str> = cfs.iter().map(|h| h.name()).collect();
        assert!(cf_names.contains(&"default"));
        assert!(cf_names.contains(&"cf2"));
        assert!(!cf_names.contains(&"cf1"));

        // Verify CF2 data
        let cf2 = cfs.iter().find(|h| h.name() == "cf2").unwrap();
        let val = db
            .get_cf(&ReadOptions::default(), cf2, &Slice::from("cf2_key"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("cf2_value")));
    }
}

#[test]
fn test_cross_restart_multi_cf_many_keys() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Phase 1: Create multiple CFs and write many keys
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let cf1 = db
            .create_column_family("cf1", ColumnFamilyOptions::default())
            .unwrap();
        let cf2 = db
            .create_column_family("cf2", ColumnFamilyOptions::default())
            .unwrap();
        let cf3 = db
            .create_column_family("cf3", ColumnFamilyOptions::default())
            .unwrap();

        // Write 1000 keys to each CF
        for i in 0..1000 {
            let key = format!("key{i}");
            let value = format!("value{i}");

            db.put(
                &WriteOptions::default(),
                Slice::from(key.clone()),
                Slice::from(value.clone()),
            )
            .unwrap();
            db.put_cf(
                &WriteOptions::default(),
                &cf1,
                Slice::from(key.clone()),
                Slice::from(value.clone()),
            )
            .unwrap();
            db.put_cf(
                &WriteOptions::default(),
                &cf2,
                Slice::from(key.clone()),
                Slice::from(value.clone()),
            )
            .unwrap();
            db.put_cf(
                &WriteOptions::default(),
                &cf3,
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Phase 2: Reopen and verify all keys in all CFs
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let cfs = db.list_column_families();
        assert_eq!(cfs.len(), 4); // default + cf1 + cf2 + cf3

        let cf1 = cfs.iter().find(|h| h.name() == "cf1").unwrap();
        let cf2 = cfs.iter().find(|h| h.name() == "cf2").unwrap();
        let cf3 = cfs.iter().find(|h| h.name() == "cf3").unwrap();

        for i in 0..1000 {
            let key = format!("key{i}");
            let expected_value = format!("value{i}");

            // Verify default CF
            let val = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(val, Some(Slice::from(expected_value.as_str())));

            // Verify CF1
            let val = db
                .get_cf(&ReadOptions::default(), cf1, &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(val, Some(Slice::from(expected_value.as_str())));

            // Verify CF2
            let val = db
                .get_cf(&ReadOptions::default(), cf2, &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(val, Some(Slice::from(expected_value.as_str())));

            // Verify CF3
            let val = db
                .get_cf(&ReadOptions::default(), cf3, &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(val, Some(Slice::from(expected_value.as_str())));
        }
    }
}

#[test]
fn test_cross_restart_cf_id_preservation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let cf1_id;
    let cf2_id;

    // Phase 1: Create CFs and record their IDs
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let cf1 = db
            .create_column_family("cf1", ColumnFamilyOptions::default())
            .unwrap();
        let cf2 = db
            .create_column_family("cf2", ColumnFamilyOptions::default())
            .unwrap();

        cf1_id = cf1.id();
        cf2_id = cf2.id();

        db.put_cf(
            &WriteOptions::default(),
            &cf1,
            Slice::from("key"),
            Slice::from("value"),
        )
        .unwrap();
    }

    // Phase 2: Reopen and verify CF IDs are preserved
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let cfs = db.list_column_families();
        let cf1 = cfs.iter().find(|h| h.name() == "cf1").unwrap();
        let cf2 = cfs.iter().find(|h| h.name() == "cf2").unwrap();

        assert_eq!(cf1.id(), cf1_id);
        assert_eq!(cf2.id(), cf2_id);

        // Verify data is accessible
        let val = db
            .get_cf(&ReadOptions::default(), cf1, &Slice::from("key"))
            .unwrap();
        assert_eq!(val, Some(Slice::from("value")));
    }
}
