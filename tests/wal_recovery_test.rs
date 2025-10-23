use rucksdb::{DBOptions, ReadOptions, Slice, WriteOptions, DB};
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

        let val1 = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
        assert_eq!(val1, Some(Slice::from("value1")));

        let val2 = db.get(&ReadOptions::default(), &Slice::from("key2")).unwrap();
        assert_eq!(val2, Some(Slice::from("value2")));

        let val3 = db.get(&ReadOptions::default(), &Slice::from("key3")).unwrap();
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

        let val1 = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
        assert_eq!(val1, None);

        let val2 = db.get(&ReadOptions::default(), &Slice::from("key2")).unwrap();
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

        let val = db.get(&ReadOptions::default(), &Slice::from("key")).unwrap();
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
            let key = format!("key{}", i);
            let value = format!("value{}", i);
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
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            let val = db
                .get(&ReadOptions::default(), &Slice::from(key))
                .unwrap();
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

        let mut sync_opts = WriteOptions::default();
        sync_opts.sync = true;

        db.put(&sync_opts, Slice::from("key"), Slice::from("value"))
            .unwrap();
    }

    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
        let val = db.get(&ReadOptions::default(), &Slice::from("key")).unwrap();
        assert_eq!(val, Some(Slice::from("value")));
    }
}
