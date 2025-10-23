use rucksdb::{DBOptions, ReadOptions, Slice, WriteOptions, DB};
use tempfile::TempDir;

#[test]
fn test_flush_memtable() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create DB with small write buffer to trigger flush
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024, // 1KB to trigger flush quickly
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write enough data to trigger flush (>1KB)
    for i in 0..100 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}_with_padding_to_increase_size", i);
        db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))
            .unwrap();
    }

    // Verify all keys are still readable
    for i in 0..100 {
        let key = format!("key{:04}", i);
        let expected_value = format!("value{:04}_with_padding_to_increase_size", i);
        let value = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(value, Some(Slice::from(expected_value)));
    }
}

#[test]
fn test_flush_and_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create DB and write data that triggers flush
    {
        let options = DBOptions {
            create_if_missing: true,
            error_if_exists: false,
            write_buffer_size: 1024,
        };

        let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}_padding", i);
            db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))
                .unwrap();
        }
    }

    // Reopen DB and verify data is recovered from SSTables
    {
        let options = DBOptions {
            create_if_missing: false,
            error_if_exists: false,
            write_buffer_size: 4 * 1024 * 1024,
        };

        let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected_value = format!("value{:04}_padding", i);
            let value = db.get(&ReadOptions::default(), &Slice::from(key.as_str())).unwrap();
            assert_eq!(value, Some(Slice::from(expected_value)), "Failed for key: {}", key);
        }
    }
}

#[test]
fn test_mixed_memtable_and_sstable() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024,
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write data that gets flushed to SSTable
    for i in 0..50 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}_padding_for_flush", i);
        db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))
            .unwrap();
    }

    // Write more data that stays in MemTable
    for i in 50..60 {
        let key = format!("key{:04}", i);
        let value = format!("small{:04}", i);
        db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))
            .unwrap();
    }

    // Verify all data is readable (from both MemTable and SSTable)
    for i in 0..50 {
        let key = format!("key{:04}", i);
        let expected_value = format!("value{:04}_padding_for_flush", i);
        let value = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(value, Some(Slice::from(expected_value)));
    }

    for i in 50..60 {
        let key = format!("key{:04}", i);
        let expected_value = format!("small{:04}", i);
        let value = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(value, Some(Slice::from(expected_value)));
    }
}

#[test]
fn test_overwrite_across_flush() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024,
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write initial value that gets flushed
    for i in 0..50 {
        let key = format!("key{:04}", i);
        let value = format!("old_value{:04}_with_padding", i);
        db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))
            .unwrap();
    }

    // Overwrite some keys with new values
    for i in 0..10 {
        let key = format!("key{:04}", i);
        let value = format!("new{:04}", i);
        db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))
            .unwrap();
    }

    // Verify overwrites are visible (should read from MemTable first)
    for i in 0..10 {
        let key = format!("key{:04}", i);
        let expected_value = format!("new{:04}", i);
        let value = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(value, Some(Slice::from(expected_value)));
    }

    // Verify old values are still there for non-overwritten keys
    for i in 10..50 {
        let key = format!("key{:04}", i);
        let expected_value = format!("old_value{:04}_with_padding", i);
        let value = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(value, Some(Slice::from(expected_value)));
    }
}

#[test]
fn test_delete_after_flush() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024,
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write data that gets flushed
    for i in 0..50 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}_padding_text", i);
        db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))
            .unwrap();
    }

    // Delete some keys
    for i in 0..10 {
        let key = format!("key{:04}", i);
        db.delete(&WriteOptions::default(), Slice::from(key))
            .unwrap();
    }

    // Verify deleted keys are not found
    for i in 0..10 {
        let key = format!("key{:04}", i);
        let value = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(value, None);
    }

    // Verify non-deleted keys are still there
    for i in 10..50 {
        let key = format!("key{:04}", i);
        let expected_value = format!("value{:04}_padding_text", i);
        let value = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(value, Some(Slice::from(expected_value)));
    }
}
