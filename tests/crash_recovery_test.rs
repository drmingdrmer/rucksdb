use std::fs;

use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

/// Crash recovery tests to ensure durability guarantees
///
/// These tests simulate various crash scenarios to verify that:
/// 1. WAL (Write-Ahead Log) correctly recovers data
/// 2. Data is not lost or corrupted after crashes
/// 3. Database can reopen and recover after failures
///
///    Test basic crash recovery: write data, close DB, reopen and verify
#[test]
fn test_crash_recovery_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    // Write some data
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // DB dropped here - simulates clean shutdown
    }

    // Reopen and verify all data exists
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        for i in 0..100 {
            let key = format!("key{i:04}");
            let expected = format!("value{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value,
                Some(Slice::from(expected.as_str())),
                "Key {key} should exist after recovery"
            );
        }
    }
}

/// Test recovery with data in both MemTable and SSTables
#[test]
fn test_crash_recovery_memtable_and_sstable() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    let options = DBOptions {
        write_buffer_size: 1024, // Small buffer to trigger flush
        ..Default::default()
    };

    // Write enough data to trigger flush
    {
        let db = DB::open(db_path.to_str().unwrap(), options.clone()).unwrap();

        // Write data that will be flushed to SSTables
        for i in 0..50 {
            let key = format!("flushed_key{i:04}");
            let value = "x".repeat(100); // Large values to trigger flush
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // Write data that stays in MemTable/WAL
        for i in 0..20 {
            let key = format!("memtable_key{i:04}");
            let value = format!("memtable_value{i:04}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // DB dropped - simulates crash
    }

    // Reopen and verify all data
    {
        let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

        // Verify flushed data
        for i in 0..50 {
            let key = format!("flushed_key{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert!(value.is_some(), "Flushed key {key} should exist");
        }

        // Verify MemTable data recovered from WAL
        for i in 0..20 {
            let key = format!("memtable_key{i:04}");
            let expected = format!("memtable_value{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value,
                Some(Slice::from(expected.as_str())),
                "MemTable key {key} should be recovered from WAL"
            );
        }
    }
}

/// Test crash recovery with deletes in WAL
#[test]
fn test_crash_recovery_with_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    // Write data, then delete some, then crash
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Write initial data
        for i in 0..50 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // Delete some keys
        for i in 10..20 {
            let key = format!("key{i:04}");
            db.delete(&WriteOptions::default(), Slice::from(key))
                .unwrap();
        }

        // Overwrite some keys
        for i in 30..40 {
            let key = format!("key{i:04}");
            let value = format!("updated_value{i:04}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // DB dropped - simulates crash
    }

    // Reopen and verify state
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Keys 0-9 should exist with original values
        for i in 0..10 {
            let key = format!("key{i:04}");
            let expected = format!("value{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Keys 10-19 should be deleted
        for i in 10..20 {
            let key = format!("key{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, None, "Deleted key {key} should not exist");
        }

        // Keys 20-29 should exist with original values
        for i in 20..30 {
            let key = format!("key{i:04}");
            let expected = format!("value{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Keys 30-39 should have updated values
        for i in 30..40 {
            let key = format!("key{i:04}");
            let expected = format!("updated_value{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Keys 40-49 should exist with original values
        for i in 40..50 {
            let key = format!("key{i:04}");
            let expected = format!("value{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }
    }
}

/// Test recovery after abrupt termination (without sync)
#[test]
fn test_crash_recovery_no_sync() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    // Write with sync=false, then crash
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        let options = WriteOptions { sync: false };

        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            db.put(&options, Slice::from(key), Slice::from(value))
                .unwrap();
        }

        // DB dropped without explicit sync - OS buffers may not be flushed
        // But WAL should still work since we write to it
    }

    // Reopen - should recover from WAL
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        for i in 0..100 {
            let key = format!("key{i:04}");
            let expected = format!("value{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value,
                Some(Slice::from(expected.as_str())),
                "Key {key} should be recovered from WAL even without sync"
            );
        }
    }
}

/// Test recovery with multiple reopen cycles
#[test]
fn test_crash_recovery_multiple_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    // Cycle 1: Write 0-49
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
        for i in 0..50 {
            let key = format!("key{i:04}");
            let value = format!("value_cycle1_{i:04}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Cycle 2: Reopen, verify, add 50-99
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Verify cycle 1 data
        for i in 0..50 {
            let key = format!("key{i:04}");
            let expected = format!("value_cycle1_{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Add more data
        for i in 50..100 {
            let key = format!("key{i:04}");
            let value = format!("value_cycle2_{i:04}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Cycle 3: Reopen, verify all, update some
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Verify all previous data
        for i in 0..50 {
            let key = format!("key{i:04}");
            let expected = format!("value_cycle1_{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        for i in 50..100 {
            let key = format!("key{i:04}");
            let expected = format!("value_cycle2_{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Update some keys
        for i in 0..25 {
            let key = format!("key{i:04}");
            let value = format!("value_cycle3_{i:04}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Final verification
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Keys 0-24 should have cycle 3 values
        for i in 0..25 {
            let key = format!("key{i:04}");
            let expected = format!("value_cycle3_{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Keys 25-49 should have cycle 1 values
        for i in 25..50 {
            let key = format!("key{i:04}");
            let expected = format!("value_cycle1_{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Keys 50-99 should have cycle 2 values
        for i in 50..100 {
            let key = format!("key{i:04}");
            let expected = format!("value_cycle2_{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }
    }
}

/// Test recovery with corrupted/missing WAL file
#[test]
fn test_crash_recovery_missing_wal() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    let options = DBOptions {
        write_buffer_size: 1024, // Small buffer to trigger flush
        ..Default::default()
    };

    // Write data that will be flushed
    {
        let db = DB::open(db_path.to_str().unwrap(), options.clone()).unwrap();

        for i in 0..50 {
            let key = format!("key{i:04}");
            let value = "x".repeat(100); // Large values to trigger flush
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Simulate WAL corruption by deleting it
    let wal_path = db_path.join("WAL");
    if wal_path.exists() {
        fs::remove_file(&wal_path).unwrap();
    }

    // Reopen - should recover from SSTables even without WAL
    {
        let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

        for i in 0..50 {
            let key = format!("key{i:04}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert!(
                value.is_some(),
                "Key {key} should exist from SSTables even without WAL"
            );
        }
    }
}

/// Test recovery with empty database
#[test]
fn test_crash_recovery_empty_db() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    // Create and close empty DB
    {
        let _db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
        // Immediately dropped
    }

    // Reopen empty DB
    {
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Try to read - should get None
        let value = db
            .get(&ReadOptions::default(), &Slice::from("any_key"))
            .unwrap();
        assert_eq!(value, None);

        // Should be able to write
        db.put(
            &WriteOptions::default(),
            Slice::from("new_key"),
            Slice::from("new_value"),
        )
        .unwrap();

        let value = db
            .get(&ReadOptions::default(), &Slice::from("new_key"))
            .unwrap();
        assert_eq!(value, Some(Slice::from("new_value")));
    }
}

/// Test large-scale crash recovery
#[test]
fn test_crash_recovery_large_scale() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_db");

    let options = DBOptions {
        write_buffer_size: 512 * 1024, // 512KB
        ..Default::default()
    };

    // Write large amount of data
    {
        let db = DB::open(db_path.to_str().unwrap(), options.clone()).unwrap();

        for i in 0..1000 {
            let key = format!("key{i:06}");
            let value = format!("value{i:06}_with_extra_data_to_make_it_larger");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // Some deletes
        for i in 100..150 {
            let key = format!("key{i:06}");
            db.delete(&WriteOptions::default(), Slice::from(key))
                .unwrap();
        }

        // Some updates
        for i in 500..600 {
            let key = format!("key{i:06}");
            let value = format!("updated_value{i:06}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }
    }

    // Reopen and verify
    {
        let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

        // Verify non-deleted, non-updated keys
        for i in 0..100 {
            let key = format!("key{i:06}");
            let expected = format!("value{i:06}_with_extra_data_to_make_it_larger");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }

        // Verify deleted keys
        for i in 100..150 {
            let key = format!("key{i:06}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, None);
        }

        // Verify updated keys
        for i in 500..600 {
            let key = format!("key{i:06}");
            let expected = format!("updated_value{i:06}");
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected.as_str())));
        }
    }
}
