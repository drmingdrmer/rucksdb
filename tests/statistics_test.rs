use rucksdb::{DB, DBOptions, ReadOptions, Slice, Statistics, WriteOptions};
use tempfile::TempDir;

#[test]
fn test_statistics_basic_tracking() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
    let stats = db.statistics();

    // Initially zero
    assert_eq!(stats.num_keys_written(), 0);
    assert_eq!(stats.num_keys_read(), 0);

    // Write some keys - statistics tracked automatically
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

    // Verify write statistics
    assert_eq!(stats.num_keys_written(), 2);
    assert_eq!(stats.bytes_written(), (4 + 6) * 2); // (key1+value1) + (key2+value2)

    // Read keys
    db.get(&ReadOptions::default(), &Slice::from("key1"))
        .unwrap();
    db.get(&ReadOptions::default(), &Slice::from("key2"))
        .unwrap();

    // Verify read statistics - should hit MemTable
    assert_eq!(stats.num_keys_read(), 2);
    assert_eq!(stats.memtable_hit_rate(), 1.0); // 100% hit rate

    // Statistics report contains expected sections
    let report = stats.report();
    assert!(report.contains("Database Statistics"));
    assert!(report.contains("Operations:"));
    assert!(report.contains("MemTable:"));
    assert!(report.contains("WAL:"));
}

#[test]
fn test_statistics_report_format() {
    let stats = Statistics::new();

    // Record some operations manually
    stats.record_write(1024);
    stats.record_write(2048);
    stats.record_read(512);
    stats.record_delete();

    stats.record_memtable_hit();
    stats.record_memtable_hit();
    stats.record_memtable_miss();

    let report = stats.report();

    // Verify report contains expected sections
    assert!(report.contains("Keys written:  2"));
    assert!(report.contains("Keys read:     1"));
    assert!(report.contains("Keys deleted:  1"));
    assert!(report.contains("Bytes written: 3072"));
    assert!(report.contains("Bytes read:    512"));
    assert!(report.contains("Hit rate:      66.67%")); // 2 hits out of 3 total
}

#[test]
fn test_statistics_reset() {
    let stats = Statistics::new();

    stats.record_write(100);
    stats.record_read(50);

    assert!(stats.num_keys_written() > 0);
    assert!(stats.num_keys_read() > 0);

    stats.reset();

    assert_eq!(stats.num_keys_written(), 0);
    assert_eq!(stats.num_keys_read(), 0);
    assert_eq!(stats.bytes_written(), 0);
    assert_eq!(stats.bytes_read(), 0);
}

#[test]
fn test_statistics_automatic_tracking() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
    let stats = db.statistics();

    // Write keys
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

    // Verify write tracking
    assert_eq!(stats.num_keys_written(), 2);
    assert!(stats.bytes_written() > 0);
    assert!(stats.wal_writes.load(std::sync::atomic::Ordering::Relaxed) > 0);

    // Delete a key
    db.delete(&WriteOptions::default(), Slice::from("key1"))
        .unwrap();

    // Verify delete tracking
    assert_eq!(stats.num_keys_deleted(), 1);

    // Read existing key (MemTable hit)
    let result = db
        .get(&ReadOptions::default(), &Slice::from("key2"))
        .unwrap();
    assert!(result.is_some());

    // Read deleted key (MemTable hit but value is None)
    let result = db
        .get(&ReadOptions::default(), &Slice::from("key1"))
        .unwrap();
    assert!(result.is_none());

    // Read non-existent key (MemTable miss)
    let result = db
        .get(&ReadOptions::default(), &Slice::from("key3"))
        .unwrap();
    assert!(result.is_none());

    // Verify MemTable statistics
    assert!(
        stats
            .memtable_hits
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(
        stats
            .memtable_misses
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
}

#[test]
fn test_statistics_concurrent_updates() {
    use std::{sync::Arc, thread};

    let stats = Arc::new(Statistics::new());
    let mut handles = vec![];

    // Spawn 4 threads that each record 1000 writes
    for _ in 0..4 {
        let stats_clone = Arc::clone(&stats);
        let handle = thread::spawn(move || {
            for _ in 0..1000 {
                stats_clone.record_write(100);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have 4000 writes total
    assert_eq!(stats.num_keys_written(), 4000);
    assert_eq!(stats.bytes_written(), 400000);
}
