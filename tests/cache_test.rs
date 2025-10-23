use rucksdb::{DBOptions, ReadOptions, Slice, WriteOptions, DB};
use tempfile::TempDir;

#[test]
fn test_block_cache_hit_rate() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create DB with small write buffer to trigger flush
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024, // 1KB to trigger flush quickly
        block_cache_size: 100,   // Small cache to test eviction
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write enough data to trigger flush and create SSTables
    for i in 0..200 {
        let key = format!("key{i:04}");
        let value = format!("value{i:04}_padding");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // First read - will miss cache
    let stats_before = db.cache_stats();
    assert_eq!(stats_before.hits, 0);
    assert_eq!(stats_before.misses, 0);

    // Read some keys
    for i in 0..50 {
        let key = format!("key{i:04}");
        let _ = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
    }

    // Check cache stats - should have some misses
    let stats_after_first = db.cache_stats();
    assert!(
        stats_after_first.misses > 0,
        "Should have cache misses on first read"
    );
    println!(
        "After first read - hits: {}, misses: {}",
        stats_after_first.hits, stats_after_first.misses
    );

    // Read same keys again - should hit cache
    for i in 0..50 {
        let key = format!("key{i:04}");
        let _ = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
    }

    // Check cache stats - should have more hits
    let stats_after_second = db.cache_stats();
    let new_hits = stats_after_second.hits - stats_after_first.hits;
    assert!(new_hits > 0, "Should have cache hits on second read");
    println!(
        "After second read - hits: {}, misses: {}",
        stats_after_second.hits, stats_after_second.misses
    );
    println!(
        "Cache hit rate: {:.2}%",
        stats_after_second.hit_rate() * 100.0
    );

    // Verify hit rate is reasonable
    assert!(
        stats_after_second.hit_rate() > 0.1,
        "Hit rate should be > 10%"
    );
}

#[test]
fn test_cache_eviction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create DB with very small cache
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024,
        block_cache_size: 5, // Very small cache - only 5 blocks
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write data to trigger flush
    for i in 0..100 {
        let key = format!("key{i:04}");
        let value = format!("value{i:04}_padding");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Read all keys to populate cache
    for i in 0..100 {
        let key = format!("key{i:04}");
        let _ = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
    }

    // Check that cache size doesn't exceed capacity
    let stats = db.cache_stats();
    assert!(
        stats.entries <= stats.capacity,
        "Cache should not exceed capacity"
    );
    println!("Cache entries: {}/{}", stats.entries, stats.capacity);
}

#[test]
fn test_cache_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create DB with cache size 0 (disabled)
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 1024,
        block_cache_size: 0,
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Write and read data
    for i in 0..50 {
        let key = format!("key{i:04}");
        let value = format!("value{i:04}");
        db.put(
            &WriteOptions::default(),
            Slice::from(key.clone()),
            Slice::from(value.clone()),
        )
        .unwrap();
    }

    for i in 0..50 {
        let key = format!("key{i:04}");
        let expected_value = format!("value{i:04}");
        let value = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert_eq!(value, Some(Slice::from(expected_value)));
    }

    // Cache should be empty since capacity is 0
    let stats = db.cache_stats();
    assert_eq!(stats.capacity, 0);
}
