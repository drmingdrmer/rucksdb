use rucksdb::{Checkpoint, DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

/// Performance analysis workload with statistics collection
#[test]
fn test_performance_analysis_mixed_workload() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("perf_db");

    // Configure for realistic performance testing
    let options = DBOptions {
        write_buffer_size: 4 * 1024 * 1024, // 4MB - default
        block_cache_size: 1000,             // Cache 1000 blocks
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();
    let stats = db.statistics();

    println!("\n=== Performance Analysis: Mixed Workload ===\n");

    // Phase 1: Sequential writes (simulates initial data load)
    println!("Phase 1: Sequential Write Test (10,000 keys)");
    let num_keys = 10_000;
    for i in 0..num_keys {
        let key = format!("key{i:08}");
        let value = format!("value{i:08}_with_some_padding_data");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    println!("  Keys written: {}", stats.num_keys_written());
    println!(
        "  Bytes written: {} ({:.2} MB)",
        stats.bytes_written(),
        stats.bytes_written() as f64 / 1024.0 / 1024.0
    );
    println!(
        "  WAL writes: {}",
        stats.wal_writes.load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "  MemTable flushes: {}",
        stats
            .num_memtable_flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    // Phase 2: Random reads (hot cache scenario)
    println!("\nPhase 2: Random Read Test - Hot Cache (5,000 reads)");
    stats.reset(); // Reset to measure read performance

    for i in 0..5_000 {
        let key_id = (i * 7) % num_keys; // Pseudo-random access
        let key = format!("key{key_id:08}");
        let result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert!(result.is_some());
    }

    println!("  Keys read: {}", stats.num_keys_read());
    println!(
        "  Bytes read: {} ({:.2} MB)",
        stats.bytes_read(),
        stats.bytes_read() as f64 / 1024.0 / 1024.0
    );
    println!(
        "  MemTable hit rate: {:.2}%",
        stats.memtable_hit_rate() * 100.0
    );
    println!(
        "  SSTable reads: {}",
        stats
            .sstable_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "  SSTable hit rate: {:.2}%",
        stats.sstable_hit_rate() * 100.0
    );

    // Phase 3: Mixed workload (reads + writes + deletes)
    println!("\nPhase 3: Mixed Workload (2,000 writes, 3,000 reads, 500 deletes)");
    stats.reset();

    // Writes
    for i in num_keys..(num_keys + 2_000) {
        let key = format!("key{i:08}");
        let value = format!("value{i:08}_new_data");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    // Reads
    for i in 0..3_000 {
        let key_id = (i * 11) % (num_keys + 2_000);
        let key = format!("key{key_id:08}");
        let _result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
    }

    // Deletes
    for i in 0..500 {
        let key = format!("key{i:08}");
        db.delete(&WriteOptions::default(), Slice::from(key))
            .unwrap();
    }

    println!("  Keys written: {}", stats.num_keys_written());
    println!("  Keys read: {}", stats.num_keys_read());
    println!("  Keys deleted: {}", stats.num_keys_deleted());
    println!(
        "  MemTable hit rate: {:.2}%",
        stats.memtable_hit_rate() * 100.0
    );

    // Phase 4: Full statistics report
    println!("\n=== Complete Statistics Report ===");
    println!("{}", stats.report());

    // Assertions to validate expected behavior
    assert!(stats.num_keys_written() > 0, "Should have written keys");
    assert!(stats.num_keys_read() > 0, "Should have read keys");
    assert!(stats.num_keys_deleted() > 0, "Should have deleted keys");
    assert!(
        stats.memtable_hit_rate() > 0.0,
        "Should have some MemTable hits"
    );
}

#[test]
fn test_performance_analysis_flush_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("flush_perf_db");

    // Small write buffer to trigger multiple flushes
    let options = DBOptions {
        write_buffer_size: 256 * 1024, // 256KB - will trigger flushes
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();
    let stats = db.statistics();

    println!("\n=== Performance Analysis: Flush Behavior ===\n");

    // Write enough data to trigger multiple flushes
    let num_keys = 5_000;
    let value_size = 100; // bytes
    println!("Writing {} keys with ~{} byte values", num_keys, value_size);

    for i in 0..num_keys {
        let key = format!("key{i:08}");
        let value = "x".repeat(value_size);
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    let flushes = stats
        .num_memtable_flushes
        .load(std::sync::atomic::Ordering::Relaxed);
    let bytes_flushed = stats
        .bytes_flushed
        .load(std::sync::atomic::Ordering::Relaxed);

    println!("\nFlush Statistics:");
    println!("  Total keys written: {}", stats.num_keys_written());
    println!(
        "  Total bytes written: {} ({:.2} MB)",
        stats.bytes_written(),
        stats.bytes_written() as f64 / 1024.0 / 1024.0
    );
    println!("  MemTable flushes: {}", flushes);
    println!(
        "  Bytes flushed: {} ({:.2} MB)",
        bytes_flushed,
        bytes_flushed as f64 / 1024.0 / 1024.0
    );

    if flushes > 0 {
        println!(
            "  Average flush size: {:.2} KB",
            (bytes_flushed as f64 / flushes as f64) / 1024.0
        );
    }

    assert!(
        flushes > 0,
        "Should have triggered at least one flush with 256KB buffer"
    );
}

#[test]
fn test_performance_analysis_checkpoint_overhead() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("checkpoint_perf_db");
    let checkpoint_path = temp_dir.path().join("checkpoint");

    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
    let stats = db.statistics();

    println!("\n=== Performance Analysis: Checkpoint Overhead ===\n");

    // Write initial data
    let num_keys = 1_000;
    for i in 0..num_keys {
        let key = format!("key{i:06}");
        let value = format!("value{i:06}");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    println!("Initial data: {} keys written", num_keys);

    // Measure checkpoint creation
    use std::time::Instant;
    let start = Instant::now();
    Checkpoint::create(&db, &checkpoint_path).unwrap();
    let checkpoint_duration = start.elapsed();

    println!(
        "Checkpoint creation time: {:.2}ms",
        checkpoint_duration.as_secs_f64() * 1000.0
    );
    println!(
        "MemTable flushes during checkpoint: {}",
        stats
            .num_memtable_flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    // Verify checkpoint can be opened
    let checkpoint_db = DB::open(checkpoint_path.to_str().unwrap(), DBOptions::default()).unwrap();
    let checkpoint_stats = checkpoint_db.statistics();

    // Read from checkpoint
    for i in 0..num_keys {
        let key = format!("key{i:06}");
        let result = checkpoint_db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
        assert!(result.is_some(), "Key should exist in checkpoint");
    }

    println!(
        "Checkpoint validation: {} keys read successfully",
        checkpoint_stats.num_keys_read()
    );
    println!(
        "Checkpoint MemTable hit rate: {:.2}%",
        checkpoint_stats.memtable_hit_rate() * 100.0
    );
}

#[test]
fn test_performance_analysis_cache_effectiveness() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("cache_perf_db");

    // Configure with specific cache size
    let options = DBOptions {
        write_buffer_size: 512 * 1024, // 512KB
        block_cache_size: 100,         // Small cache to test eviction
        ..Default::default()
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();
    let stats = db.statistics();

    println!("\n=== Performance Analysis: Cache Effectiveness ===\n");

    // Write data to force flush to SSTables
    let num_keys = 2_000;
    for i in 0..num_keys {
        let key = format!("key{i:06}");
        let value = "x".repeat(200); // Force larger SSTables
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    println!("Data written: {} keys", num_keys);
    println!(
        "MemTable flushes: {}",
        stats
            .num_memtable_flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    // Reset stats for read test
    stats.reset();

    // Sequential scan (should have poor cache hit rate due to small cache)
    println!("\nSequential scan:");
    for i in 0..num_keys {
        let key = format!("key{i:06}");
        let _result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();
    }

    println!("  Keys read: {}", stats.num_keys_read());
    println!(
        "  MemTable hit rate: {:.2}%",
        stats.memtable_hit_rate() * 100.0
    );
    println!(
        "  SSTable reads: {}",
        stats
            .sstable_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    // Get cache statistics
    let cache_stats = db.cache_stats();
    println!("\nBlock Cache Statistics:");
    println!("  Hits: {}", cache_stats.hits);
    println!("  Misses: {}", cache_stats.misses);
    println!("  Hit rate: {:.2}%", cache_stats.hit_rate() * 100.0);
    println!(
        "  Entries: {} / {} blocks",
        cache_stats.entries, cache_stats.capacity
    );

    stats.reset();

    // Repeated access to same keys (should have better cache hit rate)
    println!("\nRepeated access (reading same 100 keys 10 times):");
    for _ in 0..10 {
        for i in 0..100 {
            let key = format!("key{i:06}");
            let _result = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
        }
    }

    println!("  Keys read: {}", stats.num_keys_read());
    println!(
        "  MemTable hit rate: {:.2}%",
        stats.memtable_hit_rate() * 100.0
    );

    let cache_stats2 = db.cache_stats();
    println!("  Cache hit rate: {:.2}%", cache_stats2.hit_rate() * 100.0);
}
