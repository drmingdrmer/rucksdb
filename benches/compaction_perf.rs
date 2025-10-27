use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rucksdb::{CompressionType, DB, DBOptions, Slice, WriteOptions};
use tempfile::TempDir;

/// Setup database with specific parallel compaction configuration
fn setup_db(parallel_threads: usize) -> (DB, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 8 * 1024, // Small buffer to trigger compaction
        block_cache_size: 100,
        parallel_compaction_threads: parallel_threads,
        enable_subcompaction: parallel_threads > 0,
        subcompaction_min_size: 10 * 1024,
        compression_type: CompressionType::None, // Disable compression for fair comparison
        ..Default::default()
    };
    let db = DB::open(temp_dir.path().to_str().unwrap(), options).unwrap();
    (db, temp_dir)
}

/// Write data that will trigger compaction
fn write_compaction_workload(db: &DB, num_keys: usize, value_size: usize) {
    let write_opts = WriteOptions { sync: false };

    // Write data in 3 batches to create multiple L0 files
    for _batch in 0..3 {
        for i in 0..num_keys {
            let key = format!("key{i:06}");
            let value = vec![b'x'; value_size];
            db.put(
                &write_opts,
                Slice::from(key.as_str()),
                Slice::from(value.as_slice()),
            )
            .unwrap();
        }

        // Trigger compaction after each batch
        db.maybe_compact().unwrap();
    }
}

/// Benchmark sequential compaction (parallel_threads = 0)
fn bench_sequential_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("sequential", |b| {
        b.iter(|| {
            let (db, _temp) = setup_db(0); // Sequential
            write_compaction_workload(black_box(&db), 200, 100);
        });
    });

    group.finish();
}

/// Benchmark parallel compaction with 4 threads
fn bench_parallel_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("parallel_4_threads", |b| {
        b.iter(|| {
            let (db, _temp) = setup_db(4); // 4 threads
            write_compaction_workload(black_box(&db), 200, 100);
        });
    });

    group.finish();
}

/// Benchmark compaction with varying thread counts
fn bench_compaction_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_scaling");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    for threads in [0, 2, 4, 8] {
        group.bench_function(format!("{threads}_threads"), |b| {
            b.iter(|| {
                let (db, _temp) = setup_db(threads);
                write_compaction_workload(black_box(&db), 200, 100);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_compaction,
    bench_parallel_compaction,
    bench_compaction_scaling
);
criterion_main!(benches);
