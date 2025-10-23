use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rucksdb::{DBOptions, Slice, WriteOptions, DB};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");

    for num_threads in [1, 2, 4, 8].iter() {
        group.throughput(Throughput::Elements(*num_threads as u64 * 1000));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_threads}_threads")),
            num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let options = DBOptions {
                        create_if_missing: true,
                        error_if_exists: false,
                        write_buffer_size: 4 * 1024 * 1024,
                        block_cache_size: 1000,
                        ..Default::default()
                    };
                    let db =
                        Arc::new(DB::open(temp_dir.path().to_str().unwrap(), options).unwrap());

                    let mut handles = vec![];
                    for thread_id in 0..num_threads {
                        let db = Arc::clone(&db);
                        let handle = thread::spawn(move || {
                            let value = vec![b'x'; 1024];
                            for i in 0..1000 {
                                let key = format!("t{thread_id}_key{i:06}");
                                db.put(
                                    &WriteOptions::default(),
                                    Slice::from(key),
                                    Slice::from(value.as_slice()),
                                )
                                .unwrap();
                            }
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_flush_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_impact");
    group.sample_size(20); // Fewer samples for expensive benchmark

    // Measure write latency distribution during flush
    group.bench_function("writes_during_flush", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let options = DBOptions {
                create_if_missing: true,
                error_if_exists: false,
                write_buffer_size: 1024 * 1024, // Smaller buffer to trigger flush
                block_cache_size: 100,
                ..Default::default()
            };
            let db = Arc::new(DB::open(temp_dir.path().to_str().unwrap(), options).unwrap());

            // Write enough to trigger multiple flushes
            let value = vec![b'x'; 1024];
            for i in 0..2000 {
                let key = format!("key{i:06}");
                db.put(
                    &WriteOptions::default(),
                    Slice::from(key),
                    Slice::from(value.as_slice()),
                )
                .unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_writes, bench_flush_impact);
criterion_main!(benches);
