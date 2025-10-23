use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

fn setup_db() -> (DB, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 4 * 1024 * 1024,
        block_cache_size: 1000,
        ..Default::default()
    };
    let db = DB::open(temp_dir.path().to_str().unwrap(), options).unwrap();
    (db, temp_dir)
}

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");
    group.throughput(Throughput::Elements(1));

    // Small values (100 bytes)
    group.bench_function("put_100b", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 100];
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
            i += 1;
        });
    });

    // Medium values (1KB)
    group.bench_function("put_1kb", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 1024];
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
            i += 1;
        });
    });

    // Large values (10KB)
    group.bench_function("put_10kb", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 10240];
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
            i += 1;
        });
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));

    // Get from MemTable (hot data)
    group.bench_function("get_memtable", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 1024];

        // Insert 1000 keys
        for i in 0..1000 {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            let key = format!("key{:010}", i % 1000);
            black_box(
                db.get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap(),
            );
            i += 1;
        });
    });

    // Get from SSTable (cold data)
    group.bench_function("get_sstable", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 1024];

        // Insert enough data to trigger flush
        for i in 0..5000 {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            let key = format!("key{:010}", i % 5000);
            black_box(
                db.get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap(),
            );
            i += 1;
        });
    });

    // Get non-existent keys
    group.bench_function("get_not_found", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 1024];

        // Insert 1000 keys
        for i in 0..1000 {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            let key = format!("notfound{i:010}");
            black_box(
                db.get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap(),
            );
            i += 1;
        });
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    group.throughput(Throughput::Elements(1));

    group.bench_function("delete", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 1024];

        // Insert keys for deletion
        for i in 0..10000 {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            let key = format!("key{i:010}");
            db.delete(&WriteOptions::default(), Slice::from(key))
                .unwrap();
            i += 1;
        });
    });

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed");
    group.throughput(Throughput::Elements(1));

    // 50% reads, 50% writes
    group.bench_function("read_write_50_50", |b| {
        let (db, _temp) = setup_db();
        let value = vec![b'x'; 1024];

        // Pre-populate with some data
        for i in 0..1000 {
            let key = format!("key{i:010}");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value.as_slice()),
            )
            .unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            if i % 2 == 0 {
                // Read
                let key = format!("key{:010}", i % 1000);
                black_box(
                    db.get(&ReadOptions::default(), &Slice::from(key.as_str()))
                        .unwrap(),
                );
            } else {
                // Write
                let key = format!("key{i:010}");
                db.put(
                    &WriteOptions::default(),
                    Slice::from(key),
                    Slice::from(value.as_slice()),
                )
                .unwrap();
            }
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_get,
    bench_delete,
    bench_mixed_workload
);
criterion_main!(benches);
