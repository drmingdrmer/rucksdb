use std::{
    io::{self, Write},
    time::{Duration, Instant},
};

use rucksdb::{CompressionType, DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

/// Benchmark configuration
struct BenchConfig {
    num_keys: usize,
    value_size: usize,
    cache_size: usize,
    compression: CompressionType,
    use_bloom_filter: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        BenchConfig {
            num_keys: 100_000,
            value_size: 1000,
            cache_size: 1000,
            compression: CompressionType::Snappy,
            use_bloom_filter: true,
        }
    }
}

/// Statistics for a benchmark run
struct BenchStats {
    duration: Duration,
    operations: usize,
    bytes_written: usize,
    latencies: Vec<Duration>,
}

impl BenchStats {
    fn new() -> Self {
        BenchStats {
            duration: Duration::ZERO,
            operations: 0,
            bytes_written: 0,
            latencies: Vec::new(),
        }
    }

    fn ops_per_sec(&self) -> f64 {
        self.operations as f64 / self.duration.as_secs_f64()
    }

    fn mb_per_sec(&self) -> f64 {
        (self.bytes_written as f64 / (1024.0 * 1024.0)) / self.duration.as_secs_f64()
    }

    fn avg_latency_us(&self) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        let sum: u128 = self.latencies.iter().map(|d| d.as_micros()).sum();
        sum as f64 / self.latencies.len() as f64
    }

    fn percentile_latency_us(&mut self, percentile: f64) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        self.latencies.sort();
        let idx = ((self.latencies.len() as f64 * percentile / 100.0) as usize)
            .min(self.latencies.len() - 1);
        self.latencies[idx].as_micros() as f64
    }

    fn print_summary(&mut self, name: &str) {
        println!("\n{}", "=".repeat(60));
        println!("Benchmark: {}", name);
        println!("{}", "=".repeat(60));
        println!("Operations:     {:>12}", format_number(self.operations));
        println!("Duration:       {:>12.2} sec", self.duration.as_secs_f64());
        println!("Throughput:     {:>12.0} ops/sec", self.ops_per_sec());
        println!("Throughput:     {:>12.2} MB/sec", self.mb_per_sec());
        println!("\nLatency (microseconds):");
        println!("  Average:      {:>12.2}", self.avg_latency_us());
        println!("  P50:          {:>12.2}", self.percentile_latency_us(50.0));
        println!("  P95:          {:>12.2}", self.percentile_latency_us(95.0));
        println!("  P99:          {:>12.2}", self.percentile_latency_us(99.0));
        println!("  P99.9:        {:>12.2}", self.percentile_latency_us(99.9));
        println!("{}", "=".repeat(60));
    }
}

fn format_number(n: usize) -> String {
    n.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap()
        .join(",")
}

/// Progress indicator
struct ProgressBar {
    total: usize,
    current: usize,
    last_update: Instant,
}

impl ProgressBar {
    fn new(total: usize) -> Self {
        ProgressBar {
            total,
            current: 0,
            last_update: Instant::now(),
        }
    }

    fn update(&mut self, current: usize) {
        self.current = current;
        if self.last_update.elapsed() > Duration::from_millis(100) {
            self.display();
            self.last_update = Instant::now();
        }
    }

    fn finish(&mut self) {
        self.current = self.total;
        self.display();
        println!();
    }

    fn display(&self) {
        let percent = (self.current as f64 / self.total as f64 * 100.0) as usize;
        let bar_width = 40;
        let filled = (bar_width * self.current) / self.total;
        let bar = "=".repeat(filled) + &" ".repeat(bar_width - filled);
        print!(
            "\r[{}] {:>3}% ({}/{})",
            bar,
            percent,
            format_number(self.current),
            format_number(self.total)
        );
        io::stdout().flush().unwrap();
    }
}

/// Generate a value of specified size
fn generate_value(size: usize, seed: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(size);
    let mut x = seed;
    for _ in 0..size {
        x = x.wrapping_mul(1103515245).wrapping_add(12345);
        value.push((x >> 16) as u8);
    }
    value
}

/// Sequential write benchmark
fn bench_seq_write(db: &DB, config: &BenchConfig) -> BenchStats {
    println!("\n📝 Running sequential write benchmark...");
    let mut stats = BenchStats::new();
    let mut progress = ProgressBar::new(config.num_keys);
    let write_opts = WriteOptions { sync: false };

    let start = Instant::now();
    for i in 0..config.num_keys {
        let key = format!("key{:08}", i);
        let value = generate_value(config.value_size, i);

        let op_start = Instant::now();
        db.put(
            &write_opts,
            Slice::from(key.as_str()),
            Slice::from(value.as_slice()),
        )
        .unwrap();
        stats.latencies.push(op_start.elapsed());

        stats.operations += 1;
        stats.bytes_written += key.len() + value.len();

        if i % 1000 == 0 {
            progress.update(i);
        }
    }
    progress.finish();
    stats.duration = start.elapsed();

    stats
}

/// Random read benchmark
fn bench_random_read(db: &DB, config: &BenchConfig) -> BenchStats {
    println!("\n📖 Running random read benchmark...");
    let mut stats = BenchStats::new();
    let mut progress = ProgressBar::new(config.num_keys);
    let read_opts = ReadOptions::default();

    // Use simple LCG for random-ish access pattern
    let mut x = 123456789u64;
    let start = Instant::now();

    for i in 0..config.num_keys {
        x = x.wrapping_mul(1103515245).wrapping_add(12345);
        let key_num = (x as usize) % config.num_keys;
        let key = format!("key{:08}", key_num);

        let op_start = Instant::now();
        let value = db.get(&read_opts, &Slice::from(key.as_str())).unwrap();
        stats.latencies.push(op_start.elapsed());

        assert!(value.is_some(), "Key not found: {}", key);
        stats.operations += 1;

        if i % 1000 == 0 {
            progress.update(i);
        }
    }
    progress.finish();
    stats.duration = start.elapsed();

    stats
}

/// Sequential read benchmark using iterator
fn bench_seq_read_iter(db: &DB, _config: &BenchConfig) -> BenchStats {
    println!("\n📚 Running sequential read (iterator) benchmark...");
    let mut stats = BenchStats::new();

    let start = Instant::now();
    let mut iter = db.iter().unwrap();

    let op_start = Instant::now();
    if !iter.seek_to_first().unwrap() {
        panic!("Iterator seek failed");
    }
    stats.latencies.push(op_start.elapsed());

    loop {
        let _key = iter.key();
        let _value = iter.value();
        stats.operations += 1;

        let op_start = Instant::now();
        if !iter.next().unwrap() {
            break;
        }
        stats.latencies.push(op_start.elapsed());
    }

    stats.duration = start.elapsed();
    println!("✅ Read {} entries", format_number(stats.operations));

    stats
}

/// Run fill benchmark (write only, used to populate DB)
fn bench_fill(db: &DB, config: &BenchConfig) -> BenchStats {
    println!(
        "\n🗂️  Filling database with {} keys...",
        format_number(config.num_keys)
    );
    bench_seq_write(db, config)
}

fn main() {
    println!("\n🚀 RucksDB Benchmark Tool");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let config = BenchConfig::default();

    println!("Configuration:");
    println!("  Keys:           {}", format_number(config.num_keys));
    println!("  Value size:     {} bytes", config.value_size);
    println!("  Cache size:     {} blocks", config.cache_size);
    println!("  Compression:    {:?}", config.compression);
    println!("  Bloom filter:   {}", config.use_bloom_filter);

    // Create temporary database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench_db");

    let options = DBOptions {
        create_if_missing: true,
        error_if_exists: false,
        write_buffer_size: 4 * 1024 * 1024, // 4MB
        block_cache_size: config.cache_size,
        table_cache_size: 100, // Keep up to 100 table files open
        compression_type: config.compression,
        filter_bits_per_key: if config.use_bloom_filter {
            Some(10)
        } else {
            None
        },
        enable_subcompaction: true,
        subcompaction_min_size: 10 * 1024 * 1024,
    };

    let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

    // Run benchmarks
    let mut write_stats = bench_fill(&db, &config);
    write_stats.print_summary("Sequential Write (fillseq)");

    let mut read_stats = bench_random_read(&db, &config);
    read_stats.print_summary("Random Read (readrandom)");

    let mut iter_stats = bench_seq_read_iter(&db, &config);
    iter_stats.print_summary("Sequential Read with Iterator (readseq)");

    // Print cache statistics
    let block_cache_stats = db.cache_stats();
    let table_cache_stats = db.table_cache_stats();
    println!("\n📊 Cache Statistics:");
    println!("\n  Block Cache:");
    println!(
        "    Hits:         {:>12}",
        format_number(block_cache_stats.hits as usize)
    );
    println!(
        "    Misses:       {:>12}",
        format_number(block_cache_stats.misses as usize)
    );
    println!(
        "    Hit Rate:     {:>12.2}%",
        block_cache_stats.hit_rate() * 100.0
    );
    println!(
        "    Entries:      {:>12}",
        format_number(block_cache_stats.entries)
    );
    println!(
        "    Capacity:     {:>12}",
        format_number(block_cache_stats.capacity)
    );
    println!("\n  Table Cache:");
    println!(
        "    Hits:         {:>12}",
        format_number(table_cache_stats.hits as usize)
    );
    println!(
        "    Misses:       {:>12}",
        format_number(table_cache_stats.misses as usize)
    );
    println!(
        "    Hit Rate:     {:>12.2}%",
        table_cache_stats.hit_rate() * 100.0
    );
    println!(
        "    Entries:      {:>12}",
        format_number(table_cache_stats.entries)
    );
    println!(
        "    Capacity:     {:>12}",
        format_number(table_cache_stats.capacity)
    );

    // Print database properties
    println!("\n📈 Database Properties:");

    // Files per level
    println!("\n  Files per level:");
    for level in 0..7 {
        if let Some(num_files) = db.get_property(&format!("rocksdb.num-files-at-level{}", level)) {
            let count: usize = num_files.parse().unwrap_or(0);
            if count > 0 {
                println!("    Level {}:      {:>12}", level, format_number(count));
            }
        }
    }

    // Total size
    if let Some(total_size_str) = db.get_property("rocksdb.total-size")
        && let Ok(total_size) = total_size_str.parse::<u64>()
    {
        let size_mb = total_size as f64 / (1024.0 * 1024.0);
        println!("\n  Total SST Size: {:>12.2} MB", size_mb);
    }

    // Statistics
    if let Some(stats) = db.get_property("rocksdb.stats") {
        println!("\n  Database Statistics:");
        for line in stats.lines() {
            println!("    {}", line);
        }
    }

    println!("\n✅ Benchmark completed!");
}
