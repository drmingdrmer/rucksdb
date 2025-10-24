# RucksDB Performance Tuning Guide

## Overview

This guide provides practical advice for optimizing RucksDB performance based on your workload characteristics. RucksDB achieves:

- **Write Throughput:** 105K ops/sec
- **Random Read:** 4.3K ops/sec
- **Sequential Read:** 773K ops/sec
- **Latency (P99):** 10μs write, 366μs random read

## Table of Contents

- [Quick Tuning Checklist](#quick-tuning-checklist)
- [Write-Heavy Workloads](#write-heavy-workloads)
- [Read-Heavy Workloads](#read-heavy-workloads)
- [Mixed Workloads](#mixed-workloads)
- [Memory Management](#memory-management)
- [Compression](#compression)
- [Column Families](#column-families)
- [Monitoring & Diagnostics](#monitoring--diagnostics)
- [Common Pitfalls](#common-pitfalls)

## Quick Tuning Checklist

### Write-Optimized Settings

```rust
use rucksdb::{DBOptions, CompressionType};

let options = DBOptions {
    write_buffer_size: 64 * 1024 * 1024,     // 64MB (large buffer)
    block_cache_size: 1000,                   // Minimal (writes don't need cache)
    table_cache_size: 50,                     // Minimal
    compression_type: CompressionType::LZ4,   // Fast compression
    filter_bits_per_key: None,                // Skip bloom filters
    ..Default::default()
};
```

### Read-Optimized Settings

```rust
let options = DBOptions {
    write_buffer_size: 4 * 1024 * 1024,      // 4MB (default)
    block_cache_size: 10000,                  // Large cache (~40MB)
    table_cache_size: 500,                    // Many open files
    compression_type: CompressionType::Snappy, // Good balance
    filter_bits_per_key: Some(10),            // Enable bloom filters
    ..Default::default()
};
```

### Balanced Settings (Default)

```rust
let options = DBOptions::default();
// write_buffer_size: 4MB
// block_cache_size: 1000
// table_cache_size: 100
// compression: Snappy
// filter_bits_per_key: Some(10)
```

## Write-Heavy Workloads

### Symptoms

- High put/delete rate (>50K ops/sec)
- Frequent MemTable flushes
- High compaction overhead
- Growing database size

### Optimization Strategies

#### 1. Increase Write Buffer Size

**Problem:** Frequent MemTable flushes create many small SSTables

**Solution:** Larger write buffer = fewer, larger SSTables

```rust
let options = DBOptions {
    write_buffer_size: 64 * 1024 * 1024, // 64MB
    ..Default::default()
};
```

**Impact:**
- **Pros:** Fewer SSTables, less compaction, higher throughput
- **Cons:** More memory usage, longer recovery time
- **Recommended:** 16-64MB for write-heavy workloads

#### 2. Use Fast Compression

**Problem:** Snappy compression adds 10-20% CPU overhead

**Solution:** Use LZ4 for faster compression

```rust
let options = DBOptions {
    compression_type: CompressionType::LZ4,
    ..Default::default()
};
```

**Comparison:**
| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| None | Fastest | 1.0x | SSDs, CPU-bound |
| LZ4 | Fast | 1.8x | Write-heavy |
| Snappy | Medium | 2.0x | Balanced |

#### 3. Batch Writes

**Problem:** Individual writes with sync=true are slow

**Solution:** Batch multiple operations

```rust
// Inefficient: sync every write
for i in 0..1000 {
    db.put(&WriteOptions { sync: true }, ...)?; // 1000 fsyncs!
}

// Efficient: batch writes, sync once
for i in 0..1000 {
    db.put(&WriteOptions { sync: false }, ...)?;
}
db.put(&WriteOptions { sync: true }, ...)?; // 1 fsync
```

**Impact:** 100x faster (1000 fsyncs -> 1 fsync)

#### 4. Disable Bloom Filters (Write-Only Systems)

**Problem:** Building bloom filters during flush adds overhead

**Solution:** Skip bloom filters if no reads

```rust
let options = DBOptions {
    filter_bits_per_key: None, // Disable bloom filters
    ..Default::default()
};
```

**Trade-off:**
- **Saves:** 10 bits per key (e.g., 1MB for 1M keys)
- **Costs:** Slower reads (no false-negative filtering)
- **Use when:** Write-only or infrequent reads

#### 5. Monitor Compaction

```rust
let stats = db.statistics();
println!("Compactions: {}", stats.num_compactions());
println!("Compaction bytes: {}", stats.compaction_bytes_written());

// High compaction -> consider larger write buffer
```

## Read-Heavy Workloads

### Symptoms

- High get/scan rate (>10K ops/sec)
- Low MemTable hit rate (<50%)
- Many SSTable reads
- High read latency (>1ms)

### Optimization Strategies

#### 1. Increase Block Cache Size

**Problem:** Cache misses force disk reads

**Solution:** Larger cache = more hot blocks in memory

```rust
let options = DBOptions {
    block_cache_size: 10000, // 10K blocks = ~40MB
    ..Default::default()
};
```

**Guidelines:**
- **4KB per block** (block_cache_size × 4KB = memory)
- **Start with:** 1000 blocks (~4MB)
- **Read-heavy:** 5000-10000 blocks (20-40MB)
- **Very hot:** 50000+ blocks (200MB+)

**Check effectiveness:**
```rust
let cache_stats = db.cache_stats();
println!("Hit rate: {:.2}%", cache_stats.hit_rate() * 100.0);

// Target: >80% for good performance
```

#### 2. Increase Table Cache Size

**Problem:** Opening SSTable files on every read is expensive

**Solution:** Keep more files open

```rust
let options = DBOptions {
    table_cache_size: 500, // Keep 500 SSTables open
    ..Default::default()
};
```

**Impact:**
- **Default (100):** Good for most workloads
- **Read-heavy (500+):** Eliminates file open overhead
- **Measured improvement:** 1.8x random read throughput (2.4K -> 4.3K ops/sec)

**Memory cost:**
- ~10KB per open file (index + filter blocks)
- 500 files ≈ 5MB memory

#### 3. Enable Bloom Filters

**Problem:** Checking non-existent keys requires disk reads

**Solution:** Bloom filters skip 99% of unnecessary reads

```rust
let options = DBOptions {
    filter_bits_per_key: Some(10), // ~1% false positive rate
    ..Default::default()
};
```

**Effectiveness:**
```rust
let stats = db.statistics();
let fp_rate = stats.bloom_filter_misses as f64
    / (stats.bloom_filter_hits + stats.bloom_filter_misses) as f64;

println!("Bloom filter false positive rate: {:.2}%", fp_rate * 100.0);
// Target: <2%
```

**Configuration:**
| Bits/Key | False Positive Rate | Memory (1M keys) |
|----------|---------------------|------------------|
| 5        | ~6%                 | 625KB            |
| 10       | ~1%                 | 1.25MB           |
| 15       | ~0.2%               | 1.875MB          |

#### 4. Use Compression

**Problem:** Reading more data from disk than necessary

**Solution:** Compression reduces I/O

```rust
let options = DBOptions {
    compression_type: CompressionType::Snappy, // Good balance
    ..Default::default()
};
```

**Benchmark results:**
- **Snappy:** 2x compression, 10% CPU overhead
- **LZ4:** 1.8x compression, 5% CPU overhead
- **None:** No CPU overhead, 2x more I/O

**Choose Snappy when:** I/O bound (HDD, network storage)
**Choose LZ4 when:** CPU bound, fast SSDs

#### 5. Optimize Range Scans

**Use iterators, not individual gets:**

```rust
// Inefficient: 1000 random reads
for i in 0..1000 {
    let key = format!("key{:06}", i);
    db.get(&ReadOptions::default(), &Slice::from(key))?;
}

// Efficient: 1 sequential scan
let mut iter = db.iter()?;
if iter.seek(&Slice::from("key000000"))? {
    for _ in 0..1000 {
        // Process iter.key(), iter.value()
        if !iter.next()? {
            break;
        }
    }
}
```

**Performance:**
- Individual gets: ~4K ops/sec
- Sequential scan: ~773K ops/sec (180x faster!)

## Mixed Workloads

### Balancing Read and Write Performance

#### 1. Moderate Buffer Size

```rust
let options = DBOptions {
    write_buffer_size: 16 * 1024 * 1024, // 16MB (between 4MB and 64MB)
    ..Default::default()
};
```

#### 2. Balanced Cache

```rust
let options = DBOptions {
    block_cache_size: 5000,   // ~20MB
    table_cache_size: 200,     // ~2MB
    ..Default::default()
};
```

#### 3. Enable Bloom Filters

```rust
let options = DBOptions {
    filter_bits_per_key: Some(10), // Essential for mixed workloads
    ..Default::default()
};
```

#### 4. Monitor Both Paths

```rust
let stats = db.statistics();

// Write path
println!("Puts/sec: {:.0}", stats.num_puts() as f64 / uptime_secs);
println!("MemTable flushes: {}", stats.num_memtable_flushes());

// Read path
println!("Gets/sec: {:.0}", stats.num_gets() as f64 / uptime_secs);
println!("MemTable hit rate: {:.2}%", stats.memtable_hit_rate() * 100.0);
println!("Cache hit rate: {:.2}%", db.cache_stats().hit_rate() * 100.0);
```

## Memory Management

### Total Memory Budget

```
Total Memory = Write Buffer + Block Cache + Table Cache + Overhead

Example (Default):
  Write Buffer:    4MB
  Block Cache:     4MB (1000 blocks × 4KB)
  Table Cache:     1MB (100 files × 10KB)
  Overhead:        ~2MB (VersionSet, MemTable structure)
  ─────────────────────
  Total:           ~11MB
```

### Memory-Constrained Systems

For systems with <100MB available:

```rust
let options = DBOptions {
    write_buffer_size: 2 * 1024 * 1024,  // 2MB
    block_cache_size: 500,                // ~2MB
    table_cache_size: 50,                 // ~500KB
    compression_type: CompressionType::Snappy, // Reduce disk usage
    filter_bits_per_key: Some(10),        // Worth the 1MB for 1M keys
    ..Default::default()
};

// Total: ~5.5MB
```

### High-Memory Systems

For systems with >1GB available:

```rust
let options = DBOptions {
    write_buffer_size: 128 * 1024 * 1024, // 128MB
    block_cache_size: 50000,               // ~200MB
    table_cache_size: 1000,                // ~10MB
    ..Default::default()
};

// Total: ~338MB (+ 100MB headroom)
```

## Compression

### Choosing Compression Algorithm

| Workload | Recommendation | Reason |
|----------|----------------|--------|
| Write-heavy | LZ4 | Fastest compression |
| Read-heavy, HDD | Snappy | Better ratio, reduces I/O |
| SSD, hot data | None | CPU saved, data in cache |
| Cold storage | Snappy | Maximize space savings |

### Compression Effectiveness

```rust
use std::fs;

// Check actual compression ratio
let db_size = fs::read_dir("/path/to/db")?
    .filter_map(|e| e.ok())
    .filter(|e| e.path().extension().map_or(false, |ext| ext == "sst"))
    .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
    .sum::<u64>();

let uncompressed_estimate = db.statistics().num_puts() * (avg_key_size + avg_value_size);
let ratio = uncompressed_estimate as f64 / db_size as f64;

println!("Compression ratio: {:.2}x", ratio);
// Typical: 1.8x (LZ4) to 2.5x (Snappy)
```

### Disabling Compression

When to use `CompressionType::None`:

1. **Fast SSDs**: I/O is not the bottleneck
2. **CPU-bound**: CPU is maxed out
3. **Pre-compressed data**: JSON, images, videos
4. **Temporary data**: Short-lived, deletion-heavy

## Column Families

### When to Use Column Families

**Use CFs for:**
- Logical separation (metadata vs. data)
- Different access patterns per dataset
- Independent TTL/compaction settings
- Multi-tenant systems

**Example:**

```rust
// Metadata: small, frequently accessed
let metadata_opts = ColumnFamilyOptions::default();
let metadata_cf = db.create_column_family("metadata", metadata_opts)?;

// Data: large, infrequently accessed
let data_opts = ColumnFamilyOptions::default();
let data_cf = db.create_column_family("data", data_opts)?;
```

### Per-CF Tuning

```rust
// Fast metadata reads
db.put_cf(&WriteOptions::default(), &metadata_cf, ...)?;

// Bulk data writes
db.put_cf(&WriteOptions { sync: false }, &data_cf, ...)?;
```

### CF Best Practices

1. **Don't overuse:** Each CF adds overhead (VersionSet, compaction)
2. **Typical usage:** 2-10 CFs per database
3. **Shared WAL:** All CFs write to same WAL (efficient recovery)

## Monitoring & Diagnostics

### Essential Metrics

```rust
use std::time::Instant;

let start = Instant::now();

// Run workload...

let elapsed = start.elapsed().as_secs_f64();
let stats = db.statistics();

println!("=== Performance Report ===");
println!("Duration: {:.2}s", elapsed);
println!();
println!("Throughput:");
println!("  Puts/sec:    {:.0}", stats.num_puts() as f64 / elapsed);
println!("  Gets/sec:    {:.0}", stats.num_gets() as f64 / elapsed);
println!();
println!("Hit Rates:");
println!("  MemTable:    {:.2}%", stats.memtable_hit_rate() * 100.0);
println!("  Block Cache: {:.2}%", db.cache_stats().hit_rate() * 100.0);
println!("  Bloom:       {:.2}%", stats.bloom_filter_effectiveness() * 100.0);
println!();
println!("Storage:");
println!("  SSTables:    {}", stats.num_sstable_files());
println!("  Compactions: {}", stats.num_compactions());
```

### Red Flags

**Low MemTable hit rate (<50%):**
- Data not in MemTable (too small or cold data)
- Solution: Increase `write_buffer_size` or use cache

**Low block cache hit rate (<70%):**
- Cache too small for working set
- Solution: Increase `block_cache_size`

**High compaction activity (>10% uptime):**
- Too many small SSTables
- Solution: Increase `write_buffer_size`

**Low bloom filter effectiveness (<80%):**
- Many existing keys being checked
- Normal for workloads with high hit rates
- Not a problem if reads are fast

### Profiling Read Latency

```rust
use std::time::Instant;

let mut latencies = Vec::new();

for _ in 0..10000 {
    let start = Instant::now();
    db.get(&ReadOptions::default(), &Slice::from("key"))?;
    latencies.push(start.elapsed());
}

latencies.sort();
let p50 = latencies[latencies.len() / 2];
let p99 = latencies[latencies.len() * 99 / 100];

println!("Read latency P50: {:?}", p50);
println!("Read latency P99: {:?}", p99);

// Target: <100μs P99 for cached data
//         <500μs P99 for disk reads
```

## Common Pitfalls

### 1. Sync on Every Write

**Problem:**
```rust
// SLOW: 100-1000 ops/sec
for i in 0..10000 {
    db.put(&WriteOptions { sync: true }, ...)?; // fsync!
}
```

**Solution:**
```rust
// FAST: 100K ops/sec
for i in 0..10000 {
    db.put(&WriteOptions { sync: false }, ...)?;
}
// Optional: sync at end
db.put(&WriteOptions { sync: true }, ...)?;
```

### 2. Reading Keys Individually

**Problem:**
```rust
// SLOW: 4K ops/sec
for i in 0..1000 {
    let key = format!("key{}", i);
    db.get(&ReadOptions::default(), &Slice::from(key))?;
}
```

**Solution:**
```rust
// FAST: 773K ops/sec
let mut iter = db.iter()?;
if iter.seek_to_first()? {
    for _ in 0..1000 {
        // Use iter.key(), iter.value()
        if !iter.next()? { break; }
    }
}
```

### 3. Ignoring Statistics

**Problem:** Performance degrades over time, no visibility

**Solution:**
```rust
// Periodically check
if db.statistics().memtable_hit_rate() < 0.5 {
    eprintln!("WARNING: Low MemTable hit rate - increase write_buffer_size");
}

if db.cache_stats().hit_rate() < 0.7 {
    eprintln!("WARNING: Low cache hit rate - increase block_cache_size");
}
```

### 4. Tiny Cache Sizes

**Problem:**
```rust
let options = DBOptions {
    block_cache_size: 10, // Only 40KB!
    ..Default::default()
};
```

**Solution:**
```rust
let options = DBOptions {
    block_cache_size: 1000, // 4MB (minimum)
    ..Default::default()
};
```

### 5. Not Using Bloom Filters

**Problem:** Every get() checks all SSTables

**Solution:**
```rust
let options = DBOptions {
    filter_bits_per_key: Some(10), // 1% false positive
    ..Default::default()
};

// Saves 99% of unnecessary disk reads!
```

## Performance Testing

### Benchmarking Tool

RucksDB includes `db_bench`:

```bash
# Build benchmark
cargo build --release --bin db_bench

# Run benchmark
./target/release/db_bench

# Output:
# Sequential Write: 105K ops/sec (P99=10μs)
# Random Read: 4.3K ops/sec (P99=366μs)
# Sequential Read: 773K ops/sec
```

### Custom Benchmark Template

```rust
use std::time::Instant;
use rucksdb::{DB, DBOptions, Slice, WriteOptions, ReadOptions};

fn benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::open("/tmp/bench", DBOptions::default())?;

    // Benchmark writes
    let start = Instant::now();
    for i in 0..100_000 {
        let key = format!("k{:08}", i);
        let value = format!("v{:08}", i);
        db.put(&WriteOptions::default(), Slice::from(key), Slice::from(value))?;
    }
    let write_duration = start.elapsed();

    println!("Write throughput: {:.0} ops/sec",
        100_000.0 / write_duration.as_secs_f64());

    // Benchmark reads
    let start = Instant::now();
    for i in 0..100_000 {
        let key = format!("k{:08}", i);
        db.get(&ReadOptions::default(), &Slice::from(key))?;
    }
    let read_duration = start.elapsed();

    println!("Read throughput: {:.0} ops/sec",
        100_000.0 / read_duration.as_secs_f64());

    // Print statistics
    println!("\n{}", db.statistics().report());

    Ok(())
}
```

## Summary

### Quick Wins

1. **Increase block_cache_size** to 5000+ for read-heavy workloads
2. **Increase write_buffer_size** to 16-64MB for write-heavy workloads
3. **Enable bloom filters** (filter_bits_per_key = 10)
4. **Use iterators** for range scans instead of individual gets
5. **Batch writes** with sync=false

### Configuration Matrix

| Workload | write_buffer_size | block_cache_size | table_cache_size | compression | bloom_filter |
|----------|-------------------|------------------|------------------|-------------|--------------|
| Write-heavy | 64MB | 1000 | 50 | LZ4 | None |
| Read-heavy | 4MB | 10000 | 500 | Snappy | 10 bits |
| Balanced | 16MB | 5000 | 200 | Snappy | 10 bits |
| Memory-limited | 2MB | 500 | 50 | Snappy | 10 bits |

### Monitoring Checklist

- [ ] MemTable hit rate >50%
- [ ] Block cache hit rate >70%
- [ ] Bloom filter effectiveness >80%
- [ ] Compaction overhead <10%
- [ ] Read P99 latency <500μs
- [ ] Write P99 latency <50μs

## Further Reading

- [Architecture Guide](./ARCHITECTURE.md) - System internals
- [API Documentation](./API.md) - Usage examples
- [LevelDB Performance Documentation](https://github.com/google/leveldb/blob/main/doc/index.md)
- [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
