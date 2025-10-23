# RucksDB Benchmarking Guide

## Overview

This document describes the benchmarking infrastructure for RucksDB and how to use it.

## Quick Start

```bash
# Run all benchmarks
./scripts/bench.sh

# Run specific benchmark
./scripts/bench.sh basic_ops

# View results
open target/criterion/report/index.html
```

## Available Benchmarks

### 1. Basic Operations (`basic_ops`)

Measures single-threaded throughput for core operations:

- **put**: Write throughput with different value sizes (100B, 1KB, 10KB)
- **get**: Read throughput from MemTable, SSTable, and non-existent keys
- **delete**: Delete operation throughput
- **mixed**: 50/50 read/write workload

**When to use**: Baseline performance measurement

### 2. Concurrent Operations (`concurrent`)

Measures multi-threaded scalability:

- **concurrent_writes**: Write throughput with 1, 2, 4, 8 threads
- **flush_impact**: Measures write latency during flush operations

**When to use**: Verify concurrent write performance and immutable MemTable effectiveness

## Important: Data Volume Impact on LSM-Tree Performance

**The total volume of data is a critical factor for LSM-Tree performance.** Results vary significantly based on dataset size:

### Why Data Volume Matters

1. **Write Performance**:
   - **Small datasets (<100MB)**: Data stays in MemTable, write amplification minimal
   - **Medium datasets (100MB-1GB)**: Multiple flushes occur, compaction starts
   - **Large datasets (>1GB)**: Full LSM-Tree behavior with multi-level compaction

2. **Read Performance**:
   - **Small datasets**: Most data in MemTable or L0, fast reads
   - **Medium datasets**: Data spreads across levels, more seeks required
   - **Large datasets**: Bloom filters and block cache become critical

3. **Current Benchmark Data Volumes**:
   - `basic_ops`: ~1MB total (1000 operations × 1KB)
   - `concurrent`: ~4MB total (4 threads × 1000 ops × 1KB)
   - **These are small-scale benchmarks focusing on throughput, not full LSM behavior**

### Implications for Real-World Performance

To understand production performance:
- Run benchmarks with >1GB datasets to see full compaction impact
- Monitor write amplification (bytes written to disk / bytes written by user)
- Track read amplification (disk reads required per get operation)
- Measure space amplification (disk space / logical data size)

**Note**: The current benchmarks measure *throughput* with small datasets. For realistic LSM-Tree performance assessment, use larger workloads like YCSB.

## Interpreting Results

Criterion produces detailed reports in `target/criterion/`:

```
target/criterion/
├── report/
│   └── index.html          # Main HTML report (open in browser)
├── put_100b/
│   ├── report/
│   │   └── index.html      # Detailed per-benchmark report
│   └── base/
│       └── estimates.json  # Raw data for comparison
└── ...
```

### Key Metrics

- **time**: Average execution time per iteration
- **thrpt**: Throughput (operations per second)
- **R²**: Goodness of fit (closer to 1.0 is better)
- **mean**: Average time across all samples
- **std dev**: Standard deviation (lower is better for consistency)

### Performance Targets

Based on current implementation:

| Operation | Target | Notes |
|-----------|--------|-------|
| put (100B) | >50K ops/sec | Single thread |
| put (1KB) | >20K ops/sec | Single thread |
| get (MemTable) | >100K ops/sec | Hot data |
| get (SSTable) | >30K ops/sec | Cold data with cache |
| concurrent writes (4 threads) | >2x single thread | Scalability check |

## Profiling

For detailed performance analysis:

```bash
# Profile a specific benchmark
./scripts/profile.sh basic_ops

# View flamegraph
open flamegraph-basic_ops.svg
```

### On macOS

For better profiling on macOS, use `cargo-instruments`:

```bash
# Install
cargo install cargo-instruments

# Profile with time profiler
cargo instruments -t time --bench basic_ops

# Profile with allocations
cargo instruments -t alloc --bench basic_ops
```

## Performance Investigation Workflow

When performance is below expectations:

1. **Run benchmarks**: `./scripts/bench.sh`
2. **Identify bottleneck**: Check which operation is slow
3. **Profile**: `./scripts/profile.sh <benchmark_name>`
4. **Analyze flamegraph**: Look for hot paths
5. **Optimize**: Focus on top time consumers
6. **Verify**: Re-run benchmarks to confirm improvement

## Continuous Performance Monitoring

To track performance over time:

1. Run benchmarks before changes: `cargo bench`
2. Make your changes
3. Run benchmarks again: `cargo bench`
4. Criterion will show comparison with previous run

Example output:
```
put/put_100b           time:   [19.234 µs 19.456 µs 19.712 µs]
                       change: [-5.2341% -3.8234% -2.1234%] (p = 0.00 < 0.05)
                       Performance has improved.
```

## Adding New Benchmarks

Create a new file in `benches/`:

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn my_benchmark(c: &mut Criterion) {
    c.bench_function("my_operation", |b| {
        // Setup
        b.iter(|| {
            // Operation to benchmark
        });
    });
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);
```

Register in `Cargo.toml`:

```toml
[[bench]]
name = "my_benchmark"
harness = false
```

## CI Integration

Benchmarks are NOT run in CI by default (they're too slow). To add performance regression testing:

```yaml
# In .github/workflows/ci.yml
  benchmark:
    name: Performance Check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run benchmarks
        run: cargo bench --bench basic_ops -- --quick
```

## Tips

- Run benchmarks on a quiet machine (close other apps)
- Disable CPU frequency scaling if possible
- Use `--quick` flag for faster iteration during development
- Save baseline before major changes: `cargo bench -- --save-baseline before`
- Compare with baseline: `cargo bench -- --baseline before`
