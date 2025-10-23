# RucksDB

A Rust implementation of [RocksDB](https://github.com/facebook/rocksdb), a high-performance embedded key-value store originally developed by Facebook.

> **Note**: This project was created using Claude Code. See [all prompts used to create this project](https://github.com/drmingdrmer/rucksdb/discussions/2) - you can use them as a reference to create your own project!

## Educational Focus

**RucksDB is designed as an educational project** to demonstrate database internals and Rust best practices:
- **Custom implementations** for learning (e.g., LRU cache using HashMap + doubly-linked list)
- **Rust 2024 Edition** with nightly features for modern idioms
- **Comprehensive documentation** explaining architectural decisions
- **Clear code structure** prioritizing readability over micro-optimizations

## Requirements

- **Rust Nightly**: This project uses Rust 2024 edition and nightly features
- Automatically configured via `rust-toolchain.toml`

## Current Status - Phase 3 Complete

LSM-Tree implementation with performance optimizations:

### Implemented Features

#### Phase 1: Foundation
- **Core Data Types**: `Status`, `Slice` with zero-copy semantics
- **MemTable**: Lock-free SkipList with MVCC support
- **Basic Operations**: Put, Get, Delete with sequence numbers

#### Phase 2: LSM-Tree Core
- **Write Ahead Log (WAL)**: Crash recovery and durability
- **SSTable**: Persistent sorted string tables with block-based storage
- **Compaction**: Multi-level compaction with size-based triggering
- **Version Management**: MVCC with multiple versions

#### Phase 3: Performance Optimizations
- **Custom LRU Cache**: Educational implementation (HashMap + doubly-linked list)
- **Bloom Filters**: Reduce unnecessary disk I/O (240x speedup for non-existent keys)
- **Compression**: Snappy and LZ4 support
- **Immutable MemTable**: Non-blocking writes during flush

### Architecture

LSM-Tree design with educational focus:
- **Write path**: WAL → MemTable → Immutable MemTable → SSTable
- **Read path**: MemTable → Immutable MemTable → L0 → L1..Ln SSTables
- **Compaction**: Size-based triggering with configurable thresholds
- See [BENCHMARKING.md](BENCHMARKING.md) and [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for performance characteristics

## Usage

```rust
use rucksdb::{DB, DBOptions, ReadOptions, WriteOptions, Slice};

fn main() {
    let db = DB::open("my_db", DBOptions::default()).unwrap();

    // Write
    db.put(&WriteOptions::default(),
           Slice::from("key"),
           Slice::from("value")).unwrap();

    // Read
    let value = db.get(&ReadOptions::default(), &Slice::from("key")).unwrap();
    println!("Value: {:?}", value);

    // Delete
    db.delete(&WriteOptions::default(), Slice::from("key")).unwrap();
}
```

## Development

```bash
# Run all tests (92 unit tests + integration tests)
just test

# Run benchmarks
just bench

# Run pre-commit checks (format, clippy, tests)
just pre-commit

# See all available commands
just
```

## Performance

- **Single-threaded write**: 330K ops/sec (100B), 134K ops/sec (1KB)
- **Single-threaded read**: 20K ops/sec (MemTable), 9.3K ops/sec (SSTable)
- **Multi-threaded**: 1.26x speedup at 4 threads
- **Bloom filter**: 240x speedup for non-existent keys

See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for detailed analysis.

⚠️ **Note**: Benchmarks use small datasets (~1-4MB). LSM-Tree behavior differs significantly with larger datasets (>1GB). See [BENCHMARKING.md](BENCHMARKING.md) for details.

## Roadmap

### Phase 4: Advanced Features (Next)
- Column Families
- Transactions
- Snapshots
- Backup/Restore
- Iterator API improvements

## License

Apache-2.0
