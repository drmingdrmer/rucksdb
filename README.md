# RucksDB

A Rust implementation of [RocksDB](https://github.com/facebook/rocksdb), a high-performance embedded key-value store originally developed by Facebook.

## Current Status - Phase 1 Complete

This is an ongoing project to reimplement RocksDB in Rust. Phase 1 is complete with basic functionality:

### Implemented Features

- **Core Data Types**
  - `Status`: Error handling with multiple error codes
  - `Slice`: Zero-copy byte slice abstraction

- **MemTable**
  - Lock-free SkipList implementation using crossbeam-skiplist
  - InternalKey encoding with sequence numbers
  - Memory usage tracking

- **Basic DB Operations**
  - `Put(key, value)`: Insert or update key-value pairs
  - `Get(key)`: Retrieve values by key
  - `Delete(key)`: Remove keys
  - MVCC support with sequence numbers

### Architecture

RucksDB follows LSM-Tree design:
- Write operations go to in-memory MemTable
- Sequence numbers ensure correct ordering
- InternalKey format: `user_key + separator + reversed_sequence + type`

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

## Testing

```bash
cargo test --all
cargo run --example simple
```

All 26 tests passing including integration tests with 1000+ keys.

## Roadmap

### Phase 2: LSM-Tree Core (Next)
- Write Ahead Log (WAL)
- SSTable implementation
- Compaction mechanism
- Version management

### Phase 3: Performance Optimization
- Block Cache (LRU)
- Bloom Filters
- Compression (Snappy, LZ4, Zstd)

### Phase 4: Advanced Features
- Column Families
- Transactions
- Snapshots
- Backup/Restore

## License

Apache-2.0
