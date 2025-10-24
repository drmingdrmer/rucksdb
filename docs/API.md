# RucksDB API Documentation

## Table of Contents

- [Getting Started](#getting-started)
- [Basic Operations](#basic-operations)
- [Column Families](#column-families)
- [Iterator API](#iterator-api)
- [Checkpoints](#checkpoints)
- [Statistics](#statistics)
- [Configuration](#configuration)
- [Error Handling](#error-handling)

## Getting Started

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rucksdb = "0.1"
```

### Opening a Database

```rust
use rucksdb::{DB, DBOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = DBOptions {
        create_if_missing: true,
        ..Default::default()
    };

    let db = DB::open("/path/to/db", options)?;

    // Use database...

    Ok(())
}
```

## Basic Operations

### Put (Write)

Insert or update a key-value pair.

```rust
use rucksdb::{Slice, WriteOptions};

// Simple write
db.put(
    &WriteOptions::default(),
    Slice::from("key1"),
    Slice::from("value1"),
)?;

// Write with sync (fsync immediately)
let sync_opts = WriteOptions { sync: true };
db.put(&sync_opts, Slice::from("key2"), Slice::from("value2"))?;

// Write binary data
let key = b"binary_key";
let value = vec![0u8, 1, 2, 3, 4];
db.put(
    &WriteOptions::default(),
    Slice::from(key.as_slice()),
    Slice::from(value.as_slice()),
)?;
```

**Options:**
- `sync`: If `true`, force fsync before returning (durable but slower)
- Default: `sync = false` (buffered by OS, faster but less durable)

### Get (Read)

Read a value by key.

```rust
use rucksdb::{ReadOptions, Slice};

let value = db.get(&ReadOptions::default(), &Slice::from("key1"))?;

match value {
    Some(v) => {
        println!("Found: {}", v.to_string());
        // Convert to bytes
        let bytes: &[u8] = v.data();
    }
    None => println!("Key not found"),
}
```

### Delete

Remove a key-value pair.

```rust
db.delete(&WriteOptions::default(), Slice::from("key1"))?;

// Verify deletion
let value = db.get(&ReadOptions::default(), &Slice::from("key1"))?;
assert_eq!(value, None);
```

### Batch Operations

```rust
// Multiple operations
for i in 0..1000 {
    let key = format!("key{:06}", i);
    let value = format!("value{:06}", i);
    db.put(
        &WriteOptions::default(),
        Slice::from(key),
        Slice::from(value),
    )?;
}
```

## Column Families

Logical partitioning of the keyspace.

### Creating Column Families

```rust
use rucksdb::{ColumnFamilyDescriptor, ColumnFamilyOptions};

// Open database
let db = DB::open("/path/to/db", DBOptions::default())?;

// Create new column families
let cf_opts = ColumnFamilyOptions::default();
let index_cf = db.create_column_family("index", cf_opts.clone())?;
let data_cf = db.create_column_family("data", cf_opts)?;

println!("Index CF ID: {}", index_cf.id());
println!("Data CF ID: {}", data_cf.id());
```

### Writing to Column Families

```rust
// Write to default CF
db.put(
    &WriteOptions::default(),
    Slice::from("key1"),
    Slice::from("value1"),
)?;

// Write to specific CF
db.put_cf(
    &WriteOptions::default(),
    &index_cf,
    Slice::from("index_key"),
    Slice::from("index_value"),
)?;

db.put_cf(
    &WriteOptions::default(),
    &data_cf,
    Slice::from("data_key"),
    Slice::from("data_value"),
)?;
```

### Reading from Column Families

```rust
// Read from default CF
let value = db.get(&ReadOptions::default(), &Slice::from("key1"))?;

// Read from specific CF
let index_value = db.get_cf(
    &ReadOptions::default(),
    &index_cf,
    &Slice::from("index_key"),
)?;

let data_value = db.get_cf(
    &ReadOptions::default(),
    &data_cf,
    &Slice::from("data_key"),
)?;
```

### Deleting from Column Families

```rust
// Delete from default CF
db.delete(&WriteOptions::default(), Slice::from("key1"))?;

// Delete from specific CF
db.delete_cf(&WriteOptions::default(), &index_cf, Slice::from("index_key"))?;
```

### Dropping Column Families

```rust
// Drop a column family
db.drop_column_family(&index_cf)?;

// Attempting to use dropped CF will error
// db.put_cf(&WriteOptions::default(), &index_cf, ...)?; // Error!
```

### Listing Column Families

```rust
// Access all column families
let cf_names = db.list_column_families();
for name in cf_names {
    println!("CF: {}", name);
}
```

## Iterator API

Range scans and ordered iteration.

### Basic Iteration

```rust
// Create iterator on default CF
let mut iter = db.iter()?;

// Iterate from start to end
if iter.seek_to_first()? {
    loop {
        let key = iter.key();
        let value = iter.value();

        println!("{} -> {}", key.to_string(), value.to_string());

        if !iter.next()? {
            break;
        }
    }
}
```

### Reverse Iteration

```rust
// Iterate from end to start
let mut iter = db.iter()?;
if iter.seek_to_last()? {
    loop {
        let key = iter.key();
        let value = iter.value();

        println!("{} -> {}", key.to_string(), value.to_string());

        if !iter.prev()? {
            break;
        }
    }
}
```

### Range Queries

```rust
// Seek to specific key
let mut iter = db.iter()?;
if iter.seek(&Slice::from("key100"))? {
    // Iterate from "key100" onwards
    while iter.valid() {
        println!("{} -> {}", iter.key().to_string(), iter.value().to_string());
        if !iter.next()? {
            break;
        }
    }
}
```

### Bounded Range Scan

```rust
let start_key = "key100";
let end_key = "key200";

let mut iter = db.iter()?;
if iter.seek(&Slice::from(start_key))? {
    while iter.valid() {
        let key = iter.key();

        // Stop if we've passed the end key
        if key.data() > end_key.as_bytes() {
            break;
        }

        println!("{} -> {}", key.to_string(), iter.value().to_string());

        if !iter.next()? {
            break;
        }
    }
}
```

### Seek for Previous

```rust
// Find largest key <= target
let mut iter = db.iter()?;
if iter.seek_for_prev(&Slice::from("key150"))? {
    // Found key <= "key150"
    println!("Found: {} -> {}",
        iter.key().to_string(),
        iter.value().to_string()
    );
}
```

### Column Family Iterators

```rust
// Create iterator on specific CF
let mut iter = db.iter_cf(&index_cf)?;

if iter.seek_to_first()? {
    loop {
        println!("{} -> {}", iter.key().to_string(), iter.value().to_string());
        if !iter.next()? {
            break;
        }
    }
}
```

### Iterator Patterns

**Count entries:**
```rust
let mut iter = db.iter()?;
let mut count = 0;

if iter.seek_to_first()? {
    loop {
        count += 1;
        if !iter.next()? {
            break;
        }
    }
}

println!("Total entries: {}", count);
```

**Prefix scan:**
```rust
let prefix = "user:";
let mut iter = db.iter()?;

if iter.seek(&Slice::from(prefix))? {
    while iter.valid() {
        let key = iter.key();

        // Stop if key doesn't have prefix
        if !key.to_string().starts_with(prefix) {
            break;
        }

        println!("{} -> {}", key.to_string(), iter.value().to_string());

        if !iter.next()? {
            break;
        }
    }
}
```

**Collect to vector:**
```rust
let mut results = Vec::new();
let mut iter = db.iter()?;

if iter.seek_to_first()? {
    loop {
        results.push((iter.key().to_vec(), iter.value().to_vec()));
        if !iter.next()? {
            break;
        }
    }
}
```

## Checkpoints

Point-in-time snapshots for backups.

### Creating Checkpoints

```rust
use rucksdb::checkpoint::Checkpoint;

// Create checkpoint
let checkpoint = Checkpoint::new(&db)?;
checkpoint.create("/path/to/checkpoint")?;

println!("Checkpoint created at /path/to/checkpoint");
```

### Opening Checkpoint as Read-Only DB

```rust
// Open checkpoint as independent database
let checkpoint_db = DB::open(
    "/path/to/checkpoint",
    DBOptions {
        create_if_missing: false,
        error_if_exists: false,
        ..Default::default()
    }
)?;

// Read from checkpoint (snapshot at checkpoint time)
let value = checkpoint_db.get(
    &ReadOptions::default(),
    &Slice::from("key1")
)?;
```

### Checkpoint Use Cases

```rust
// 1. Backup while database is running
fn backup_db(db: &DB, backup_path: &str) -> Result<()> {
    let checkpoint = Checkpoint::new(db)?;
    checkpoint.create(backup_path)?;
    println!("Backup created at {}", backup_path);
    Ok(())
}

// 2. Create read replica
fn create_replica(db: &DB, replica_path: &str) -> Result<DB> {
    let checkpoint = Checkpoint::new(db)?;
    checkpoint.create(replica_path)?;

    let replica = DB::open(replica_path, DBOptions::default())?;
    Ok(replica)
}

// 3. Point-in-time recovery
fn restore_from_checkpoint(checkpoint_path: &str, db_path: &str) -> Result<()> {
    use std::fs;

    // Remove current database
    fs::remove_dir_all(db_path)?;

    // Copy checkpoint to database path
    fs_extra::dir::copy(checkpoint_path, db_path, &Default::default())?;

    println!("Restored from checkpoint");
    Ok(())
}
```

## Statistics

Database performance metrics.

### Accessing Statistics

```rust
let stats = db.statistics();

// Operation counts
println!("Puts: {}", stats.num_puts());
println!("Gets: {}", stats.num_gets());
println!("Deletes: {}", stats.num_deletes());

// Hit rates
println!("MemTable hit rate: {:.2}%", stats.memtable_hit_rate() * 100.0);
println!("Bloom filter effectiveness: {:.2}%",
    stats.bloom_filter_effectiveness() * 100.0);

// I/O metrics
println!("WAL bytes written: {}", stats.wal_bytes_written());
println!("SSTable reads: {}", stats.num_sstable_reads());

// Compaction
println!("Compactions: {}", stats.num_compactions());
println!("Compaction bytes: {}", stats.compaction_bytes_written());
```

### Printing Statistics Report

```rust
println!("{}", db.statistics().report());
```

Output example:
```
RucksDB Statistics Report
========================

Operations:
  Puts:                    1,000,000
  Gets:                    5,000,000
  Deletes:                 100,000

MemTable:
  Hits:                    4,500,000
  Misses:                  500,000
  Hit Rate:                90.00%

WAL:
  Writes:                  1,000,000
  Bytes Written:           50 MB

SSTables:
  Reads:                   500,000
  Files:                   42

Compaction:
  Compactions Run:         15
  Bytes Written:           500 MB

Bloom Filter:
  Hits:                    450,000
  Misses:                  50,000
  Effectiveness:           90.00%

Cache:
  Block Cache Hit Rate:    85.00%
```

### Resetting Statistics

```rust
// Reset all counters to zero
db.statistics().reset();
```

### Cache Statistics

```rust
// Block cache stats
let cache_stats = db.cache_stats();
println!("Cache entries: {} / {}", cache_stats.entries, cache_stats.capacity);
println!("Cache hit rate: {:.2}%", cache_stats.hit_rate() * 100.0);
```

## Configuration

### DBOptions

Main database configuration.

```rust
use rucksdb::{DBOptions, CompressionType};

let options = DBOptions {
    // Create database if it doesn't exist
    create_if_missing: true,

    // Error if database already exists
    error_if_exists: false,

    // MemTable size limit (4MB default)
    write_buffer_size: 4 * 1024 * 1024,

    // Block cache size (number of 4KB blocks)
    block_cache_size: 1000, // ~4MB cache

    // Table cache size (number of open SSTable files)
    table_cache_size: 100,

    // Compression algorithm
    compression_type: CompressionType::Snappy,

    // Bloom filter bits per key (None to disable)
    filter_bits_per_key: Some(10), // ~1% false positive rate
};

let db = DB::open("/path/to/db", options)?;
```

### CompressionType

```rust
use rucksdb::CompressionType;

// No compression (fastest, largest size)
CompressionType::None

// Snappy (good balance, default)
CompressionType::Snappy

// LZ4 (faster compression, slightly worse ratio)
CompressionType::LZ4
```

### WriteOptions

```rust
use rucksdb::WriteOptions;

// Default: buffered write (fast)
let opts = WriteOptions { sync: false };

// Durable write: fsync immediately (slower)
let opts = WriteOptions { sync: true };
```

### ReadOptions

```rust
use rucksdb::ReadOptions;

let opts = ReadOptions::default();
```

### ColumnFamilyOptions

```rust
use rucksdb::ColumnFamilyOptions;

let cf_opts = ColumnFamilyOptions::default();

let cf = db.create_column_family("my_cf", cf_opts)?;
```

## Error Handling

### Result Type

All operations return `Result<T, Status>`.

```rust
use rucksdb::{Status, StatusCode};

match db.get(&ReadOptions::default(), &Slice::from("key1")) {
    Ok(Some(value)) => println!("Found: {}", value.to_string()),
    Ok(None) => println!("Not found"),
    Err(status) => {
        eprintln!("Error: {}", status);
        match status.code() {
            StatusCode::NotFound => eprintln!("Key not found"),
            StatusCode::IOError => eprintln!("I/O error"),
            StatusCode::Corruption => eprintln!("Data corruption detected"),
            _ => eprintln!("Other error"),
        }
    }
}
```

### Status Codes

```rust
pub enum StatusCode {
    Ok,
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IOError,
}
```

### Error Propagation

```rust
fn process_keys(db: &DB) -> Result<(), Status> {
    // ? operator propagates errors
    db.put(&WriteOptions::default(), Slice::from("k1"), Slice::from("v1"))?;
    db.put(&WriteOptions::default(), Slice::from("k2"), Slice::from("v2"))?;
    db.put(&WriteOptions::default(), Slice::from("k3"), Slice::from("v3"))?;

    Ok(())
}
```

## Complete Examples

### Simple Key-Value Store

```rust
use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database
    let db = DB::open(
        "/tmp/my_db",
        DBOptions {
            create_if_missing: true,
            ..Default::default()
        }
    )?;

    // Write data
    db.put(&WriteOptions::default(), Slice::from("name"), Slice::from("Alice"))?;
    db.put(&WriteOptions::default(), Slice::from("age"), Slice::from("30"))?;
    db.put(&WriteOptions::default(), Slice::from("city"), Slice::from("NYC"))?;

    // Read data
    if let Some(name) = db.get(&ReadOptions::default(), &Slice::from("name"))? {
        println!("Name: {}", name.to_string());
    }

    // Delete data
    db.delete(&WriteOptions::default(), Slice::from("age"))?;

    // Scan all keys
    let mut iter = db.iter()?;
    if iter.seek_to_first()? {
        loop {
            println!("{} = {}", iter.key().to_string(), iter.value().to_string());
            if !iter.next()? {
                break;
            }
        }
    }

    Ok(())
}
```

### Multi-Column Family Application

```rust
use rucksdb::{
    ColumnFamilyOptions, DB, DBOptions, ReadOptions, Slice, WriteOptions
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database
    let db = DB::open("/tmp/multi_cf_db", DBOptions::default())?;

    // Create column families
    let users_cf = db.create_column_family("users", ColumnFamilyOptions::default())?;
    let posts_cf = db.create_column_family("posts", ColumnFamilyOptions::default())?;

    // Write to different CFs
    db.put_cf(
        &WriteOptions::default(),
        &users_cf,
        Slice::from("user:1"),
        Slice::from(r#"{"name": "Alice", "email": "alice@example.com"}"#),
    )?;

    db.put_cf(
        &WriteOptions::default(),
        &posts_cf,
        Slice::from("post:1"),
        Slice::from(r#"{"title": "Hello World", "author": "user:1"}"#),
    )?;

    // Read from specific CF
    if let Some(user) = db.get_cf(&ReadOptions::default(), &users_cf, &Slice::from("user:1"))? {
        println!("User: {}", user.to_string());
    }

    // Iterate CF
    let mut iter = db.iter_cf(&posts_cf)?;
    if iter.seek_to_first()? {
        loop {
            println!("Post: {} = {}", iter.key().to_string(), iter.value().to_string());
            if !iter.next()? {
                break;
            }
        }
    }

    Ok(())
}
```

### High-Performance Bulk Load

```rust
use rucksdb::{DB, DBOptions, CompressionType, Slice, WriteOptions};

fn bulk_load(path: &str, num_keys: usize) -> Result<(), Box<dyn std::error::Error>> {
    let options = DBOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024 * 1024, // 64MB buffer
        block_cache_size: 10000,              // Large cache
        compression_type: CompressionType::LZ4, // Fast compression
        ..Default::default()
    };

    let db = DB::open(path, options)?;
    let write_opts = WriteOptions { sync: false }; // Buffered writes

    // Bulk write
    for i in 0..num_keys {
        let key = format!("key{:010}", i);
        let value = format!("value{:010}", i);

        db.put(&write_opts, Slice::from(key), Slice::from(value))?;

        if i % 10000 == 0 {
            println!("Loaded {} keys", i);
        }
    }

    println!("Bulk load complete: {} keys", num_keys);
    println!("{}", db.statistics().report());

    Ok(())
}
```

## Best Practices

### 1. Use Appropriate Options

```rust
// High write throughput
let write_heavy = DBOptions {
    write_buffer_size: 64 * 1024 * 1024, // Large buffer
    compression_type: CompressionType::LZ4, // Fast compression
    filter_bits_per_key: None, // Skip bloom filters
    ..Default::default()
};

// Read-optimized
let read_heavy = DBOptions {
    block_cache_size: 10000, // Large cache
    table_cache_size: 500,   // Many open files
    filter_bits_per_key: Some(10), // Enable bloom filters
    ..Default::default()
};
```

### 2. Batch Writes

```rust
// Efficient: batch multiple operations
for i in 0..1000 {
    db.put(&WriteOptions { sync: false }, ...)?;
}
// Sync once at end if needed
db.put(&WriteOptions { sync: true }, ...)?;

// Inefficient: sync every write
for i in 0..1000 {
    db.put(&WriteOptions { sync: true }, ...)?; // Slow!
}
```

### 3. Use Iterators for Range Scans

```rust
// Efficient: iterator
let mut iter = db.iter()?;
if iter.seek(&Slice::from("start"))? {
    // Scan range
}

// Inefficient: individual gets
for i in 0..1000 {
    db.get(&ReadOptions::default(), &Slice::from(format!("key{}", i)))?;
}
```

### 4. Monitor Statistics

```rust
// Periodically check health
if db.statistics().memtable_hit_rate() < 0.8 {
    eprintln!("Warning: Low MemTable hit rate!");
}

if db.statistics().bloom_filter_effectiveness() < 0.9 {
    eprintln!("Warning: Bloom filters not effective!");
}
```

### 5. Use Column Families for Logical Separation

```rust
// Separate concerns
let metadata_cf = db.create_column_family("metadata", ...)?;
let data_cf = db.create_column_family("data", ...)?;

// Independent compaction and configuration per CF
```

## Further Reading

- [Architecture Guide](./ARCHITECTURE.md) - System internals
- [Performance Tuning Guide](./PERFORMANCE.md) - Optimization tips
- [Examples](../examples/) - More code examples
