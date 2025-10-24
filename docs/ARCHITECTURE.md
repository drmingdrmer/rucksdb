# RucksDB Architecture Guide

## Overview

RucksDB is a Rust implementation of an [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) based key-value storage engine, inspired by [RocksDB](https://rocksdb.org/) and [LevelDB](https://github.com/google/leveldb). It provides high write throughput through sequential writes, efficient reads via multi-level caching, and horizontal scalability through LSM-tree design.

**Key Features:**
- ACID durability via Write-Ahead Log (WAL)
- Multi-Version Concurrency Control (MVCC)
- Bloom filters for efficient reads
- Block-level compression (Snappy/LZ4)
- Column Family support for logical data partitioning
- Iterator API for range scans
- Point-in-time snapshots via Checkpoint

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client API                          │
│  (DB::open, put, get, delete, iter, checkpoint, stats)     │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────┴────────────────────────────────────────┐
│                    Database (DB)                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   MemTable   │  │  Immutable   │  │     WAL      │     │
│  │  (Active)    │  │   MemTable   │  │ (Write-Ahead │     │
│  │              │  │  (Flushing)  │  │     Log)     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              ColumnFamilySet                         │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │  │
│  │  │ CF: default │  │ CF: index   │  │ CF: data    │ │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘ │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              VersionSet                              │  │
│  │  Manages SSTable file versions and compaction       │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────┬────────────────────────────────────┬─┘
                      │                                    │
            ┌─────────┴─────────┐              ┌──────────┴────────┐
            │   LSM-Tree Levels │              │   Cache Layer     │
            │                   │              │                   │
            │  Level 0: 4 files │              │  Block Cache      │
            │  Level 1: 10 files│              │  Table Cache      │
            │  Level 2: 100 files              │  (LRU)            │
            │  ...              │              │                   │
            └───────────────────┘              └───────────────────┘
                      │
            ┌─────────┴─────────┐
            │  Persistent Layer │
            │                   │
            │  ┌──────────────┐ │
            │  │  SSTables    │ │
            │  │  (*.sst)     │ │
            │  └──────────────┘ │
            │  ┌──────────────┐ │
            │  │  MANIFEST    │ │
            │  │  CURRENT     │ │
            │  └──────────────┘ │
            │  ┌──────────────┐ │
            │  │  WAL Log     │ │
            │  │  (*.log)     │ │
            │  └──────────────┘ │
            └───────────────────┘
```

## Core Components

### 1. MemTable

The active in-memory write buffer using a lock-free skip list.

**Implementation:** `src/memtable/memtable.rs`

**Key Features:**
- **Data Structure:** [crossbeam-skiplist](https://docs.rs/crossbeam-skiplist/) for O(log n) concurrent operations
- **InternalKey Format:** `user_key + 0x00 + reversed_seq + type`
  - Sequence numbers enable MVCC (newer writes shadow older ones)
  - Type byte: 0x01 (value) or 0x00 (deletion/tombstone)
- **Concurrency:** Lock-free reads and writes via atomic operations
- **Size Limit:** Configurable (default 4MB via `write_buffer_size`)

**Write Path:**
1. Encode key as InternalKey with monotonic sequence number
2. Insert into skip list (O(log n))
3. When size exceeds limit, convert to Immutable MemTable
4. Background thread flushes Immutable MemTable to SSTable

**Read Path:**
1. Search skip list for key prefix (O(log n))
2. Return latest version (highest sequence number)
3. Skip deletion markers

### 2. Write-Ahead Log (WAL)

Ensures durability by logging writes before applying to MemTable.

**Implementation:** `src/wal/`

**File Format:**
```
┌─────────────┬─────────────┬─────────────┬───────┐
│  Block 0    │  Block 1    │  Block 2    │  ...  │
│  (32KB)     │  (32KB)     │  (32KB)     │       │
└─────────────┴─────────────┴─────────────┴───────┘

Block Structure:
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│ CRC32    │ Length   │ Type     │ Data     │ Trailer  │
│ (4 bytes)│ (2 bytes)│ (1 byte) │ (N bytes)│ (padding)│
└──────────┴──────────┴──────────┴──────────┴──────────┘

Record Types:
- Full (0x01): Complete record fits in one block
- First (0x02): First fragment of multi-block record
- Middle (0x03): Middle fragment
- Last (0x04): Last fragment
```

**Write Record Format:**
```
┌─────────┬────────┬────────────┬────────┬───────┐
│ CF ID   │ Key Len│ Key        │ Val Len│ Value │
│ (4B)    │ (4B)   │ (variable) │ (4B)   │ (var) │
└─────────┴────────┴────────────┴────────┴───────┘
```

**Recovery Process:**
1. Read MANIFEST to get last sequence number
2. Scan WAL blocks sequentially
3. Validate CRC32 for each record
4. Replay valid records to MemTables (per Column Family)
5. Skip corrupted trailing records
6. Resume from recovered sequence number

### 3. SSTable (Sorted String Table)

Immutable on-disk sorted key-value files.

**Implementation:** `src/table/`

**File Structure:**
```
┌──────────────────────────────────────────────────┐
│              Data Blocks (4KB each)              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐│
│  │ Block 0    │  │ Block 1    │  │ Block N    ││
│  │ key1:val1  │  │ key100:val │  │ key999:val ││
│  │ key2:val2  │  │ ...        │  │ ...        ││
│  └────────────┘  └────────────┘  └────────────┘│
├──────────────────────────────────────────────────┤
│              Meta Index Block                    │
│  ┌────────────────────────────────────────────┐ │
│  │  "filter.bloomfilter" -> offset, size      │ │
│  └────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────┤
│              Filter Block (Bloom)                │
│  ┌────────────────────────────────────────────┐ │
│  │  Bloom filter: k=7 hash functions         │ │
│  │  ~1% false positive rate at 10 bits/key   │ │
│  └────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────┤
│              Index Block                         │
│  ┌────────────────────────────────────────────┐ │
│  │  last_key_in_block_0 -> block_0_offset    │ │
│  │  last_key_in_block_1 -> block_1_offset    │ │
│  │  ...                                       │ │
│  └────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────┤
│              Footer (48 bytes)                   │
│  ┌─────────────┬─────────────┬───────────────┐ │
│  │ Meta Index │ Index Block │ Magic Number  │ │
│  │ Handle     │ Handle      │ (8 bytes)     │ │
│  │ (20 bytes) │ (20 bytes)  │ 0x88e3f3fb... │ │
│  └─────────────┴─────────────┴───────────────┘ │
└──────────────────────────────────────────────────┘
```

**Data Block Format:**
```
Restart Points every 16 entries:
┌────────────────────────────────────────────┐
│ Entry 0: "userkey001" -> "value001"       │  <- Restart point
│ Entry 1: Δ"002" -> "value002"             │  (prefix compression)
│ ...                                        │
│ Entry 15: Δ"016" -> "value016"            │
│ Entry 16: "userkey017" -> "value017"      │  <- Restart point
│ ...                                        │
│ [Restart offsets array]                   │
│ [Num restart points: 4 bytes]             │
└────────────────────────────────────────────┘
```

**Read Path:**
1. **Check Table Cache** (LRU cache of opened TableReader)
   - Hit: Reuse cached reader (O(1))
   - Miss: Open file, parse footer/index/filter (expensive)
2. **Bloom Filter Check** (~1% false positive)
   - Negative: Key definitely not present, return None
   - Positive: Key might be present, proceed
3. **Index Block Lookup** (binary search)
   - Find data block containing key range
4. **Data Block Read**
   - Read 4KB block from disk (or block cache)
   - Decompress if compressed
   - Binary search restart points
   - Linear scan with prefix compression
5. **Return value or None**

### 4. LSM-Tree Levels & Compaction

Multi-level structure for organizing SSTables.

**Implementation:** `src/version/version_set.rs`

**Level Structure:**
```
Level 0: 4 files (10MB total) - Overlapping key ranges
  ├─ 000123.sst [a-m]
  ├─ 000124.sst [d-p]
  ├─ 000125.sst [f-z]
  └─ 000126.sst [b-k]

Level 1: 10 files (100MB total) - Non-overlapping
  ├─ 000100.sst [a-c]
  ├─ 000101.sst [d-f]
  ├─ 000102.sst [g-i]
  └─ ...

Level 2: 100 files (1GB total) - Non-overlapping
  └─ ...

Size limits: Level N = 10^N MB
```

**Compaction Trigger:**
- Level 0: >= 4 files (trigger on count)
- Level N: >= size limit (trigger on total size)

**Compaction Process:**
1. **Pick Files:** Select files from Level N and overlapping files from Level N+1
2. **Merge Sort:** Multi-way merge of sorted files
3. **Apply MVCC:** Keep only latest version of each key
4. **Write Level N+1:** Create new SSTables (max 2MB each)
5. **Update MANIFEST:** Log version edit atomically
6. **Delete Old Files:** Remove compacted files

**MANIFEST File:**
```
Format: Log of VersionEdit records

VersionEdit Tags:
- Tag 1: Comparator name
- Tag 2: Log number
- Tag 3: Next file number
- Tag 4: Last sequence number
- Tag 5: Compact pointer (level, key)
- Tag 6: Deleted file (level, file_number)
- Tag 7: New file (level, file_number, size, smallest, largest)
- Tag 8: Column family create (cf_id, name)
- Tag 9: Column family drop (cf_id)
```

### 5. Column Families

Logical partitioning of keyspace with independent LSM-trees.

**Implementation:** `src/column_family/`

**Architecture:**
```
Database
  ├─ ColumnFamilySet
  │    ├─ ColumnFamily "default" (ID=0)
  │    │    ├─ MemTable
  │    │    ├─ VersionSet (Level 0-6)
  │    │    └─ Options
  │    ├─ ColumnFamily "index" (ID=1)
  │    └─ ColumnFamily "data" (ID=2)
  └─ Shared WAL (all CFs write to same log)
```

**WAL Multi-CF Format:**
```
┌─────────┬────────┬────────────┬────────┬───────┐
│ CF ID   │ Key Len│ Key        │ Val Len│ Value │
│ (4B)    │ (4B)   │ (variable) │ (4B)   │ (var) │
└─────────┴────────┴────────────┴────────┴───────┘
```

**Recovery:**
1. Read MANIFEST to recover Column Family metadata
2. Replay WAL, routing records by CF ID
3. Handle missing CFs gracefully (skip records)

### 6. Iterator

Unified interface for scanning data across all layers.

**Implementation:** `src/iterator/`

**Iterator Hierarchy:**
```
MergingIterator (DB-level)
  ├─ MemTableIterator (active writes)
  ├─ MemTableIterator (immutable)
  ├─ TableIterator (Level 0, file 1)
  ├─ TableIterator (Level 0, file 2)
  ├─ TableIterator (Level 1, file 1)
  └─ ...
```

**MergingIterator Algorithm:**
- **Data Structure:** Min-heap of (key, iterator_index)
- **Priority:** Lower index = higher priority (newer data shadows older)
- **Duplicate Handling:** Skip old versions of same user key
- **Deletion Markers:** Filter tombstones from results
- **Complexity:** O(log k) per next() where k = number of iterators

**Seek Operations:**
- `seek_to_first()`: Position all iterators at start
- `seek(key)`: Binary search to key >= target
- `seek_for_prev(key)`: Find key <= target
- `next()`: Advance to next unique key

### 7. Cache Layers

Two-level caching for performance.

**Implementation:** `src/cache/`

**Block Cache (LRU):**
- **Key:** (file_number, block_offset)
- **Value:** Decompressed 4KB data block
- **Size:** Configurable via `block_cache_size`
- **Eviction:** Least Recently Used
- **Hit Rate:** ~80-95% for hot datasets

**Table Cache (LRU):**
- **Key:** file_number
- **Value:** Arc<Mutex<TableReader>> (opened file + parsed metadata)
- **Size:** Default 100 files
- **Purpose:** Avoid repeated file open/parse overhead
- **Impact:** 1.8x random read improvement (2.4K -> 4.3K ops/sec)

### 8. Statistics

Database-wide performance metrics.

**Implementation:** `src/statistics/`

**Metrics:**
```rust
pub struct Statistics {
    // Operations
    pub num_puts: AtomicU64,
    pub num_gets: AtomicU64,
    pub num_deletes: AtomicU64,

    // MemTable
    pub memtable_hits: AtomicU64,
    pub memtable_misses: AtomicU64,

    // WAL
    pub wal_writes: AtomicU64,
    pub wal_bytes_written: AtomicU64,

    // SSTable
    pub sstable_reads: AtomicU64,
    pub num_sstable_files: AtomicU64,

    // Compaction
    pub num_compactions: AtomicU64,
    pub compaction_bytes_written: AtomicU64,

    // Bloom Filter
    pub bloom_filter_hits: AtomicU64,
    pub bloom_filter_misses: AtomicU64,
}
```

**Tracking:** Automatic tracking via instrumented put/get/delete operations

## Key Design Decisions

### 1. MVCC via InternalKey

**Problem:** How to support concurrent reads without blocking writes?

**Solution:** Encode sequence number into key:
```
InternalKey = user_key + 0x00 + reversed_seq + type
              ^^^^^^^^     ^^^   ^^^^^^^^^^^^^   ^^^^
              User data    Sep   MVCC version    Value/Delete
```

**Benefits:**
- Readers see consistent snapshot at their sequence number
- Writers don't block readers (new versions written alongside)
- Natural support for iterators (sorted by key, then reverse time)

**Trade-off:** Old versions accumulate, require compaction to reclaim space

### 2. Skip List for MemTable

**Alternatives Considered:**
- B-Tree: Good locality but complex concurrent implementation
- Hash Table: O(1) lookup but no range scan support
- Red-Black Tree: Requires global lock

**Why Skip List:**
- Lock-free concurrent access (via crossbeam-skiplist)
- O(log n) operations with good constant factors
- Natural ordered iteration for flush/iterator
- Memory efficient (no tree rebalancing)

### 3. Prefix Compression in Data Blocks

**Example:**
```
Without compression:
  userkey001: 10 bytes
  userkey002: 10 bytes
  userkey003: 10 bytes
  Total: 30 bytes

With compression:
  userkey001: 10 bytes (restart point)
  Δ002: 3 bytes (shared prefix = "userkey00")
  Δ003: 3 bytes
  Total: 16 bytes (53% space saving)
```

**Restart Points (every 16 entries):**
- Allow binary search without decompressing entire block
- Trade-off between compression ratio and read performance

### 4. Bloom Filters

**Parameters:**
- **Bits per key:** 10 bits
- **Hash functions:** k = bits_per_key × 0.69 ≈ 7
- **False positive rate:** ~1% at 10 bits/key

**Cost-Benefit:**
- **Storage:** 10 bits × num_keys (e.g., 1MB for 1M keys)
- **Benefit:** Skip 99% of non-existent key lookups
- **ROI:** Avoids expensive disk reads for $0.01 of RAM

### 5. WAL Block Size (32KB)

**Why 32KB:**
- Amortize syscall overhead (write many records per syscall)
- Balance between write amplification and recovery time
- Match filesystem page size for efficient I/O
- Small enough for reasonable recovery latency

### 6. SSTable Block Size (4KB)

**Why 4KB:**
- Match OS page size for efficient disk I/O
- Granularity for block cache (cache hot blocks, not entire file)
- Balance between compression ratio and random read performance
- Restart points allow binary search within block

## Data Flow Examples

### Write Path

```
1. Client: db.put("key1", "value1")
   ↓
2. DB: Append to WAL
   ├─ Encode: CF_ID + key + value
   ├─ Write to 32KB buffer
   └─ fsync() if sync=true
   ↓
3. DB: Insert into MemTable
   ├─ Create InternalKey: "key1\x00" + reversed_seq + 0x01
   ├─ Insert into skip list: O(log n)
   └─ Increment sequence number atomically
   ↓
4. Background: MemTable → SSTable (when size > 4MB)
   ├─ Convert to Immutable MemTable
   ├─ Create TableBuilder
   ├─ Write sorted entries with prefix compression
   ├─ Build bloom filter
   ├─ Write index block, footer
   └─ Register with VersionSet
   ↓
5. Background: Compaction (when Level 0 >= 4 files)
   ├─ Pick files from Level 0 and Level 1
   ├─ Multi-way merge sort
   ├─ Write new Level 1 files (2MB each)
   ├─ Update MANIFEST
   └─ Delete old files
```

### Read Path

```
1. Client: db.get("key1")
   ↓
2. DB: Search MemTable
   ├─ Encode: "key1\x00"
   ├─ Skip list range lookup: O(log n)
   ├─ Return if found (latest version)
   └─ Otherwise continue
   ↓
3. DB: Search Immutable MemTable
   └─ Same as step 2
   ↓
4. DB: Search SSTables (Level 0 → Level N)
   ├─ For each level:
   │   ├─ Get TableReader from Table Cache
   │   │   ├─ Hit: Reuse cached (O(1))
   │   │   └─ Miss: Open file, parse metadata
   │   ├─ Check Bloom Filter
   │   │   ├─ Negative: Skip file
   │   │   └─ Positive: Continue
   │   ├─ Binary search Index Block
   │   ├─ Read Data Block (check Block Cache first)
   │   ├─ Decompress if needed
   │   └─ Binary search + linear scan
   └─ Return first found (newest version)
   ↓
5. Return value or None
```

### Iterator Scan

```
1. Client: iter = db.iter()
   ↓
2. DB: Create MergingIterator
   ├─ Child: MemTableIterator (active)
   ├─ Child: MemTableIterator (immutable)
   ├─ Child: TableIterator (Level 0, file 1)
   ├─ Child: TableIterator (Level 0, file 2)
   └─ ... (all SSTables)
   ↓
3. iter.seek_to_first()
   ├─ Position all child iterators at start
   ├─ Build min-heap of (key, iterator_index)
   └─ Return smallest key (highest priority)
   ↓
4. Loop: iter.next()
   ├─ Pop min from heap
   ├─ Advance that iterator until user key changes
   ├─ Skip all other iterators with same user key
   ├─ Skip deletion markers
   └─ Return next unique key
```

## Performance Characteristics

### Throughput

| Operation | Throughput | Latency (P99) |
|-----------|------------|---------------|
| Sequential Write | 105K ops/sec | 10μs |
| Random Read | 4.3K ops/sec | 366μs |
| Sequential Read (iterator) | 773K ops/sec | <1μs |
| Checkpoint | 1K keys/15ms | 15ms |

### Space Amplification

```
Write Amplification = Total bytes written / User bytes written
                   ≈ 10-20x (LSM-tree characteristic)

Breakdown:
- WAL write: 1x
- MemTable flush: 1x
- Level 0→1 compaction: ~5x (read 1, write 5)
- Level 1→2 compaction: ~10x
- ...
```

### Tuning Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `write_buffer_size` | 4MB | MemTable size limit |
| `block_cache_size` | 1000 blocks | Number of 4KB blocks cached |
| `table_cache_size` | 100 files | Number of opened SSTables |
| `filter_bits_per_key` | 10 bits | Bloom filter size |
| `compression_type` | Snappy | Compression algorithm |

## Concurrency Model

### Lock Hierarchy

```
Global Locks:
  1. DB::mutex (protects VersionSet, MANIFEST)
  2. MemTable::RwLock (write lock only for flush)

Lock-Free:
  - MemTable reads/writes (skip list)
  - Statistics counters (AtomicU64)
  - Block cache (internal RwLock)
  - Table cache (internal RwLock)
```

### Background Threads

```
Thread 1: Flush Thread
  - Monitors MemTable size
  - Converts active → immutable
  - Flushes immutable → SSTable

Thread 2: Compaction Thread
  - Monitors level sizes
  - Picks files for compaction
  - Merges and writes new files
  - Updates MANIFEST
```

## Testing Strategy

### Test Categories

1. **Unit Tests** (131 tests)
   - Component-level testing
   - src/**/tests/ modules

2. **Integration Tests** (14 tests)
   - Multi-component workflows
   - tests/integration_test.rs

3. **Crash Recovery Tests** (8 tests)
   - WAL recovery validation
   - tests/crash_recovery_test.rs

4. **Property-Based Tests** (7 tests)
   - Randomized testing with proptest
   - tests/property_test.rs

5. **Stress Tests** (8 tests)
   - Concurrent operations
   - tests/stress_test.rs

6. **Performance Tests** (4 tests)
   - Throughput benchmarks
   - tests/performance_analysis.rs

### Test Coverage

- **Total Tests:** 201
- **LOC Coverage:** >80%
- **Critical Paths:** 100% (WAL recovery, MVCC, compaction)

## Further Reading

- [LSM-Tree: Wikipedia](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
- [LevelDB Documentation](https://github.com/google/leveldb/blob/main/doc/index.md)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [The Log-Structured Merge-Tree (LSM-Tree) - O'Neil et al., 1996](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
