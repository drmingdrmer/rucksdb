# Fuzzing RucksDB

This directory contains fuzz targets for testing RucksDB with [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).

## Fuzz Targets

### 1. `internal_key`
Tests InternalKey encoding/decoding with arbitrary byte sequences. This target specifically validates the fix for the null byte bug discovered in Phase 12.

**Focus**: Ensures any byte sequence (including those with `0x00` bytes) can be safely encoded and decoded.

```bash
cargo fuzz run internal_key
```

### 2. `db_operations`
Tests basic database operations (put/get/delete) with random key-value pairs.

**Focus**: Core database functionality and edge cases in key-value operations.

```bash
cargo fuzz run db_operations
```

### 3. `write_batch`
Tests atomic batch operations with arbitrary sequences of puts, deletes, and merges.

**Focus**: WriteBatch correctness and atomicity guarantees.

```bash
cargo fuzz run write_batch
```

## Running Fuzzing Campaigns

### Quick Test (60 seconds)
```bash
cargo fuzz run <target> -- -max_total_time=60
```

### Extended Campaign (1 hour)
```bash
cargo fuzz run <target> -- -max_total_time=3600
```

### Continuous Fuzzing
```bash
cargo fuzz run <target>
```

### Parallel Fuzzing
```bash
cargo fuzz run <target> -- -jobs=4
```

## Viewing Coverage
```bash
cargo fuzz coverage <target>
```

## Analyzing Crashes

If a crash is found, artifacts are saved to `fuzz/artifacts/<target>/`:

```bash
# Reproduce a specific crash
cargo fuzz run <target> fuzz/artifacts/<target>/crash-xyz

# Minimize a crashing input
cargo fuzz cmin <target>
```

## Tips

- Fuzzing generates temporary database directories in `/tmp/rucksdb_fuzz_*`
- Corpus files are saved to `fuzz/corpus/<target>/`
- Start with short runs (60s) to verify fuzzer setup
- For continuous fuzzing, use `tmux` or `screen` sessions
- Monitor system resources - fuzzing is CPU-intensive

## Integration with CI

Fuzzing is not run in CI due to resource requirements. Developers should run fuzzing campaigns locally when:
- Making changes to core data structures
- Implementing new database operations
- Fixing encoding/decoding bugs
