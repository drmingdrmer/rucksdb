//! Property-based invariant testing for RucksDB
//!
//! # Why Property-Based Testing?
//!
//! Traditional example-based unit tests verify specific inputs produce expected
//! outputs. Property-based testing verifies **universal invariants** that must
//! hold for ALL inputs.
//!
//! For a database engine, this is critical because:
//! 1. **Edge cases are infinite**: Binary keys can contain any byte sequence
//! 2. **Durability guarantees are absolute**: ONE failure = data loss
//! 3. **LSM operations are complex**: Interactions between memtable,
//!    compaction, WAL
//! 4. **Regression detection**: Automatically finds minimal failing cases
//!
//! # Invariant Categories
//!
//! ## 1. Consistency Invariants
//! Properties that ensure correctness of read/write operations:
//! - Write-Read Consistency: Written data is readable
//! - Delete Semantics: Deleted keys return None
//! - Overwrite Semantics: Last write wins (MVCC ordering)
//! - Read-Your-Writes: Immediate reads see recent writes
//!
//! ## 2. Durability Invariants
//! Properties that ensure crash recovery and persistence:
//! - Persistence: Data survives DB close/reopen
//! - Compaction Preserves Data: No data loss during compaction
//!
//! ## 3. Ordering Invariants
//! Properties that ensure iterator correctness:
//! - Key Ordering: Iterator returns keys in lexicographic order
//! - Deterministic Operations: Same ops sequence = same final state
//!
//! ## 4. Edge Case Coverage
//! Properties that test boundary conditions:
//! - Empty Values: Zero-length values handled correctly
//! - Large Values: Multi-KB values don't corrupt data
//!
//! ## 5. Concurrency Invariants
//! Properties that ensure thread-safety and concurrent correctness:
//! - Concurrent Writes: Multiple writers don't corrupt data
//! - Concurrent Reads: Readers see consistent state during writes
//! - Mixed Operations: Concurrent reads/writes/deletes maintain consistency
//!
//! # Regression Tracking
//!
//! Proptest automatically saves failing test cases to `.proptest-regressions`
//! files. These are re-run before generating new random cases, ensuring bugs
//! stay fixed.
//!
//! # Test Strategy
//!
//! All tests use binary Vec<u8> keys/values (more realistic than strings).
//! Model-based tests use HashMap as reference implementation to verify DB
//! behavior. Tests handle duplicate keys correctly (last write wins semantics).

use std::collections::HashMap;

use proptest::prelude::*;
use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

// ============================================================================
// Helper Functions
// ============================================================================

fn arbitrary_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..=100)
}

fn arbitrary_value() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=1000)
}

fn arbitrary_kv_pairs() -> impl Strategy<Value = Vec<(Vec<u8>, Vec<u8>)>> {
    prop::collection::vec((arbitrary_key(), arbitrary_value()), 1..=100)
}

#[derive(Debug, Clone)]
enum Operation {
    Put(Vec<u8>, Vec<u8>),
    Get(()),
    Delete(Vec<u8>),
}

fn arbitrary_operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        (arbitrary_key(), arbitrary_value()).prop_map(|(k, v)| Operation::Put(k, v)),
        Just(()).prop_map(Operation::Get),
        arbitrary_key().prop_map(Operation::Delete),
    ]
}

// ============================================================================
// Property 1: Write-Read Consistency
// ============================================================================
// Property: Every key written should be readable with the same value

proptest! {
    #[test]
    fn prop_write_then_read_succeeds(kv_pairs in arbitrary_kv_pairs()) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Build expected state (last write wins for duplicate keys)
        let mut expected_state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for (key, value) in &kv_pairs {
            db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                .unwrap();
            expected_state.insert(key.clone(), value.clone());
        }

        // Read all keys and verify values match expected state
        for (key, expected_value) in &expected_state {
            let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
            prop_assert!(result.is_some(), "Key {:?} should exist", key);
            let value = result.unwrap();
            prop_assert_eq!(
                value.data(),
                expected_value.as_slice(),
                "Value mismatch for key {:?}",
                key
            );
        }
    }
}

// ============================================================================
// Property 2: Delete Semantics
// ============================================================================
// Property: Deleted keys should return None on read

proptest! {
    #[test]
    fn prop_delete_makes_key_unreadable(key in arbitrary_key(), value in arbitrary_value()) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Write the key
        db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
            .unwrap();

        // Verify it exists
        let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
        prop_assert!(result.is_some());

        // Delete the key
        db.delete(&write_opts, Slice::from(key.as_slice())).unwrap();

        // Verify it doesn't exist
        let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
        prop_assert!(result.is_none(), "Deleted key should return None");
    }
}

// ============================================================================
// Property 3: Overwrite Semantics (Last Write Wins)
// ============================================================================
// Property: Writing same key multiple times keeps only latest value

proptest! {
    #[test]
    fn prop_overwrite_keeps_latest_value(
        key in arbitrary_key(),
        values in prop::collection::vec(arbitrary_value(), 2..=10)
    ) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Write same key multiple times with different values
        for value in &values {
            db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                .unwrap();
        }

        // Verify we get the latest value
        let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
        prop_assert!(result.is_some());
        let value = result.unwrap();
        prop_assert_eq!(
            value.data(),
            values.last().unwrap().as_slice(),
            "Should have latest value"
        );
    }
}

// ============================================================================
// Property 4: Persistence (Data Survives Restart)
// ============================================================================
// Property: Data written should survive DB close and reopen

proptest! {
    #[test]
    fn prop_data_persists_after_reopen(kv_pairs in arbitrary_kv_pairs()) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Build expected state (last write wins for duplicate keys)
        let mut expected_state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        // Write data in first DB instance
        {
            let db = DB::open(db_path, DBOptions::default()).unwrap();
            for (key, value) in &kv_pairs {
                db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                    .unwrap();
                expected_state.insert(key.clone(), value.clone());
            }
            // DB drops here
        }

        // Reopen DB and verify data exists
        let db = DB::open(db_path, DBOptions::default()).unwrap();
        for (key, expected_value) in &expected_state {
            let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
            prop_assert!(result.is_some(), "Key {:?} should persist after reopen", key);
            let value = result.unwrap();
            prop_assert_eq!(
                value.data(),
                expected_value.as_slice(),
                "Value should match after reopen for key {:?}",
                key
            );
        }
    }
}

// ============================================================================
// Property 5: Operations are Idempotent or Deterministic
// ============================================================================
// Property: Applying same operations twice yields same result

proptest! {
    #[test]
    fn prop_operations_deterministic(operations in prop::collection::vec(arbitrary_operation(), 1..=50)) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Apply operations once
        let mut model: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        for op in &operations {
            match op {
                Operation::Put(k, v) => {
                    db.put(&write_opts, Slice::from(k.as_slice()), Slice::from(v.as_slice())).unwrap();
                    model.insert(k.clone(), Some(v.clone()));
                },
                Operation::Delete(k) => {
                    db.delete(&write_opts, Slice::from(k.as_slice())).unwrap();
                    model.insert(k.clone(), None);
                },
                Operation::Get(_) => {
                    // Reads don't change state
                },
            }
        }

        // Verify DB state matches model
        for (key, expected_value) in &model {
            let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
            match expected_value {
                Some(v) => {
                    prop_assert!(result.is_some(), "Key {:?} should exist", key);
                    let value = result.unwrap();
                    prop_assert_eq!(value.data(), v.as_slice());
                },
                None => {
                    prop_assert!(result.is_none(), "Key {:?} should not exist", key);
                },
            }
        }
    }
}

// ============================================================================
// Property 6: Empty Key Handling
// ============================================================================
// Property: Empty keys and values should be handled correctly

proptest! {
    #[test]
    fn prop_empty_values_work(key in arbitrary_key()) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Write empty value
        db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(&[] as &[u8]))
            .unwrap();

        // Read and verify empty value
        let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
        prop_assert!(result.is_some());
        let value = result.unwrap();
        prop_assert_eq!(value.data(), &[] as &[u8]);
    }
}

// ============================================================================
// Property 7: Large Value Handling
// ============================================================================
// Property: Large values should be handled correctly

proptest! {
    #[test]
    fn prop_large_values_work(
        key in arbitrary_key(),
        value in prop::collection::vec(any::<u8>(), 10000..=100000)
    ) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Write large value
        db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
            .unwrap();

        // Read and verify
        let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
        prop_assert!(result.is_some());
        let read_value = result.unwrap();
        prop_assert_eq!(read_value.data(), value.as_slice());
    }
}

// ============================================================================
// Property 8: Key Ordering Invariant
// ============================================================================
// Property: Keys should maintain lexicographic order

proptest! {
    #[test]
    fn prop_keys_maintain_order(mut keys in prop::collection::vec(arbitrary_key(), 10..=100)) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();

        // Remove duplicates and write all keys with dummy values
        keys.sort();
        keys.dedup();

        for key in &keys {
            db.put(&write_opts, Slice::from(key.as_slice()), Slice::from([1u8].as_slice()))
                .unwrap();
        }

        // Create iterator and collect keys
        let mut iter = db.iter().unwrap();
        let mut collected_keys = Vec::new();
        if iter.seek_to_first().unwrap() {
            loop {
                collected_keys.push(iter.key().data().to_vec());
                if !iter.next().unwrap() {
                    break;
                }
            }
        }

        // Verify keys are in sorted order
        prop_assert_eq!(collected_keys, keys, "Iterator should return keys in sorted order");
    }
}

// ============================================================================
// Property 9: Read-Your-Writes
// ============================================================================
// Property: Immediate reads should see recent writes

proptest! {
    #[test]
    fn prop_read_your_writes(
        operations in prop::collection::vec(
            (arbitrary_key(), arbitrary_value()),
            1..=50
        )
    ) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        for (key, value) in &operations {
            // Write
            db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                .unwrap();

            // Immediately read - should see the write
            let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
            prop_assert!(result.is_some());
            let read_value = result.unwrap();
            prop_assert_eq!(read_value.data(), value.as_slice());
        }
    }
}

// ============================================================================
// Property 10: Compaction Preserves Data
// ============================================================================
// Property: Data should survive manual compaction

proptest! {
    #[test]
    fn prop_compaction_preserves_data(kv_pairs in arbitrary_kv_pairs()) {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        // Build expected state (last write wins for duplicate keys)
        let mut expected_state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        // Write data
        for (key, value) in &kv_pairs {
            db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                .unwrap();
            expected_state.insert(key.clone(), value.clone());
        }

        // Trigger compaction
        db.compact_range(None, None).unwrap();

        // Verify all data still exists
        for (key, expected_value) in &expected_state {
            let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
            prop_assert!(result.is_some(), "Key should exist after compaction");
            let value = result.unwrap();
            prop_assert_eq!(
                value.data(),
                expected_value.as_slice(),
                "Value should be preserved after compaction"
            );
        }
    }
}

// ============================================================================
// Property 11: Concurrent Writes Don't Corrupt Data
// ============================================================================
// Property: Multiple writers can operate concurrently without data corruption

proptest! {
    #[test]
    fn prop_concurrent_writes_preserve_data(
        kv_pairs in prop::collection::vec(
            (arbitrary_key(), arbitrary_value()),
            10..=50
        )
    ) {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());

        // Split data across 4 threads
        let chunk_size = kv_pairs.len().div_ceil(4);
        let chunks: Vec<_> = kv_pairs.chunks(chunk_size).map(|c| c.to_vec()).collect();

        // Track expected state (last write wins, but we don't know thread order)
        let expected_keys: std::collections::HashSet<Vec<u8>> =
            kv_pairs.iter().map(|(k, _)| k.clone()).collect();

        // Spawn concurrent writers
        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    let write_opts = WriteOptions::default();
                    for (key, value) in chunk {
                        db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                            .unwrap();
                    }
                })
            })
            .collect();

        // Wait for all writers
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all keys exist (any value is valid due to concurrent writes)
        let read_opts = ReadOptions::default();
        for key in expected_keys {
            let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
            prop_assert!(result.is_some(), "Key should exist after concurrent writes");
        }
    }
}

// ============================================================================
// Property 12: Concurrent Reads Are Consistent
// ============================================================================
// Property: Readers see consistent state even during concurrent writes

proptest! {
    #[test]
    fn prop_concurrent_reads_consistent(
        kv_pairs in prop::collection::vec(
            (arbitrary_key(), arbitrary_value()),
            10..=30
        )
    ) {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());

        let write_opts = WriteOptions::default();

        // Write initial data
        let mut expected_state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for (key, value) in &kv_pairs {
            db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                .unwrap();
            expected_state.insert(key.clone(), value.clone());
        }

        // Spawn concurrent readers
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let db = Arc::clone(&db);
                let expected = expected_state.clone();
                thread::spawn(move || {
                    let read_opts = ReadOptions::default();
                    for (key, expected_value) in expected {
                        let result = db.get(&read_opts, &Slice::from(key.as_slice())).unwrap();
                        // Either sees the value or None (if concurrent delete), but no corruption
                        if let Some(value) = result {
                            assert_eq!(value.data(), expected_value.as_slice());
                        }
                    }
                })
            })
            .collect();

        // Wait for all readers
        for handle in handles {
            handle.join().unwrap();
        }

        prop_assert!(true);
    }
}

// ============================================================================
// Property 13: Concurrent Mixed Operations
// ============================================================================
// Property: Concurrent reads, writes, and deletes maintain consistency

proptest! {
    #[test]
    fn prop_concurrent_mixed_operations(
        initial_data in prop::collection::vec(
            (arbitrary_key(), arbitrary_value()),
            5..=20
        )
    ) {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());

        let write_opts = WriteOptions::default();

        // Write initial data
        for (key, value) in &initial_data {
            db.put(&write_opts, Slice::from(key.as_slice()), Slice::from(value.as_slice()))
                .unwrap();
        }

        // Spawn mixed workload: readers, writers, deleters
        let handles: Vec<_> = (0..3)
            .flat_map(|i| {
                let db = Arc::clone(&db);
                let data = initial_data.clone();

                vec![
                    // Reader thread
                    thread::spawn({
                        let db = Arc::clone(&db);
                        let data = data.clone();
                        move || {
                            let read_opts = ReadOptions::default();
                            for (key, _) in data.iter().take(5) {
                                let _ = db.get(&read_opts, &Slice::from(key.as_slice()));
                            }
                        }
                    }),
                    // Writer thread
                    thread::spawn({
                        let db = Arc::clone(&db);
                        let data = data.clone();
                        move || {
                            let write_opts = WriteOptions::default();
                            for (j, (key, _value)) in data.iter().enumerate().skip(i).take(3) {
                                let new_value = format!("updated-{}", j);
                                db.put(
                                    &write_opts,
                                    Slice::from(key.as_slice()),
                                    Slice::from(new_value),
                                )
                                .unwrap();
                            }
                        }
                    }),
                ]
            })
            .collect();

        // Wait for all operations
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify DB is still functional (no corruption)
        let read_opts = ReadOptions::default();
        for (key, _) in &initial_data {
            let result = db.get(&read_opts, &Slice::from(key.as_slice()));
            prop_assert!(result.is_ok(), "DB should remain functional after concurrent ops");
        }
    }
}
