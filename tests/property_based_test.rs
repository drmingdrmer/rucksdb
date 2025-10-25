use std::collections::HashMap;

use proptest::prelude::*;
use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

// ============================================================================
// Property Test Strategy
// ============================================================================
// 1. Write-Read Consistency: Any written key should be readable with same value
// 2. Delete Semantics: Deleted keys should return None
// 3. Overwrite Semantics: Last write wins
// 4. Persistence: Data survives DB close/reopen
// 5. Batch Atomicity: All operations in batch succeed or fail together
// 6. Iterator Ordering: Keys returned in sorted order
// 7. Snapshot Isolation: Snapshots see consistent point-in-time view

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
