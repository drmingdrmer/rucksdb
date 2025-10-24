use std::collections::BTreeMap;

use proptest::prelude::*;
use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

/// Property: If you write a key-value pair, you can read it back
#[test]
fn property_write_then_read() {
    proptest!(|(key in "[a-z]{1,20}", value in "[a-z]{1,100}")| {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().join("db").to_str().unwrap(), DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from(key.as_str()),
            Slice::from(value.as_str()),
        )
        .unwrap();

        let result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();

        assert_eq!(result, Some(Slice::from(value.as_str())));
    });
}

/// Property: If you delete a key, reading it returns None
#[test]
fn property_delete_then_read() {
    proptest!(|(key in "[a-z]{1,20}", value in "[a-z]{1,100}")| {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().join("db").to_str().unwrap(), DBOptions::default()).unwrap();

        // First write the key
        db.put(
            &WriteOptions::default(),
            Slice::from(key.as_str()),
            Slice::from(value.as_str()),
        )
        .unwrap();

        // Then delete it
        db.delete(&WriteOptions::default(), Slice::from(key.as_str()))
            .unwrap();

        // Should return None
        let result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();

        assert_eq!(result, None);
    });
}

/// Property: Overwriting a key updates its value
#[test]
fn property_update_overwrites() {
    proptest!(|(key in "[a-z]{1,20}", value1 in "[a-z]{1,100}", value2 in "[a-z]{1,100}")| {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().join("db").to_str().unwrap(), DBOptions::default()).unwrap();

        // Write first value
        db.put(
            &WriteOptions::default(),
            Slice::from(key.as_str()),
            Slice::from(value1.as_str()),
        )
        .unwrap();

        // Overwrite with second value
        db.put(
            &WriteOptions::default(),
            Slice::from(key.as_str()),
            Slice::from(value2.as_str()),
        )
        .unwrap();

        // Should get second value
        let result = db
            .get(&ReadOptions::default(), &Slice::from(key.as_str()))
            .unwrap();

        assert_eq!(result, Some(Slice::from(value2.as_str())));
    });
}

/// Property: Iterator returns keys in sorted order
#[test]
fn property_iterator_sorted() {
    proptest!(|(keys in prop::collection::vec("[a-z]{1,20}", 1..20))| {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().join("db").to_str().unwrap(), DBOptions::default()).unwrap();

        // Insert all keys with unique values
        for (i, key) in keys.iter().enumerate() {
            db.put(
                &WriteOptions::default(),
                Slice::from(key.as_str()),
                Slice::from(format!("value{i}")),
            )
            .unwrap();
        }

        // Iterate and collect keys
        let mut iter = db.iter().unwrap();
        let mut collected_keys = Vec::new();
        if iter.seek_to_first().unwrap() {
            loop {
                collected_keys.push(iter.key().to_string());
                if !iter.next().unwrap() {
                    break;
                }
            }
        }

        // Keys should be sorted
        let mut expected_keys: Vec<String> = keys.to_vec();
        expected_keys.sort();
        expected_keys.dedup(); // Remove duplicates

        prop_assert_eq!(collected_keys, expected_keys);
    });
}

/// Property: Multiple operations maintain consistency (model-based testing)
#[test]
fn property_operations_consistent_with_model() {
    #[derive(Debug, Clone)]
    enum Op {
        Put(String, String),
        Delete(String),
        Get(String),
    }

    proptest!(|(ops in prop::collection::vec(
        prop_oneof![
            (r"[a-z]{1,10}", r"[a-z]{1,50}").prop_map(|(k, v)| Op::Put(k, v)),
            r"[a-z]{1,10}".prop_map(Op::Delete),
            r"[a-z]{1,10}".prop_map(Op::Get),
        ],
        1..50
    ))| {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().join("db").to_str().unwrap(), DBOptions::default()).unwrap();

        // Model using BTreeMap
        let mut model: BTreeMap<String, String> = BTreeMap::new();

        for op in ops {
            match op {
                Op::Put(key, value) => {
                    db.put(
                        &WriteOptions::default(),
                        Slice::from(key.as_str()),
                        Slice::from(value.as_str()),
                    )
                    .unwrap();
                    model.insert(key, value);
                }
                Op::Delete(key) => {
                    db.delete(&WriteOptions::default(), Slice::from(key.as_str()))
                        .unwrap();
                    model.remove(&key);
                }
                Op::Get(key) => {
                    let db_result = db
                        .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                        .unwrap();
                    let model_result = model.get(&key);

                    match (db_result, model_result) {
                        (Some(db_val), Some(model_val)) => {
                            prop_assert_eq!(db_val.to_string(), model_val.clone());
                        }
                        (None, None) => {
                            // Both return None - correct
                        }
                        (db_val, model_val) => {
                            panic!(
                                "Mismatch for key {}: db={:?}, model={:?}",
                                key, db_val, model_val
                            );
                        }
                    }
                }
            }
        }
    });
}

/// Property: Iterator range is consistent with model
#[test]
fn property_iterator_range_consistent() {
    proptest!(|(ops in prop::collection::vec(
        (r"[a-z]{1,10}", r"[a-z]{1,50}"),
        5..30
    ))| {
        let temp_dir = TempDir::new().unwrap();
        let db = DB::open(temp_dir.path().join("db").to_str().unwrap(), DBOptions::default()).unwrap();

        // Insert all key-value pairs
        let mut model: BTreeMap<String, String> = BTreeMap::new();
        for (key, value) in ops {
            db.put(
                &WriteOptions::default(),
                Slice::from(key.as_str()),
                Slice::from(value.as_str()),
            )
            .unwrap();
            model.insert(key, value);
        }

        // Collect from iterator
        let mut iter = db.iter().unwrap();
        let mut db_data: BTreeMap<String, String> = BTreeMap::new();
        if iter.seek_to_first().unwrap() {
            loop {
                db_data.insert(iter.key().to_string(), iter.value().to_string());
                if !iter.next().unwrap() {
                    break;
                }
            }
        }

        // Should match model
        prop_assert_eq!(db_data, model);
    });
}

/// Property: Recovery preserves all data (crash recovery property)
#[test]
fn property_recovery_preserves_data() {
    proptest!(|(ops in prop::collection::vec(
        (r"[a-z]{1,10}", r"[a-z]{1,50}"),
        5..50
    ))| {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let mut expected: BTreeMap<String, String> = BTreeMap::new();

        // Write data
        {
            let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
            for (key, value) in ops {
                db.put(
                    &WriteOptions::default(),
                    Slice::from(key.as_str()),
                    Slice::from(value.as_str()),
                )
                .unwrap();
                expected.insert(key, value);
            }
            // DB dropped here - simulates crash
        }

        // Reopen and verify all data
        {
            let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();
            for (key, expected_value) in &expected {
                let result = db
                    .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                    .unwrap();
                prop_assert_eq!(result, Some(Slice::from(expected_value.as_str())));
            }
        }
    });
}
