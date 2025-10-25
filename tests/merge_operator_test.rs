use std::sync::Arc;

use rucksdb::{CounterMerge, DB, DBOptions, MergeOperator, Slice, StringAppendMerge, WriteOptions};
use tempfile::TempDir;

#[test]
fn test_counter_merge_operator() {
    let dir = TempDir::new().unwrap();

    // Create DB with CounterMerge operator
    let options = DBOptions {
        merge_operator: Some(Arc::new(CounterMerge)),
        ..Default::default()
    };

    let db = DB::open(dir.path().to_str().unwrap(), options).unwrap();

    // Initialize counter
    db.put(
        &WriteOptions::default(),
        Slice::from("counter"),
        Slice::from("10"),
    )
    .unwrap();

    // Simulate merges (for now, we're just demonstrating the API exists)
    // In a full implementation, these would be applied during get/compaction
    let _merge_result = CounterMerge.full_merge(
        &Slice::from("counter"),
        Some(&Slice::from("10")),
        &[Slice::from("5"), Slice::from("3"), Slice::from("-2")],
    );

    // Verify the operator works
    assert!(_merge_result.is_ok());
    assert_eq!(_merge_result.unwrap().to_string(), "16"); // 10 + 5 + 3 - 2
}

#[test]
fn test_string_append_merge_operator() {
    let dir = TempDir::new().unwrap();

    // Create DB with StringAppendMerge operator
    let options = DBOptions {
        merge_operator: Some(Arc::new(StringAppendMerge::new(","))),
        ..Default::default()
    };

    let db = DB::open(dir.path().to_str().unwrap(), options).unwrap();

    // Initialize value
    db.put(
        &WriteOptions::default(),
        Slice::from("log"),
        Slice::from("a"),
    )
    .unwrap();

    // Simulate merge operations
    let merge_result = StringAppendMerge::new(",").full_merge(
        &Slice::from("log"),
        Some(&Slice::from("a")),
        &[Slice::from("b"), Slice::from("c"), Slice::from("d")],
    );

    assert!(merge_result.is_ok());
    assert_eq!(merge_result.unwrap().to_string(), "a,b,c,d");
}

#[test]
fn test_merge_operator_configuration() {
    let dir = TempDir::new().unwrap();

    // Test that DB can be created with and without merge operator
    let options_with_merge = DBOptions {
        merge_operator: Some(Arc::new(CounterMerge)),
        ..Default::default()
    };

    let options_without_merge = DBOptions {
        merge_operator: None,
        ..Default::default()
    };

    // Both should work
    let _db_with = DB::open(
        dir.path().join("with").to_str().unwrap(),
        options_with_merge,
    );
    assert!(_db_with.is_ok());

    let _db_without = DB::open(
        dir.path().join("without").to_str().unwrap(),
        options_without_merge,
    );
    assert!(_db_without.is_ok());
}

#[test]
fn test_counter_merge_partial_merge() {
    let merge = CounterMerge;

    // Test partial merge combines multiple operands
    let operands = vec![Slice::from("10"), Slice::from("20"), Slice::from("30")];

    let result = merge.partial_merge(&Slice::from("key"), &operands);
    assert!(result.is_some());
    assert_eq!(result.unwrap().to_string(), "60");
}

#[test]
fn test_string_append_partial_merge() {
    let merge = StringAppendMerge::new("-");

    let operands = vec![
        Slice::from("hello"),
        Slice::from("world"),
        Slice::from("test"),
    ];

    let result = merge.partial_merge(&Slice::from("key"), &operands);
    assert!(result.is_some());
    assert_eq!(result.unwrap().to_string(), "hello-world-test");
}

#[test]
fn test_counter_merge_with_negative_values() {
    let merge = CounterMerge;

    let result = merge.full_merge(
        &Slice::from("counter"),
        Some(&Slice::from("100")),
        &[Slice::from("-10"), Slice::from("-20"), Slice::from("5")],
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap().to_string(), "75"); // 100 - 10 - 20 + 5
}

#[test]
fn test_counter_merge_no_existing_value() {
    let merge = CounterMerge;

    // Starting from zero when no existing value
    let result = merge.full_merge(
        &Slice::from("counter"),
        None,
        &[Slice::from("10"), Slice::from("20")],
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap().to_string(), "30");
}

#[test]
fn test_string_append_no_existing_value() {
    let merge = StringAppendMerge::new(" ");

    let result = merge.full_merge(
        &Slice::from("text"),
        None,
        &[Slice::from("Hello"), Slice::from("World"), Slice::from("!")],
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap().to_string(), "Hello World !");
}

#[test]
fn test_merge_operator_name() {
    let counter = CounterMerge;
    let append = StringAppendMerge::default();

    assert_eq!(counter.name(), "CounterMerge");
    assert_eq!(append.name(), "StringAppendMerge");
}
