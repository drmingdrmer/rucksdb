use rucksdb::{DBOptions, ReadOptions, Slice, WriteOptions, DB};

#[test]
fn test_string_ordering() {
    let db = DB::open("test_db", DBOptions::default()).unwrap();

    db.put(
        &WriteOptions::default(),
        Slice::from("key0"),
        Slice::from("value0"),
    )
    .unwrap();

    db.put(
        &WriteOptions::default(),
        Slice::from("key1"),
        Slice::from("value1"),
    )
    .unwrap();

    let result0 = db.get(&ReadOptions::default(), &Slice::from("key0")).unwrap();
    println!("key0: {:?}", result0);
    assert_eq!(result0, Some(Slice::from("value0")));

    let result1 = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
    println!("key1: {:?}", result1);
    assert_eq!(result1, Some(Slice::from("value1")));

    db.put(
        &WriteOptions::default(),
        Slice::from("key10"),
        Slice::from("value10"),
    )
    .unwrap();

    let result1_again = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
    println!("key1 after key10: {:?}", result1_again);
    assert_eq!(result1_again, Some(Slice::from("value1")));
}
