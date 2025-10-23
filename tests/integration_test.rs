use rucksdb::{DB, DBOptions, ReadOptions, Slice, WriteOptions};
use tempfile::TempDir;

#[test]
fn test_basic_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    db.put(
        &WriteOptions::default(),
        Slice::from("key1"),
        Slice::from("value1"),
    )
    .unwrap();
    db.put(
        &WriteOptions::default(),
        Slice::from("key2"),
        Slice::from("value2"),
    )
    .unwrap();

    let val1 = db
        .get(&ReadOptions::default(), &Slice::from("key1"))
        .unwrap();
    assert_eq!(val1, Some(Slice::from("value1")));

    let val2 = db
        .get(&ReadOptions::default(), &Slice::from("key2"))
        .unwrap();
    assert_eq!(val2, Some(Slice::from("value2")));

    db.delete(&WriteOptions::default(), Slice::from("key1"))
        .unwrap();
    let val1_after_delete = db
        .get(&ReadOptions::default(), &Slice::from("key1"))
        .unwrap();
    assert_eq!(val1_after_delete, None);
}

#[test]
fn test_update_value() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    db.put(
        &WriteOptions::default(),
        Slice::from("key"),
        Slice::from("value1"),
    )
    .unwrap();

    let val = db
        .get(&ReadOptions::default(), &Slice::from("key"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("value1")));

    db.put(
        &WriteOptions::default(),
        Slice::from("key"),
        Slice::from("value2"),
    )
    .unwrap();

    let val = db
        .get(&ReadOptions::default(), &Slice::from("key"))
        .unwrap();
    assert_eq!(val, Some(Slice::from("value2")));
}

#[test]
fn test_nonexistent_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    let val = db
        .get(&ReadOptions::default(), &Slice::from("nonexistent"))
        .unwrap();
    assert_eq!(val, None);
}

#[test]
fn test_many_keys() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

    for i in 0..1000 {
        let key = format!("key{i}");
        let value = format!("value{i}");
        db.put(
            &WriteOptions::default(),
            Slice::from(key),
            Slice::from(value),
        )
        .unwrap();
    }

    for i in 0..1000 {
        let key = format!("key{i}");
        let value = format!("value{i}");
        let result = db.get(&ReadOptions::default(), &Slice::from(key)).unwrap();
        assert_eq!(result, Some(Slice::from(value)));
    }
}
