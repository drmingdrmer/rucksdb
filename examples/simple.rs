use rucksdb::{DBOptions, ReadOptions, Slice, WriteOptions, DB};

fn main() {
    println!("RucksDB Simple Example");

    let db = DB::open("example_db", DBOptions::default()).expect("Failed to open database");

    let write_opts = WriteOptions::default();
    let read_opts = ReadOptions::default();

    db.put(&write_opts, Slice::from("name"), Slice::from("Alice"))
        .expect("Failed to put");
    db.put(&write_opts, Slice::from("age"), Slice::from("30"))
        .expect("Failed to put");

    if let Some(name) = db.get(&read_opts, &Slice::from("name")).expect("Failed to get") {
        println!("Name: {}", name);
    }

    if let Some(age) = db.get(&read_opts, &Slice::from("age")).expect("Failed to get") {
        println!("Age: {}", age);
    }

    db.put(&write_opts, Slice::from("name"), Slice::from("Bob"))
        .expect("Failed to update");

    if let Some(name) = db.get(&read_opts, &Slice::from("name")).expect("Failed to get") {
        println!("Updated Name: {}", name);
    }

    db.delete(&write_opts, Slice::from("age"))
        .expect("Failed to delete");

    match db.get(&read_opts, &Slice::from("age")).expect("Failed to get") {
        Some(age) => println!("Age: {}", age),
        None => println!("Age has been deleted"),
    }

    db.close().expect("Failed to close database");
    println!("Database closed successfully");
}
