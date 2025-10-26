#![no_main]

use libfuzzer_sys::fuzz_target;
use rucksdb::{DBOptions, ReadOptions, WriteBatch, WriteOptions, DB};

// Fuzz target for WriteBatch operations.
// Tests atomic batch operations with arbitrary sequences.
fuzz_target!(|data: &[u8]| {
    if data.len() < 3 {
        return;
    }

    let test_dir = format!("/tmp/rucksdb_fuzz_wb_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&test_dir);

    let opts = DBOptions::default();
    let db = match DB::open(&test_dir, opts) {
        Ok(db) => db,
        Err(_) => {
            let _ = std::fs::remove_dir_all(&test_dir);
            return;
        }
    };

    let mut batch = WriteBatch::new();
    const DEFAULT_CF: u32 = 0;

    let mut i = 0;
    while i + 2 < data.len() {
        let op_type = data[i] % 3;
        i += 1;

        let key_len = (data[i] as usize).min(255).min(data.len() - i - 1);
        i += 1;

        if i + key_len > data.len() {
            break;
        }

        let key = &data[i..i + key_len];
        i += key_len;

        match op_type {
            0 => {
                if i >= data.len() {
                    break;
                }
                let value_len = (data[i] as usize).min(255).min(data.len() - i - 1);
                i += 1;

                if i + value_len > data.len() {
                    break;
                }

                let value = &data[i..i + value_len];
                i += value_len;

                let _ = batch.put(DEFAULT_CF, key.into(), value.into());
            }
            1 => {
                let _ = batch.delete(DEFAULT_CF, key.into());
            }
            2 => {
                if i >= data.len() {
                    break;
                }
                let value_len = (data[i] as usize).min(255).min(data.len() - i - 1);
                i += 1;

                if i + value_len > data.len() {
                    break;
                }

                let value = &data[i..i + value_len];
                i += value_len;

                let _ = batch.merge(DEFAULT_CF, key.into(), value.into());
            }
            _ => unreachable!(),
        }
    }

    let write_opts = WriteOptions::default();
    let read_opts = ReadOptions::default();

    let _ = db.write(&write_opts, &batch);
    let _ = db.get(&read_opts, &b"probe".as_slice().into());

    drop(db);
    let _ = std::fs::remove_dir_all(&test_dir);
});
