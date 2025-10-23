use crate::memtable::MemTable;
use crate::util::{Result, Slice};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct WriteOptions {
    pub sync: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions { sync: false }
    }
}

#[derive(Clone)]
pub struct ReadOptions {
    pub verify_checksums: bool,
    pub fill_cache: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            verify_checksums: false,
            fill_cache: true,
        }
    }
}

pub struct DBOptions {
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub write_buffer_size: usize,
}

impl Default for DBOptions {
    fn default() -> Self {
        DBOptions {
            create_if_missing: true,
            error_if_exists: false,
            write_buffer_size: 4 * 1024 * 1024, // 4MB
        }
    }
}

pub struct DB {
    mem: Arc<RwLock<MemTable>>,
    sequence: Arc<AtomicU64>,
    #[allow(dead_code)]
    options: DBOptions,
}

impl DB {
    pub fn open(_name: &str, options: DBOptions) -> Result<Self> {
        Ok(DB {
            mem: Arc::new(RwLock::new(MemTable::new())),
            sequence: Arc::new(AtomicU64::new(0)),
            options,
        })
    }

    pub fn put(&self, _options: &WriteOptions, key: Slice, value: Slice) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let mem = self.mem.read();
        mem.add(seq, key, value);
        Ok(())
    }

    pub fn get(&self, _options: &ReadOptions, key: &Slice) -> Result<Option<Slice>> {
        let mem = self.mem.read();
        Ok(mem.get(key))
    }

    pub fn delete(&self, _options: &WriteOptions, key: Slice) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let mem = self.mem.read();
        mem.delete(seq, key);
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        Ok(())
    }

    #[allow(dead_code)]
    fn should_flush(&self) -> bool {
        let mem = self.mem.read();
        mem.approximate_memory_usage() >= self.options.write_buffer_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_open() {
        let db = DB::open("test_db", DBOptions::default());
        assert!(db.is_ok());
    }

    #[test]
    fn test_db_put_get() {
        let db = DB::open("test_db", DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();

        let value = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
        assert_eq!(value, Some(Slice::from("value1")));
    }

    #[test]
    fn test_db_delete() {
        let db = DB::open("test_db", DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();

        db.delete(&WriteOptions::default(), Slice::from("key1"))
            .unwrap();

        let value = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_db_multiple_operations() {
        let db = DB::open("test_db", DBOptions::default()).unwrap();

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

        assert_eq!(
            db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap(),
            Some(Slice::from("value1"))
        );
        assert_eq!(
            db.get(&ReadOptions::default(), &Slice::from("key2")).unwrap(),
            Some(Slice::from("value2"))
        );

        db.delete(&WriteOptions::default(), Slice::from("key1"))
            .unwrap();
        assert_eq!(
            db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap(),
            None
        );
    }

    #[test]
    fn test_db_overwrite() {
        let db = DB::open("test_db", DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value2"),
        )
        .unwrap();

        let value = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
        assert_eq!(value, Some(Slice::from("value2")));
    }
}
