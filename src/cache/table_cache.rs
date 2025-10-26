use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{cache::LRUCache, table::TableReader, util::Result};

/// TableCache caches opened TableReader instances to avoid repeated file opens
///
/// Opening an SSTable file is expensive:
/// - Open file handle
/// - Read and parse footer (48 bytes)
/// - Read and decompress index block
/// - Read and parse filter block
///
/// By caching TableReaders, we avoid this overhead on every read operation.
/// This dramatically improves random read performance.
///
/// Each TableReader is wrapped in Mutex for safe concurrent access since
/// TableReader::get() requires &mut self for file reads.
pub struct TableCache {
    cache: Arc<Mutex<LRUCache<u64, Arc<Mutex<TableReader>>>>>,
    db_path: PathBuf,
    block_cache: Option<LRUCache<(u64, u64), Vec<u8>>>,
}

impl TableCache {
    /// Create a new TableCache with specified capacity
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of table files to keep open
    /// * `db_path` - Database path for locating SST files
    /// * `block_cache` - Shared block cache for data blocks
    #[inline]
    pub fn new(
        capacity: usize,
        db_path: PathBuf,
        block_cache: Option<LRUCache<(u64, u64), Vec<u8>>>,
    ) -> Self {
        TableCache {
            cache: Arc::new(Mutex::new(LRUCache::new(capacity))),
            db_path,
            block_cache,
        }
    }

    /// Get a TableReader for the specified file number
    ///
    /// If the table is already in cache, returns cached instance.
    /// Otherwise, opens the table file and caches it.
    ///
    /// # Arguments
    /// * `file_number` - SSTable file number (e.g., 123 for 000123.sst)
    ///
    /// # Returns
    /// Arc<Mutex<TableReader>> for thread-safe access
    #[inline]
    pub fn get_table(&self, file_number: u64) -> Result<Arc<Mutex<TableReader>>> {
        // Fast path: check cache first
        {
            let cache = self.cache.lock().unwrap();
            if let Some(table) = cache.get(&file_number) {
                return Ok(table);
            }
        }

        // Slow path: open table and insert into cache
        let sst_path = self.db_path.join(format!("{file_number:06}.sst"));
        let table_reader = TableReader::open(&sst_path, file_number, self.block_cache.clone())?;
        let table = Arc::new(Mutex::new(table_reader));

        // Insert into cache
        {
            let cache = self.cache.lock().unwrap();
            cache.insert(file_number, Arc::clone(&table));
        }

        Ok(table)
    }

    /// Get cache statistics
    pub fn stats(&self) -> TableCacheStats {
        let cache = self.cache.lock().unwrap();
        let stats = cache.stats();
        TableCacheStats {
            capacity: stats.capacity,
            entries: stats.entries,
            hits: stats.hits,
            misses: stats.misses,
        }
    }

    /// Clear the cache, closing all table files
    pub fn clear(&self) {
        let cache = self.cache.lock().unwrap();
        cache.clear();
    }
}

/// Statistics for the table cache
#[derive(Debug, Clone)]
pub struct TableCacheStats {
    pub capacity: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
}

impl TableCacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            return 0.0;
        }
        self.hits as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{CompressionType, table::TableBuilder, util::Slice};

    fn create_test_table(path: &std::path::Path, _file_number: u64) -> Result<()> {
        use crate::memtable::memtable::{InternalKey, VALUE_TYPE_VALUE};
        let mut builder = TableBuilder::new(path)?;
        // Encode keys as InternalKeys with sequence numbers
        let key1 = InternalKey::new(Slice::from("key1"), 1, VALUE_TYPE_VALUE).encode();
        let key2 = InternalKey::new(Slice::from("key2"), 2, VALUE_TYPE_VALUE).encode();
        builder.add(&key1, &Slice::from("value1"))?;
        builder.add(&key2, &Slice::from("value2"))?;
        builder.finish(CompressionType::None)?;
        Ok(())
    }

    #[test]
    fn test_table_cache_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path();

        // Create a test SSTable
        let sst_path = db_path.join("000001.sst");
        create_test_table(&sst_path, 1).unwrap();

        // Create cache
        let cache = TableCache::new(10, db_path.to_path_buf(), None);

        // First access - should open file (cache miss)
        let table1 = cache.get_table(1).unwrap();
        let stats1 = cache.stats();
        assert_eq!(stats1.entries, 1);
        assert_eq!(stats1.misses, 1);

        // Read from table
        {
            let mut table = table1.lock().unwrap();
            let (found, value) = table.get(&Slice::from("key1")).unwrap();
            assert!(found);
            assert_eq!(value, Some(Slice::from("value1")));
        }

        // Second access - should hit cache
        let table2 = cache.get_table(1).unwrap();
        let stats2 = cache.stats();
        assert_eq!(stats2.entries, 1);
        assert_eq!(stats2.hits, 1);

        // Should be same instance
        assert!(Arc::ptr_eq(&table1, &table2));
    }

    #[test]
    fn test_table_cache_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path();

        // Create multiple test SSTables
        for i in 1..=5 {
            let sst_path = db_path.join(format!("{i:06}.sst"));
            create_test_table(&sst_path, i).unwrap();
        }

        // Create cache with capacity 3
        let cache = TableCache::new(3, db_path.to_path_buf(), None);

        // Fill cache
        cache.get_table(1).unwrap();
        cache.get_table(2).unwrap();
        cache.get_table(3).unwrap();
        assert_eq!(cache.stats().entries, 3);

        // Add one more - should evict LRU (table 1)
        cache.get_table(4).unwrap();
        assert_eq!(cache.stats().entries, 3);

        // Access table 1 again - should be cache miss
        let stats_before = cache.stats();
        cache.get_table(1).unwrap();
        let stats_after = cache.stats();
        assert_eq!(stats_after.misses, stats_before.misses + 1);
    }

    #[test]
    fn test_table_cache_concurrent() {
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path();

        // Create test SSTable
        let sst_path = db_path.join("000001.sst");
        create_test_table(&sst_path, 1).unwrap();

        let cache = Arc::new(TableCache::new(10, db_path.to_path_buf(), None));

        // Spawn multiple threads accessing same table
        let mut handles = vec![];
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let table = cache.get_table(1).unwrap();
                    let mut t = table.lock().unwrap();
                    let (found, value) = t.get(&Slice::from("key1")).unwrap();
                    assert!(found);
                    assert_eq!(value, Some(Slice::from("value1")));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have hit cache many times
        let stats = cache.stats();
        assert!(stats.hits > 0);
    }
}
