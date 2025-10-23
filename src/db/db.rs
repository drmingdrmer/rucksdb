use crate::cache::LRUCache;
use crate::filter::{BloomFilterPolicy, FilterPolicy};
use crate::memtable::MemTable;
use crate::table::{CompressionType, TableBuilder, TableReader};
use crate::util::{Result, Slice, Status};
use crate::version::{FileMetaData, VersionEdit, VersionSet};
use crate::wal;
use parking_lot::RwLock;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Default)]
pub struct WriteOptions {
    pub sync: bool,
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
    pub block_cache_size: usize,            // Number of blocks to cache
    pub compression_type: CompressionType,  // Compression algorithm for blocks
    pub filter_bits_per_key: Option<usize>, // Bloom filter bits per key (None = disabled)
}

impl Default for DBOptions {
    fn default() -> Self {
        DBOptions {
            create_if_missing: true,
            error_if_exists: false,
            write_buffer_size: 4 * 1024 * 1024,        // 4MB
            block_cache_size: 1000, // Cache up to 1000 blocks (~4MB with 4KB blocks)
            compression_type: CompressionType::Snappy, // Snappy by default
            filter_bits_per_key: Some(10), // ~1% false positive rate
        }
    }
}

pub struct DB {
    mem: Arc<RwLock<MemTable>>,
    imm: Arc<RwLock<Option<MemTable>>>, // Immutable MemTable being flushed
    sequence: Arc<AtomicU64>,
    wal: Arc<RwLock<Option<wal::Writer>>>,
    db_path: PathBuf,
    options: DBOptions,
    // Version management
    version_set: Arc<RwLock<VersionSet>>,
    // Block cache: (file_number, block_offset) -> block_data
    block_cache: LRUCache<(u64, u64), Vec<u8>>,
}

impl DB {
    pub fn open(name: &str, options: DBOptions) -> Result<Self> {
        let db_path = Path::new(name);

        // Create directory if needed
        if options.create_if_missing {
            fs::create_dir_all(db_path)
                .map_err(|e| Status::io_error(format!("Failed to create directory: {e}")))?;
        }

        if options.error_if_exists && db_path.exists() {
            return Err(Status::invalid_argument("Database already exists"));
        }

        let wal_path = db_path.join("wal.log");
        let mem = Arc::new(RwLock::new(MemTable::new()));

        // Initialize VersionSet
        let mut version_set = VersionSet::new(db_path);
        version_set.open_or_create()?;

        let sequence = Arc::new(AtomicU64::new(version_set.last_sequence()));

        // Recover from WAL if exists
        if wal_path.exists() {
            Self::recover_from_wal(&wal_path, &mem, &sequence)?;
            // Update VersionSet with recovered sequence
            version_set.set_last_sequence(sequence.load(Ordering::SeqCst));
        }

        // Open WAL for writing
        let wal_writer = wal::Writer::new(&wal_path)?;

        // Initialize block cache
        let block_cache = LRUCache::new(options.block_cache_size);

        Ok(DB {
            mem,
            imm: Arc::new(RwLock::new(None)),
            sequence,
            wal: Arc::new(RwLock::new(Some(wal_writer))),
            db_path: db_path.to_path_buf(),
            options,
            version_set: Arc::new(RwLock::new(version_set)),
            block_cache,
        })
    }

    /// Get TableReader with block cache
    fn get_table(&self, file_number: u64) -> Result<TableReader> {
        let sst_path = self.db_path.join(format!("{file_number:06}.sst"));
        TableReader::open(&sst_path, file_number, Some(self.block_cache.clone()))
    }

    /// Recover data from WAL
    fn recover_from_wal(
        wal_path: &Path,
        mem: &Arc<RwLock<MemTable>>,
        sequence: &Arc<AtomicU64>,
    ) -> Result<()> {
        let mut reader = wal::Reader::new(wal_path)?;
        let mut max_seq = 0u64;

        while let Some(record) = reader.read_record()? {
            if record.is_empty() {
                continue;
            }

            let (seq, key, value) = Self::decode_wal_record(&record)?;
            max_seq = max_seq.max(seq);

            let mem_guard = mem.write();
            if let Some(val) = value {
                mem_guard.add(seq, key, val);
            } else {
                mem_guard.delete(seq, key);
            }
        }

        sequence.store(max_seq + 1, Ordering::SeqCst);
        Ok(())
    }

    /// Encode WAL record: op_type(1) + seq(8) + key_len(2) + key + [value_len(2) + value]
    fn encode_wal_record(seq: u64, key: &Slice, value: Option<&Slice>) -> Vec<u8> {
        let mut buf = Vec::new();

        // Operation type: 1=Put, 2=Delete
        buf.push(if value.is_some() { 1 } else { 2 });

        // Sequence number
        buf.extend_from_slice(&seq.to_le_bytes());

        // Key
        let key_data = key.data();
        buf.extend_from_slice(&(key_data.len() as u16).to_le_bytes());
        buf.extend_from_slice(key_data);

        // Value (if Put)
        if let Some(val) = value {
            let val_data = val.data();
            buf.extend_from_slice(&(val_data.len() as u16).to_le_bytes());
            buf.extend_from_slice(val_data);
        }

        buf
    }

    /// Decode WAL record
    fn decode_wal_record(data: &[u8]) -> Result<(u64, Slice, Option<Slice>)> {
        if data.len() < 11 {
            return Err(Status::corruption("WAL record too short"));
        }

        let op_type = data[0];
        let seq = u64::from_le_bytes(data[1..9].try_into().unwrap());

        let key_len = u16::from_le_bytes([data[9], data[10]]) as usize;
        if data.len() < 11 + key_len {
            return Err(Status::corruption("Invalid key length"));
        }

        let key = Slice::from(&data[11..11 + key_len]);

        let value = if op_type == 1 {
            // Put operation
            let val_start = 11 + key_len;
            if data.len() < val_start + 2 {
                return Err(Status::corruption("Invalid value length"));
            }

            let val_len = u16::from_le_bytes([data[val_start], data[val_start + 1]]) as usize;
            if data.len() < val_start + 2 + val_len {
                return Err(Status::corruption("Invalid value data"));
            }

            Some(Slice::from(&data[val_start + 2..val_start + 2 + val_len]))
        } else {
            None
        };

        Ok((seq, key, value))
    }

    pub fn put(&self, options: &WriteOptions, key: Slice, value: Slice) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);

        // Write to WAL first
        let record = Self::encode_wal_record(seq, &key, Some(&value));
        {
            let mut wal_guard = self.wal.write();
            if let Some(wal) = wal_guard.as_mut() {
                wal.add_record(&record)?;
                if options.sync {
                    wal.sync()?;
                }
            }
        }

        // Then write to MemTable
        let mem = self.mem.read();
        mem.add(seq, key, value);

        // Check if we need to flush
        if self.should_flush() {
            drop(mem);
            self.make_immutable();
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get(&self, _options: &ReadOptions, key: &Slice) -> Result<Option<Slice>> {
        // First check mutable MemTable
        {
            let mem = self.mem.read();
            let (found, value) = mem.get(key);
            if found {
                // Key exists in MemTable (either with value or deleted)
                return Ok(value);
            }
        }

        // Then check immutable MemTable
        {
            let imm = self.imm.read();
            if let Some(imm_table) = imm.as_ref()
                && let (true, value) = imm_table.get(key)
            {
                return Ok(value);
            }
        }

        // Then check SSTables through VersionSet
        let version_set = self.version_set.read();
        let current = version_set.current();
        let version = current.read();

        // Check level 0 first (newest files have highest numbers)
        for file in version.get_level_files(0).iter().rev() {
            let mut table = self.get_table(file.number)?;
            if let Some(value) = table.get(key)? {
                return Ok(Some(value));
            }
        }

        // Check other levels
        for level in 1..version.files.len() {
            let files = version.get_overlapping_files(level, key, key);
            for file in files {
                let mut table = self.get_table(file.number)?;
                if let Some(value) = table.get(key)? {
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    pub fn delete(&self, options: &WriteOptions, key: Slice) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);

        // Write to WAL first
        let record = Self::encode_wal_record(seq, &key, None);
        {
            let mut wal_guard = self.wal.write();
            if let Some(wal) = wal_guard.as_mut() {
                wal.add_record(&record)?;
                if options.sync {
                    wal.sync()?;
                }
            }
        }

        // Then write to MemTable
        let mem = self.mem.read();
        mem.delete(seq, key);
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        Ok(())
    }

    /// Get block cache statistics
    pub fn cache_stats(&self) -> crate::cache::CacheStats {
        self.block_cache.stats()
    }

    /// Flush MemTable to SSTable
    fn flush_memtable(&self) -> Result<()> {
        // Get all entries from immutable MemTable
        let entries = {
            let imm = self.imm.read();
            match imm.as_ref() {
                Some(imm_table) => imm_table.collect_entries(),
                None => return Ok(()), // Nothing to flush
            }
        };

        if entries.is_empty() {
            // Clear empty immutable MemTable
            let mut imm = self.imm.write();
            *imm = None;
            return Ok(());
        }

        // Generate new SSTable file
        let file_num = {
            let version_set = self.version_set.read();
            version_set.new_file_number()
        };
        let sst_path = self.db_path.join(format!("{file_num:06}.sst"));

        // Build SSTable with configured compression and filter
        let mut builder = self.create_table_builder(&sst_path)?;
        for (key, value) in &entries {
            builder.add(key, value)?;
        }
        builder.finish(self.options.compression_type)?;

        // Get file size and key range
        let file_size = std::fs::metadata(&sst_path)
            .map_err(|e| Status::io_error(format!("Failed to get file size: {e}")))?
            .len();
        let smallest = entries.first().unwrap().0.clone();
        let largest = entries.last().unwrap().0.clone();

        // Create FileMetaData and VersionEdit
        let file_meta = FileMetaData::new(file_num, file_size, smallest, largest);
        let mut edit = VersionEdit::new();
        edit.add_file(0, file_meta); // Always flush to Level 0
        edit.set_last_sequence(self.sequence.load(Ordering::SeqCst));

        // Apply edit to VersionSet
        {
            let version_set = self.version_set.read();
            version_set.log_and_apply(edit)?;
        }

        // Clear immutable MemTable (mem continues to accept writes)
        {
            let mut imm = self.imm.write();
            *imm = None;
        }

        // Clear WAL (all data is now persisted)
        {
            let wal_path = self.db_path.join("wal.log");
            let mut wal_guard = self.wal.write();
            *wal_guard = Some(wal::Writer::new(&wal_path)?);
        }

        Ok(())
    }

    fn should_flush(&self) -> bool {
        let mem = self.mem.read();
        if mem.approximate_memory_usage() < self.options.write_buffer_size {
            return false;
        }

        // Only return true if imm is empty (no flush in progress)
        let imm = self.imm.read();
        imm.is_none()
    }

    /// Create a TableBuilder with configured compression and filter options
    fn create_table_builder<P: AsRef<Path>>(&self, path: P) -> Result<TableBuilder> {
        let filter_policy = self.options.filter_bits_per_key.map(|bits_per_key| {
            Arc::new(BloomFilterPolicy::new(bits_per_key)) as Arc<dyn FilterPolicy>
        });

        TableBuilder::new_with_filter(path, filter_policy)
    }

    /// Move current MemTable to immutable and create new one
    fn make_immutable(&self) {
        let mut mem_guard = self.mem.write();
        let mut imm_guard = self.imm.write();

        // Only move if imm is empty
        if imm_guard.is_none() {
            let old_mem = std::mem::take(&mut *mem_guard);
            *imm_guard = Some(old_mem);
        }
    }

    /// Compact a level by merging files into the next level
    pub fn compact_level(&self, level: usize) -> Result<()> {
        if level >= 6 {
            return Ok(()); // No compaction for last level
        }

        // Get files to compact
        let (level_files, next_level_files) = {
            let version_set = self.version_set.read();
            let current = version_set.current();
            let version = current.read();

            let level_files: Vec<FileMetaData> = version.get_level_files(level).to_vec();
            if level_files.is_empty() {
                return Ok(()); // Nothing to compact
            }

            // Get overlapping files in next level
            let smallest = &level_files.iter().map(|f| &f.smallest).min().unwrap();
            let largest = &level_files.iter().map(|f| &f.largest).max().unwrap();
            let next_level_files = version.get_overlapping_files(level + 1, smallest, largest);

            (level_files, next_level_files)
        };

        // Merge all entries from selected files
        let mut all_entries: Vec<(Slice, Slice)> = Vec::new();

        // Read from level files
        for file in &level_files {
            let mut table = self.get_table(file.number)?;
            // Simple iteration - in production would use proper iterator
            let entries = self.read_all_from_table(&mut table)?;
            all_entries.extend(entries);
        }

        // Read from next level files
        for file in &next_level_files {
            let mut table = self.get_table(file.number)?;
            let entries = self.read_all_from_table(&mut table)?;
            all_entries.extend(entries);
        }

        // Sort and deduplicate (keeping newest values)
        all_entries.sort_by(|a, b| a.0.data().cmp(b.0.data()));
        let mut merged: Vec<(Slice, Slice)> = Vec::new();
        for (key, value) in all_entries {
            if merged.is_empty() || merged.last().unwrap().0 != key {
                merged.push((key, value));
            }
            // Keep only the latest value for duplicate keys
        }

        if merged.is_empty() {
            return Ok(());
        }

        // Write new file to next level
        let file_num = {
            let version_set = self.version_set.read();
            version_set.new_file_number()
        };
        let sst_path = self.db_path.join(format!("{file_num:06}.sst"));

        let mut builder = self.create_table_builder(&sst_path)?;
        for (key, value) in &merged {
            builder.add(key, value)?;
        }
        builder.finish(self.options.compression_type)?;

        // Get file size and key range
        let file_size = std::fs::metadata(&sst_path)
            .map_err(|e| Status::io_error(format!("Failed to get file size: {e}")))?
            .len();
        let smallest = merged.first().unwrap().0.clone();
        let largest = merged.last().unwrap().0.clone();

        // Create VersionEdit
        let mut edit = VersionEdit::new();

        // Delete old files
        for file in &level_files {
            edit.delete_file(level, file.number);
        }
        for file in &next_level_files {
            edit.delete_file(level + 1, file.number);
        }

        // Add new file
        let file_meta = FileMetaData::new(file_num, file_size, smallest, largest);
        edit.add_file(level + 1, file_meta);

        // Apply edit
        {
            let version_set = self.version_set.read();
            version_set.log_and_apply(edit)?;
        }

        // Delete old SSTable files
        for file in &level_files {
            let path = self.db_path.join(format!("{:06}.sst", file.number));
            let _ = std::fs::remove_file(path);
        }
        for file in &next_level_files {
            let path = self.db_path.join(format!("{:06}.sst", file.number));
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }

    /// Read all entries from a table
    fn read_all_from_table(&self, table: &mut TableReader) -> Result<Vec<(Slice, Slice)>> {
        table.scan_all()
    }

    /// Try to compact if needed
    pub fn maybe_compact(&self) -> Result<()> {
        let level = {
            let version_set = self.version_set.read();
            let current = version_set.current();
            let version = current.read();
            version.pick_compaction_level()
        };

        if let Some(level) = level {
            self.compact_level(level)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_db_open() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default());
        assert!(db.is_ok());
    }

    #[test]
    fn test_db_put_get() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();

        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value, Some(Slice::from("value1")));
    }

    #[test]
    fn test_db_delete() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();

        db.delete(&WriteOptions::default(), Slice::from("key1"))
            .unwrap();

        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_db_multiple_operations() {
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

        assert_eq!(
            db.get(&ReadOptions::default(), &Slice::from("key1"))
                .unwrap(),
            Some(Slice::from("value1"))
        );
        assert_eq!(
            db.get(&ReadOptions::default(), &Slice::from("key2"))
                .unwrap(),
            Some(Slice::from("value2"))
        );

        db.delete(&WriteOptions::default(), Slice::from("key1"))
            .unwrap();
        assert_eq!(
            db.get(&ReadOptions::default(), &Slice::from("key1"))
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_db_overwrite() {
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
            Slice::from("key1"),
            Slice::from("value2"),
        )
        .unwrap();

        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value, Some(Slice::from("value2")));
    }
}
