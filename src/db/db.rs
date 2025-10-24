use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    cache::LRUCache,
    column_family::{ColumnFamilyHandle, ColumnFamilySet},
    filter::{BloomFilterPolicy, FilterPolicy},
    table::{CompressionType, TableBuilder, TableReader},
    util::{Result, Slice, Status},
    version::{FileMetaData, VersionEdit},
    wal,
};

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
            write_buffer_size: 4 * 1024 * 1024, // 4MB
            block_cache_size: 1000,             /* Cache up to 1000 blocks (~4MB with 4KB
                                                 * blocks) */
            compression_type: CompressionType::Snappy, // Snappy by default
            filter_bits_per_key: Some(10),             // ~1% false positive rate
        }
    }
}

pub struct DB {
    /// Manages all column families
    column_families: Arc<ColumnFamilySet>,
    /// Write-ahead log (shared across all CFs)
    wal: Arc<RwLock<Option<wal::Writer>>>,
    /// Database directory path
    db_path: PathBuf,
    /// Global database options
    options: DBOptions,
    /// Block cache shared across all CFs: (file_number, block_offset) ->
    /// block_data
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

        // Create ColumnFamilySet with default CF using global options
        let default_cf_options = crate::column_family::ColumnFamilyOptions {
            write_buffer_size: options.write_buffer_size,
            compression_type: options.compression_type,
            filter_bits_per_key: options.filter_bits_per_key,
            block_cache_size: options.block_cache_size,
        };
        let cf_set = Arc::new(ColumnFamilySet::new(name, default_cf_options)?);

        let wal_path = db_path.join("wal.log");

        // Initialize VersionSet for default CF
        {
            let default_cf = cf_set.default_cf();
            let version_set = default_cf.version_set();
            let mut vs = version_set.write();
            vs.open_or_create()?;
        }

        // Recover from WAL if exists (handles all CFs)
        if wal_path.exists() {
            Self::recover_from_wal(&wal_path, &cf_set)?;
        }

        // Open WAL for writing
        let wal_writer = wal::Writer::new(&wal_path)?;

        // Initialize block cache
        let block_cache = LRUCache::new(options.block_cache_size);

        Ok(DB {
            column_families: cf_set,
            wal: Arc::new(RwLock::new(Some(wal_writer))),
            db_path: db_path.to_path_buf(),
            options,
            block_cache,
        })
    }

    /// Get TableReader with block cache
    fn get_table(&self, file_number: u64) -> Result<TableReader> {
        let sst_path = self.db_path.join(format!("{file_number:06}.sst"));
        TableReader::open(&sst_path, file_number, Some(self.block_cache.clone()))
    }

    /// Recover data from WAL (multi-CF aware)
    fn recover_from_wal(
        wal_path: &Path,
        cf_set: &Arc<crate::column_family::ColumnFamilySet>,
    ) -> Result<()> {
        let mut reader = wal::Reader::new(wal_path)?;
        let mut cf_max_seqs: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();

        while let Some(record) = reader.read_record()? {
            if record.is_empty() {
                continue;
            }

            let (cf_id, seq, key, value) = Self::decode_wal_record(&record)?;

            // Track max sequence per CF
            cf_max_seqs
                .entry(cf_id)
                .and_modify(|max_seq| *max_seq = (*max_seq).max(seq))
                .or_insert(seq);

            // Get CF by ID - create handle and lookup
            let cf_handle =
                crate::column_family::ColumnFamilyHandle::new(cf_id, format!("cf_{}", cf_id));
            if let Some(cf) = cf_set.get_cf(&cf_handle) {
                let mem = cf.mem();
                let mem_guard = mem.write();
                if let Some(val) = value {
                    mem_guard.add(seq, key, val);
                } else {
                    mem_guard.delete(seq, key);
                }
            } else {
                // CF doesn't exist anymore - skip this record
                // This can happen if a CF was dropped after WAL write but before recovery
                continue;
            }
        }

        // Update sequence numbers for all CFs that had WAL entries
        for (cf_id, max_seq) in cf_max_seqs {
            let cf_handle =
                crate::column_family::ColumnFamilyHandle::new(cf_id, format!("cf_{}", cf_id));
            if let Some(cf) = cf_set.get_cf(&cf_handle) {
                *cf.sequence.lock() = max_seq + 1;

                // Update VersionSet
                let version_set = cf.version_set();
                let vs = version_set.read();
                vs.set_last_sequence(max_seq + 1);
            }
        }

        Ok(())
    }

    /// Encode WAL record: op_type(1) + cf_id(4) + seq(8) + key_len(2) + key +
    /// [value_len(2) + value]
    fn encode_wal_record(cf_id: u32, seq: u64, key: &Slice, value: Option<&Slice>) -> Vec<u8> {
        let mut buf = Vec::new();

        // Operation type: 1=Put, 2=Delete
        buf.push(if value.is_some() { 1 } else { 2 });

        // Column Family ID
        buf.extend_from_slice(&cf_id.to_le_bytes());

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
    fn decode_wal_record(data: &[u8]) -> Result<(u32, u64, Slice, Option<Slice>)> {
        if data.len() < 15 {
            // op_type(1) + cf_id(4) + seq(8) + key_len(2) = 15 minimum
            return Err(Status::corruption("WAL record too short"));
        }

        let op_type = data[0];
        let cf_id = u32::from_le_bytes(data[1..5].try_into().unwrap());
        let seq = u64::from_le_bytes(data[5..13].try_into().unwrap());

        let key_len = u16::from_le_bytes([data[13], data[14]]) as usize;
        if data.len() < 15 + key_len {
            return Err(Status::corruption("Invalid key length"));
        }

        let key = Slice::from(&data[15..15 + key_len]);

        let value = if op_type == 1 {
            // Put operation
            let val_start = 15 + key_len;
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

        Ok((cf_id, seq, key, value))
    }

    pub fn put(&self, options: &WriteOptions, key: Slice, value: Slice) -> Result<()> {
        let default_cf = self.column_families.default_cf();
        self.put_cf(options, &default_cf.handle().clone(), key, value)
    }

    pub fn put_cf(
        &self,
        options: &WriteOptions,
        cf_handle: &ColumnFamilyHandle,
        key: Slice,
        value: Slice,
    ) -> Result<()> {
        let cf = self
            .column_families
            .get_cf(cf_handle)
            .ok_or_else(|| Status::invalid_argument("Column family not found"))?;

        let seq = cf.next_sequence();

        // Write to WAL first
        let record = Self::encode_wal_record(cf_handle.id(), seq, &key, Some(&value));
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
        let mem = cf.mem();
        let mem_guard = mem.read();
        mem_guard.add(seq, key, value);

        // Check if we need to flush
        if cf.should_flush() {
            drop(mem_guard);
            if cf.make_immutable() {
                self.flush_memtable_cf(&cf)?;
            }
        }

        Ok(())
    }

    pub fn get(&self, options: &ReadOptions, key: &Slice) -> Result<Option<Slice>> {
        let default_cf = self.column_families.default_cf();
        self.get_cf(options, &default_cf.handle().clone(), key)
    }

    pub fn get_cf(
        &self,
        _options: &ReadOptions,
        cf_handle: &ColumnFamilyHandle,
        key: &Slice,
    ) -> Result<Option<Slice>> {
        let cf = self
            .column_families
            .get_cf(cf_handle)
            .ok_or_else(|| Status::invalid_argument("Column family not found"))?;

        // First check mutable MemTable
        {
            let mem = cf.mem();
            let mem_guard = mem.read();
            let (found, value) = mem_guard.get(key);
            if found {
                // Key exists in MemTable (either with value or deleted)
                return Ok(value);
            }
        }

        // Then check immutable MemTable
        {
            let imm = cf.imm();
            let imm_guard = imm.read();
            if let Some(imm_table) = imm_guard.as_ref()
                && let (true, value) = imm_table.get(key)
            {
                return Ok(value);
            }
        }

        // Then check SSTables through VersionSet
        let version_set = cf.version_set();
        let version_set_guard = version_set.read();
        let current = version_set_guard.current();
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

    /// Create an iterator for scanning the database (default CF)
    pub fn iter(&self) -> Result<Box<dyn crate::iterator::Iterator>> {
        let default_cf = self.column_families.default_cf();
        self.iter_cf(&default_cf.handle().clone())
    }

    /// Create an iterator for scanning the database
    ///
    /// Returns a MergingIterator that combines all data sources in priority
    /// order:
    /// 1. Active MemTable (highest priority - most recent writes)
    /// 2. Immutable MemTable (being flushed)
    /// 3. Level 0 SSTables (newest to oldest)
    /// 4. Level 1+ SSTables (ordered by key range)
    ///
    /// The iterator automatically handles:
    /// - Shadowing: Newer values override older ones for the same key
    /// - Deletion markers: Deleted keys are filtered out
    /// - Multi-level merge: Efficient O(log k) seek and next operations
    pub fn iter_cf(
        &self,
        cf_handle: &ColumnFamilyHandle,
    ) -> Result<Box<dyn crate::iterator::Iterator>> {
        let cf = self
            .column_families
            .get_cf(cf_handle)
            .ok_or_else(|| Status::invalid_argument("Column family not found"))?;

        let mut iterators: Vec<Box<dyn crate::iterator::Iterator>> = Vec::new();

        // 1. Active MemTable (highest priority)
        {
            let mem = cf.mem();
            let mem_guard = mem.read();
            iterators.push(Box::new(mem_guard.iter()));
        }

        // 2. Immutable MemTable (if exists)
        {
            let imm = cf.imm();
            let imm_guard = imm.read();
            if let Some(imm_table) = imm_guard.as_ref() {
                iterators.push(Box::new(imm_table.iter()));
            }
        }

        // 3. SSTables from VersionSet
        let version_set = cf.version_set();
        let version_set_guard = version_set.read();
        let current = version_set_guard.current();
        let version = current.read();

        // Level 0: Add in reverse order (newest files first for priority)
        for file in version.get_level_files(0).iter().rev() {
            let table = self.get_table(file.number)?;
            let table_iter =
                crate::iterator::TableIterator::new(Arc::new(std::sync::Mutex::new(table)))?;
            iterators.push(Box::new(table_iter));
        }

        // Other levels: Add files in order (already sorted by key range)
        for level in 1..version.files.len() {
            for file in version.get_level_files(level) {
                let table = self.get_table(file.number)?;
                let table_iter =
                    crate::iterator::TableIterator::new(Arc::new(std::sync::Mutex::new(table)))?;
                iterators.push(Box::new(table_iter));
            }
        }

        // Create merging iterator with proper priority order
        Ok(Box::new(crate::iterator::MergingIterator::new(iterators)))
    }

    pub fn delete(&self, options: &WriteOptions, key: Slice) -> Result<()> {
        let default_cf = self.column_families.default_cf();
        self.delete_cf(options, &default_cf.handle().clone(), key)
    }

    pub fn delete_cf(
        &self,
        options: &WriteOptions,
        cf_handle: &ColumnFamilyHandle,
        key: Slice,
    ) -> Result<()> {
        let cf = self
            .column_families
            .get_cf(cf_handle)
            .ok_or_else(|| Status::invalid_argument("Column family not found"))?;

        let seq = cf.next_sequence();

        // Write to WAL first
        let record = Self::encode_wal_record(cf_handle.id(), seq, &key, None);
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
        let mem = cf.mem();
        let mem_guard = mem.read();
        mem_guard.delete(seq, key);
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        Ok(())
    }

    /// Get block cache statistics
    pub fn cache_stats(&self) -> crate::cache::CacheStats {
        self.block_cache.stats()
    }

    /// Create a new column family
    pub fn create_column_family(
        &self,
        name: &str,
        options: crate::column_family::ColumnFamilyOptions,
    ) -> Result<ColumnFamilyHandle> {
        let handle = self.column_families.create_cf(name.to_string(), options)?;

        // Log CF creation to MANIFEST (use default CF's VersionSet)
        let default_cf = self.column_families.default_cf();
        let version_set = default_cf.version_set();
        let version_set_guard = version_set.read();

        let mut edit = crate::version::VersionEdit::new();
        edit.create_column_family(handle.id(), handle.name().to_string());
        version_set_guard.log_and_apply(edit)?;

        Ok(handle)
    }

    /// Drop a column family
    pub fn drop_column_family(&self, cf_handle: &ColumnFamilyHandle) -> Result<()> {
        let cf_id = cf_handle.id();
        self.column_families.drop_cf(cf_handle)?;

        // Log CF drop to MANIFEST (use default CF's VersionSet)
        let default_cf = self.column_families.default_cf();
        let version_set = default_cf.version_set();
        let version_set_guard = version_set.read();

        let mut edit = crate::version::VersionEdit::new();
        edit.drop_column_family(cf_id);
        version_set_guard.log_and_apply(edit)?;

        Ok(())
    }

    /// List all column families
    pub fn list_column_families(&self) -> Vec<ColumnFamilyHandle> {
        self.column_families.list_column_families()
    }

    /// Flush MemTable to SSTable for a specific CF
    fn flush_memtable_cf(&self, cf: &Arc<crate::column_family::ColumnFamilyData>) -> Result<()> {
        // Get all entries from immutable MemTable
        let entries = {
            let imm = cf.imm();
            let imm_guard = imm.read();
            match imm_guard.as_ref() {
                Some(imm_table) => imm_table.collect_entries(),
                None => return Ok(()), // Nothing to flush
            }
        };

        if entries.is_empty() {
            // Clear empty immutable MemTable
            cf.clear_immutable();
            return Ok(());
        }

        // Generate new SSTable file
        let file_num = {
            let version_set = cf.version_set();
            let version_set_guard = version_set.read();
            version_set_guard.new_file_number()
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
        edit.set_last_sequence(cf.current_sequence());

        // Apply edit to VersionSet
        {
            let version_set = cf.version_set();
            let version_set_guard = version_set.read();
            version_set_guard.log_and_apply(edit)?;
        }

        // Clear immutable MemTable (mem continues to accept writes)
        cf.clear_immutable();

        // Clear WAL (all data is now persisted)
        // TODO: Multi-CF WAL needs per-CF tracking
        {
            let wal_path = self.db_path.join("wal.log");
            let mut wal_guard = self.wal.write();
            *wal_guard = Some(wal::Writer::new(&wal_path)?);
        }

        Ok(())
    }

    /// Create a TableBuilder with configured compression and filter options
    fn create_table_builder<P: AsRef<Path>>(&self, path: P) -> Result<TableBuilder> {
        let filter_policy = self.options.filter_bits_per_key.map(|bits_per_key| {
            Arc::new(BloomFilterPolicy::new(bits_per_key)) as Arc<dyn FilterPolicy>
        });

        TableBuilder::new_with_filter(path, filter_policy)
    }

    /// Compact a level by merging files into the next level (default CF)
    pub fn compact_level(&self, level: usize) -> Result<()> {
        let default_cf = self.column_families.default_cf();
        self.compact_level_cf(&default_cf.handle().clone(), level)
    }

    /// Compact a level for a specific CF
    pub fn compact_level_cf(&self, cf_handle: &ColumnFamilyHandle, level: usize) -> Result<()> {
        if level >= 6 {
            return Ok(()); // No compaction for last level
        }

        let cf = self
            .column_families
            .get_cf(cf_handle)
            .ok_or_else(|| Status::invalid_argument("Column family not found"))?;

        // Get files to compact
        let (level_files, next_level_files) = {
            let version_set = cf.version_set();
            let version_set_guard = version_set.read();
            let current = version_set_guard.current();
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
            let version_set = cf.version_set();
            let version_set_guard = version_set.read();
            version_set_guard.new_file_number()
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
            let version_set = cf.version_set();
            let version_set_guard = version_set.read();
            version_set_guard.log_and_apply(edit)?;
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

    /// Try to compact if needed (default CF)
    pub fn maybe_compact(&self) -> Result<()> {
        let default_cf = self.column_families.default_cf();
        self.maybe_compact_cf(&default_cf.handle().clone())
    }

    /// Try to compact if needed for a specific CF
    pub fn maybe_compact_cf(&self, cf_handle: &ColumnFamilyHandle) -> Result<()> {
        let cf = self
            .column_families
            .get_cf(cf_handle)
            .ok_or_else(|| Status::invalid_argument("Column family not found"))?;

        let level = {
            let version_set = cf.version_set();
            let version_set_guard = version_set.read();
            let current = version_set_guard.current();
            let version = current.read();
            version.pick_compaction_level()
        };

        if let Some(level) = level {
            self.compact_level_cf(cf_handle, level)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

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

    #[test]
    fn test_db_iterator() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Insert multiple keys
        db.put(
            &WriteOptions::default(),
            Slice::from("key3"),
            Slice::from("value3"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key5"),
            Slice::from("value5"),
        )
        .unwrap();
        db.put(
            &WriteOptions::default(),
            Slice::from("key2"),
            Slice::from("value2"),
        )
        .unwrap();

        // Create iterator and scan all keys
        let mut iter = db.iter().unwrap();
        assert!(iter.seek_to_first().unwrap());

        let mut collected = Vec::new();
        loop {
            collected.push((iter.key(), iter.value()));
            if !iter.next().unwrap() {
                break;
            }
        }

        // Verify sorted order
        assert_eq!(collected.len(), 4);
        assert_eq!(collected[0], (Slice::from("key1"), Slice::from("value1")));
        assert_eq!(collected[1], (Slice::from("key2"), Slice::from("value2")));
        assert_eq!(collected[2], (Slice::from("key3"), Slice::from("value3")));
        assert_eq!(collected[3], (Slice::from("key5"), Slice::from("value5")));
    }

    #[test]
    fn test_db_iterator_with_deletion() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Insert and delete keys
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
        db.put(
            &WriteOptions::default(),
            Slice::from("key3"),
            Slice::from("value3"),
        )
        .unwrap();
        db.delete(&WriteOptions::default(), Slice::from("key2"))
            .unwrap();

        // Verify key2 is deleted via get()
        let key2_value = db
            .get(&ReadOptions::default(), &Slice::from("key2"))
            .unwrap();
        assert_eq!(key2_value, None, "key2 should be deleted");

        // Verify iterator also filters deleted keys
        let mut iter = db.iter().unwrap();
        assert!(iter.seek_to_first().unwrap());

        let mut collected = Vec::new();
        loop {
            collected.push((iter.key(), iter.value()));
            if !iter.next().unwrap() {
                break;
            }
        }

        // Should only see key1 and key3, not the deleted key2
        assert_eq!(
            collected.len(),
            2,
            "Should only have 2 entries (key2 deleted)"
        );
        assert_eq!(collected[0], (Slice::from("key1"), Slice::from("value1")));
        assert_eq!(collected[1], (Slice::from("key3"), Slice::from("value3")));
    }

    #[test]
    fn test_multi_column_family() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Create two column families
        let users_cf = db
            .create_column_family(
                "users",
                crate::column_family::ColumnFamilyOptions::default(),
            )
            .unwrap();
        let posts_cf = db
            .create_column_family(
                "posts",
                crate::column_family::ColumnFamilyOptions::default(),
            )
            .unwrap();

        // Write to different CFs
        db.put_cf(
            &WriteOptions::default(),
            &users_cf,
            Slice::from("user1"),
            Slice::from("alice"),
        )
        .unwrap();
        db.put_cf(
            &WriteOptions::default(),
            &posts_cf,
            Slice::from("post1"),
            Slice::from("hello world"),
        )
        .unwrap();

        // Verify data in each CF
        let user = db
            .get_cf(&ReadOptions::default(), &users_cf, &Slice::from("user1"))
            .unwrap();
        assert_eq!(user, Some(Slice::from("alice")));

        let post = db
            .get_cf(&ReadOptions::default(), &posts_cf, &Slice::from("post1"))
            .unwrap();
        assert_eq!(post, Some(Slice::from("hello world")));

        // Verify isolation: post1 doesn't exist in users CF
        let not_found = db
            .get_cf(&ReadOptions::default(), &users_cf, &Slice::from("post1"))
            .unwrap();
        assert_eq!(not_found, None);

        // List CFs
        let cfs = db.list_column_families();
        assert_eq!(cfs.len(), 3); // default, users, posts
    }
}
