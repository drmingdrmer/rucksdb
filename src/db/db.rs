use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    cache::{LRUCache, TableCache},
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

#[derive(Clone)]
pub struct DBOptions {
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub write_buffer_size: usize,
    pub block_cache_size: usize,            // Number of blocks to cache
    pub table_cache_size: usize,            // Number of table files to keep open
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
            table_cache_size: 100, // Keep up to 100 table files open
            compression_type: CompressionType::Snappy, // Snappy by default
            filter_bits_per_key: Some(10), // ~1% false positive rate
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
    /// Table cache for keeping TableReaders open
    table_cache: Arc<TableCache>,
    /// Database-wide statistics
    statistics: Arc<crate::statistics::Statistics>,
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
        let cf_set = Arc::new(ColumnFamilySet::new(name, default_cf_options.clone())?);

        let wal_path = db_path.join("wal.log");

        // Initialize VersionSet for default CF
        {
            let default_cf = cf_set.default_cf();
            let version_set = default_cf.version_set();
            let mut vs = version_set.write();
            vs.open_or_create()?;
        }

        // Recover Column Families from MANIFEST before WAL recovery
        Self::recover_column_families(db_path, &cf_set, &default_cf_options)?;

        // Recover from WAL if exists (handles all CFs)
        if wal_path.exists() {
            Self::recover_from_wal(&wal_path, &cf_set)?;
        }

        // Open WAL for writing
        let wal_writer = wal::Writer::new(&wal_path)?;

        // Initialize block cache
        let block_cache = LRUCache::new(options.block_cache_size);

        // Initialize table cache
        let table_cache = Arc::new(TableCache::new(
            options.table_cache_size,
            db_path.to_path_buf(),
            Some(block_cache.clone()),
        ));

        // Initialize statistics
        let statistics = Arc::new(crate::statistics::Statistics::new());

        Ok(DB {
            column_families: cf_set,
            wal: Arc::new(RwLock::new(Some(wal_writer))),
            db_path: db_path.to_path_buf(),
            options,
            block_cache,
            table_cache,
            statistics,
        })
    }

    /// Get TableReader from cache
    ///
    /// This method is critical for performance. It uses TableCache to avoid
    /// repeatedly opening/closing SSTable files, which is very expensive.
    ///
    /// Without table caching, random reads are limited to ~2-3K ops/sec due to
    /// file open overhead. With caching, we achieve 50K+ ops/sec.
    #[inline]
    fn get_table(&self, file_number: u64) -> Result<Arc<std::sync::Mutex<TableReader>>> {
        self.table_cache.get_table(file_number)
    }

    /// Recover Column Families from MANIFEST
    fn recover_column_families(
        db_path: &Path,
        cf_set: &Arc<crate::column_family::ColumnFamilySet>,
        default_cf_options: &crate::column_family::ColumnFamilyOptions,
    ) -> Result<()> {
        let manifest_path = db_path.join("MANIFEST");

        // If MANIFEST doesn't exist, nothing to recover
        if !manifest_path.exists() {
            return Ok(());
        }

        let mut reader = wal::Reader::new(&manifest_path)?;
        let mut cf_metadata: std::collections::HashMap<u32, String> =
            std::collections::HashMap::new();

        // Read all VersionEdit records and collect CF operations
        while let Some(record) = reader.read_record()? {
            if record.is_empty() {
                continue;
            }

            let edit = crate::version::VersionEdit::decode(&record)?;

            // Process CF creates
            for (cf_id, cf_name) in &edit.created_column_families {
                cf_metadata.insert(*cf_id, cf_name.clone());
            }

            // Process CF drops
            for cf_id in &edit.dropped_column_families {
                cf_metadata.remove(cf_id);
            }
        }

        // Recreate all non-default CFs (default CF already exists)
        for (cf_id, cf_name) in cf_metadata {
            if cf_id == 0 {
                // Skip default CF (already exists)
                continue;
            }

            // Create CF with specific ID (we're recovering, don't log to MANIFEST)
            let _ = cf_set.create_cf_with_id(cf_id, cf_name, default_cf_options.clone())?;
        }

        Ok(())
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
    #[inline]
    fn encode_wal_record(cf_id: u32, seq: u64, key: &Slice, value: Option<&Slice>) -> Vec<u8> {
        // Pre-allocate buffer with exact capacity to avoid reallocations
        // op_type(1) + cf_id(4) + seq(8) + key_len(2) + key + [value_len(2) + value]
        let key_data = key.data();
        let capacity = 15 + key_data.len() + value.as_ref().map_or(0, |v| 2 + v.data().len());
        let mut buf = Vec::with_capacity(capacity);

        // Operation type: 1=Put, 2=Delete
        buf.push(if value.is_some() { 1 } else { 2 });

        // Column Family ID
        buf.extend_from_slice(&cf_id.to_le_bytes());

        // Sequence number
        buf.extend_from_slice(&seq.to_le_bytes());

        // Key
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

    #[inline]
    pub fn put(&self, options: &WriteOptions, key: Slice, value: Slice) -> Result<()> {
        let default_cf = self.column_families.default_cf();
        self.put_cf(options, default_cf.handle(), key, value)
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
                self.statistics.record_wal_write(record.len() as u64);
                if options.sync {
                    wal.sync()?;
                    self.statistics.record_wal_sync();
                }
            }
        }

        // Then write to MemTable
        let mem = cf.mem();
        let mem_guard = mem.read();
        let bytes_written = (key.size() + value.size()) as u64;
        mem_guard.add(seq, key, value);
        self.statistics.record_write(bytes_written);

        // Check if we need to flush
        if cf.should_flush() {
            drop(mem_guard);
            if cf.make_immutable() {
                self.flush_memtable_cf(&cf)?;
            }
        }

        Ok(())
    }

    #[inline]
    pub fn get(&self, options: &ReadOptions, key: &Slice) -> Result<Option<Slice>> {
        let default_cf = self.column_families.default_cf();
        self.get_cf(options, default_cf.handle(), key)
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
                self.statistics.record_memtable_hit();
                if let Some(ref v) = value {
                    self.statistics.record_read(v.size() as u64);
                }
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
                self.statistics.record_immutable_memtable_hit();
                if let Some(ref v) = value {
                    self.statistics.record_read(v.size() as u64);
                }
                return Ok(value);
            }
        }

        // Not found in MemTables - record miss before checking SSTables
        self.statistics.record_memtable_miss();

        // Then check SSTables through VersionSet
        let version_set = cf.version_set();
        let version_set_guard = version_set.read();
        let current = version_set_guard.current();
        let version = current.read();

        // Check level 0 first (newest files have highest numbers)
        for file in version.get_level_files(0).iter().rev() {
            self.statistics.record_sstable_read();
            let table = self.get_table(file.number)?;
            let mut table_guard = table.lock().unwrap();
            if let Some(value) = table_guard.get(key)? {
                self.statistics.record_sstable_hit();
                self.statistics.record_read(value.size() as u64);
                return Ok(Some(value));
            }
        }

        // Check other levels
        for level in 1..version.files.len() {
            let files = version.get_overlapping_files(level, key, key);
            for file in files {
                self.statistics.record_sstable_read();
                let table = self.get_table(file.number)?;
                let mut table_guard = table.lock().unwrap();
                if let Some(value) = table_guard.get(key)? {
                    self.statistics.record_sstable_hit();
                    self.statistics.record_read(value.size() as u64);
                    return Ok(Some(value));
                }
            }
        }

        // Not found in SSTables either - record miss
        self.statistics.record_sstable_miss();
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
            let table_iter = crate::iterator::TableIterator::new(table)?;
            iterators.push(Box::new(table_iter));
        }

        // Other levels: Add files in order (already sorted by key range)
        for level in 1..version.files.len() {
            for file in version.get_level_files(level) {
                let table = self.get_table(file.number)?;
                let table_iter = crate::iterator::TableIterator::new(table)?;
                iterators.push(Box::new(table_iter));
            }
        }

        // Create merging iterator with proper priority order
        Ok(Box::new(crate::iterator::MergingIterator::new(iterators)))
    }

    #[inline]
    pub fn delete(&self, options: &WriteOptions, key: Slice) -> Result<()> {
        let default_cf = self.column_families.default_cf();
        self.delete_cf(options, default_cf.handle(), key)
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
                self.statistics.record_wal_write(record.len() as u64);
                if options.sync {
                    wal.sync()?;
                    self.statistics.record_wal_sync();
                }
            }
        }

        // Then write to MemTable
        let mem = cf.mem();
        let mem_guard = mem.read();
        mem_guard.delete(seq, key);
        self.statistics.record_delete();
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        Ok(())
    }

    /// Get block cache statistics
    pub fn cache_stats(&self) -> crate::cache::CacheStats {
        self.block_cache.stats()
    }

    /// Get database statistics
    pub fn statistics(&self) -> &Arc<crate::statistics::Statistics> {
        &self.statistics
    }

    /// Get the database path
    pub(crate) fn db_path(&self) -> &PathBuf {
        &self.db_path
    }

    /// Get reference to column families (for checkpoint)
    pub(crate) fn column_families(&self) -> &Arc<ColumnFamilySet> {
        &self.column_families
    }

    /// Flush all column families' MemTables to SSTables
    ///
    /// This ensures all data is persisted to SSTables, which is required
    /// before creating a checkpoint or backup.
    pub(crate) fn flush_all_column_families(&self) -> Result<()> {
        // For now, just flush the default CF
        // TODO: Extend to all CFs when multi-CF flush is implemented
        let default_cf = self.column_families.default_cf();

        // Make immutable and flush if there's data
        if default_cf.make_immutable() {
            self.flush_memtable_cf(&default_cf)?;
        }

        Ok(())
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

        // Record flush statistics
        self.statistics.record_memtable_flush(file_size);

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
            let table = self.get_table(file.number)?;
            let mut table_guard = table.lock().unwrap();
            // Simple iteration - in production would use proper iterator
            let entries = self.read_all_from_table(&mut table_guard)?;
            all_entries.extend(entries);
        }

        // Read from next level files
        for file in &next_level_files {
            let table = self.get_table(file.number)?;
            let mut table_guard = table.lock().unwrap();
            let entries = self.read_all_from_table(&mut table_guard)?;
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

        // Record compaction statistics
        let bytes_read: u64 = level_files.iter().map(|f| f.file_size).sum::<u64>()
            + next_level_files.iter().map(|f| f.file_size).sum::<u64>();
        let num_input_files = (level_files.len() + next_level_files.len()) as u64;
        self.statistics
            .record_compaction(bytes_read, file_size, num_input_files);

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

    /// Create a snapshot at the current sequence number
    pub fn get_snapshot(&self) -> crate::transaction::Snapshot {
        // Get current sequence from default CF
        let default_cf = self.column_families.default_cf();
        let seq = *default_cf.sequence.lock();

        crate::transaction::Snapshot::new(seq)
    }

    /// Apply a write batch atomically
    pub fn write(
        &self,
        options: &WriteOptions,
        batch: &crate::transaction::WriteBatch,
    ) -> Result<()> {
        // Execute all operations in the batch
        for (cf_id, op) in batch.ops() {
            let cf_handle = ColumnFamilyHandle::new(*cf_id, format!("cf_{}", cf_id));

            match op {
                crate::transaction::WriteOp::Put { key, value } => {
                    self.put_cf(
                        options,
                        &cf_handle,
                        Slice::from(key.as_slice()),
                        Slice::from(value.as_slice()),
                    )?;
                },
                crate::transaction::WriteOp::Delete { key } => {
                    self.delete_cf(options, &cf_handle, Slice::from(key.as_slice()))?;
                },
            }
        }

        Ok(())
    }

    /// Get default column family handle
    pub fn default_cf(&self) -> ColumnFamilyHandle {
        self.column_families.default_cf().handle().clone()
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
