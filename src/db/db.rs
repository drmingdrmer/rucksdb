use crate::memtable::MemTable;
use crate::table::{TableBuilder, TableReader};
use crate::util::{Result, Slice, Status};
use crate::wal;
use parking_lot::RwLock;
use std::fs;
use std::path::{Path, PathBuf};
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
    wal: Arc<RwLock<Option<wal::Writer>>>,
    db_path: PathBuf,
    options: DBOptions,
    // SSTable management
    sstables: Arc<RwLock<Vec<TableReader>>>,
    next_file_number: Arc<AtomicU64>,
}

impl DB {
    pub fn open(name: &str, options: DBOptions) -> Result<Self> {
        let db_path = Path::new(name);

        // Create directory if needed
        if options.create_if_missing {
            fs::create_dir_all(db_path)
                .map_err(|e| Status::io_error(format!("Failed to create directory: {}", e)))?;
        }

        if options.error_if_exists && db_path.exists() {
            return Err(Status::invalid_argument("Database already exists"));
        }

        let wal_path = db_path.join("wal.log");
        let mem = Arc::new(RwLock::new(MemTable::new()));
        let sequence = Arc::new(AtomicU64::new(0));

        // Load existing SSTable files
        let (sstables, max_file_num) = Self::load_sstables(db_path)?;

        // Recover from WAL if exists
        if wal_path.exists() {
            Self::recover_from_wal(&wal_path, &mem, &sequence)?;
        }

        // Open WAL for writing
        let wal_writer = wal::Writer::new(&wal_path)?;

        Ok(DB {
            mem,
            sequence,
            wal: Arc::new(RwLock::new(Some(wal_writer))),
            db_path: db_path.to_path_buf(),
            options,
            sstables: Arc::new(RwLock::new(sstables)),
            next_file_number: Arc::new(AtomicU64::new(max_file_num + 1)),
        })
    }

    /// Load existing SSTable files
    fn load_sstables(db_path: &Path) -> Result<(Vec<TableReader>, u64)> {
        let mut sstables = Vec::new();
        let mut max_file_num = 0u64;

        if !db_path.exists() {
            return Ok((sstables, max_file_num));
        }

        let entries = fs::read_dir(db_path)
            .map_err(|e| Status::io_error(format!("Failed to read directory: {}", e)))?;

        let mut sst_files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| Status::io_error(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();

            if let Some(filename) = path.file_name() {
                if let Some(name) = filename.to_str() {
                    if name.ends_with(".sst") {
                        // Extract file number from filename (e.g., "000001.sst" -> 1)
                        if let Some(num_str) = name.strip_suffix(".sst") {
                            if let Ok(num) = num_str.parse::<u64>() {
                                max_file_num = max_file_num.max(num);
                                sst_files.push((num, path));
                            }
                        }
                    }
                }
            }
        }

        // Sort by file number (oldest first)
        sst_files.sort_by_key(|(num, _)| *num);

        // Open all SSTable files
        for (_, path) in sst_files {
            let reader = TableReader::open(&path)?;
            sstables.push(reader);
        }

        Ok((sstables, max_file_num))
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
            if value.is_some() {
                mem_guard.add(seq, key, value.unwrap());
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
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get(&self, _options: &ReadOptions, key: &Slice) -> Result<Option<Slice>> {
        // First check MemTable
        {
            let mem = self.mem.read();
            let (found, value) = mem.get(key);
            if found {
                // Key exists in MemTable (either with value or deleted)
                return Ok(value);
            }
        }

        // Then check SSTables in reverse order (newest first)
        let mut sstables = self.sstables.write();
        for table in sstables.iter_mut().rev() {
            if let Some(value) = table.get(key)? {
                return Ok(Some(value));
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

    /// Flush MemTable to SSTable
    fn flush_memtable(&self) -> Result<()> {
        // Get all entries from MemTable
        let entries = {
            let mem = self.mem.read();
            mem.collect_entries()
        };

        if entries.is_empty() {
            return Ok(());
        }

        // Generate new SSTable file
        let file_num = self.next_file_number.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.db_path.join(format!("{:06}.sst", file_num));

        // Build SSTable
        let mut builder = TableBuilder::new(&sst_path)?;
        for (key, value) in &entries {
            builder.add(key, value)?;
        }
        builder.finish()?;

        // Open the new SSTable for reading
        let reader = TableReader::open(&sst_path)?;

        // Add to sstables list
        {
            let mut sstables = self.sstables.write();
            sstables.push(reader);
        }

        // Clear MemTable
        {
            let mut mem = self.mem.write();
            *mem = MemTable::new();
        }

        // Clear WAL
        {
            let wal_path = self.db_path.join("wal.log");
            let mut wal_guard = self.wal.write();
            *wal_guard = Some(wal::Writer::new(&wal_path)?);
        }

        Ok(())
    }

    fn should_flush(&self) -> bool {
        let mem = self.mem.read();
        mem.approximate_memory_usage() >= self.options.write_buffer_size
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

        let value = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
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

        let value = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
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

        let value = db.get(&ReadOptions::default(), &Slice::from("key1")).unwrap();
        assert_eq!(value, Some(Slice::from("value2")));
    }
}
