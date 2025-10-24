use std::{path::Path, sync::Arc};

use parking_lot::{Mutex, RwLock};

use crate::{
    column_family::{ColumnFamilyHandle, ColumnFamilyOptions},
    memtable::MemTable,
    version::VersionSet,
};

/// Internal data for a Column Family
///
/// Represents the runtime state of a column family including:
/// - Active MemTable (current writes)
/// - Immutable MemTable (being flushed)
/// - Version history (SSTables)
/// - Configuration options
///
/// # Thread Safety
///
/// ColumnFamilyData uses interior mutability:
/// - RwLock for MemTable (many readers, exclusive writer)
/// - Mutex for immutable MemTable (exclusive access during flush)
///
/// # Lifecycle
///
/// ```text
/// Create CF → Write to MemTable → MemTable Full
///           ↓                           ↓
///      Active MemTable          → Immutable MemTable
///                                       ↓
///                                Flush to SSTable
///                                       ↓
///                                Add to Version
/// ```
#[allow(dead_code)]
pub struct ColumnFamilyData {
    /// Column family ID (unique across DB)
    id: u32,

    /// Column family name
    name: String,

    /// Configuration options for this CF
    options: ColumnFamilyOptions,

    /// Active MemTable (receives new writes)
    mem: Arc<RwLock<MemTable>>,

    /// Immutable MemTable (being flushed to disk)
    /// None if no flush in progress
    imm: Arc<RwLock<Option<MemTable>>>,

    /// Sequence number for this CF
    /// Each CF maintains its own sequence for MVCC
    pub(crate) sequence: Arc<Mutex<u64>>,

    /// Version set for this CF (SSTable history)
    version_set: Arc<RwLock<VersionSet>>,

    /// Reference to this CF as a handle
    handle: ColumnFamilyHandle,
}

#[allow(dead_code)]
impl ColumnFamilyData {
    /// Create a new column family data
    pub fn new(id: u32, name: String, options: ColumnFamilyOptions, db_path: &str) -> Self {
        let handle = ColumnFamilyHandle::new(id, name.clone());
        let version_set = VersionSet::new(Path::new(db_path));

        ColumnFamilyData {
            id,
            name,
            options,
            mem: Arc::new(RwLock::new(MemTable::new())),
            imm: Arc::new(RwLock::new(None)),
            sequence: Arc::new(Mutex::new(0)),
            version_set: Arc::new(RwLock::new(version_set)),
            handle,
        }
    }

    /// Get the column family ID
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get the column family name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the column family options
    pub fn options(&self) -> &ColumnFamilyOptions {
        &self.options
    }

    /// Get a handle to this column family
    pub fn handle(&self) -> &ColumnFamilyHandle {
        &self.handle
    }

    /// Get the active MemTable
    pub fn mem(&self) -> Arc<RwLock<MemTable>> {
        Arc::clone(&self.mem)
    }

    /// Get the immutable MemTable
    pub fn imm(&self) -> Arc<RwLock<Option<MemTable>>> {
        Arc::clone(&self.imm)
    }

    /// Get the version set
    pub fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        Arc::clone(&self.version_set)
    }

    /// Allocate a new sequence number
    pub fn next_sequence(&self) -> u64 {
        let mut seq = self.sequence.lock();
        *seq += 1;
        *seq
    }

    /// Get current sequence number
    pub fn current_sequence(&self) -> u64 {
        *self.sequence.lock()
    }

    /// Check if MemTable should be flushed
    pub fn should_flush(&self) -> bool {
        let mem = self.mem.read();
        mem.approximate_memory_usage() >= self.options.write_buffer_size
    }

    /// Make current MemTable immutable and create new one
    ///
    /// Returns true if rotation successful, false if immutable already exists
    pub fn make_immutable(&self) -> bool {
        let mut imm = self.imm.write();
        if imm.is_some() {
            return false; // Already have immutable, wait for flush
        }

        let mut mem = self.mem.write();
        let old_mem = std::mem::take(&mut *mem);
        *imm = Some(old_mem);
        true
    }

    /// Clear immutable MemTable after successful flush
    pub fn clear_immutable(&self) {
        let mut imm = self.imm.write();
        *imm = None;
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_column_family_data_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_cf");

        let cf = ColumnFamilyData::new(
            1,
            "test_cf".to_string(),
            ColumnFamilyOptions::default(),
            db_path.to_str().unwrap(),
        );

        assert_eq!(cf.id(), 1);
        assert_eq!(cf.name(), "test_cf");
        assert_eq!(cf.current_sequence(), 0);
        assert!(!cf.should_flush());
    }

    #[test]
    fn test_sequence_allocation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_cf");

        let cf = ColumnFamilyData::new(
            1,
            "test_cf".to_string(),
            ColumnFamilyOptions::default(),
            db_path.to_str().unwrap(),
        );

        assert_eq!(cf.next_sequence(), 1);
        assert_eq!(cf.next_sequence(), 2);
        assert_eq!(cf.next_sequence(), 3);
        assert_eq!(cf.current_sequence(), 3);
    }

    #[test]
    fn test_make_immutable() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_cf");

        let cf = ColumnFamilyData::new(
            1,
            "test_cf".to_string(),
            ColumnFamilyOptions::default(),
            db_path.to_str().unwrap(),
        );

        // First rotation should succeed
        assert!(cf.make_immutable());

        // Second rotation should fail (immutable exists)
        assert!(!cf.make_immutable());

        // Clear and try again
        cf.clear_immutable();
        assert!(cf.make_immutable());
    }
}
