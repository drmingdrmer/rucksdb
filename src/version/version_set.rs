use crate::util::Result;
use crate::version::version::Version;
use crate::version::version_edit::VersionEdit;
use crate::wal;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// VersionSet manages the chain of versions and applies edits
///
/// It maintains:
/// - Current version (snapshot of all SSTables)
/// - MANIFEST file (persistent log of version edits)
/// - Next file number allocation
pub struct VersionSet {
    /// Database directory
    db_path: PathBuf,
    /// Current version
    current: Arc<RwLock<Version>>,
    /// Next file number to allocate
    next_file_number: Arc<AtomicU64>,
    /// Last sequence number
    last_sequence: Arc<AtomicU64>,
    /// MANIFEST file writer
    manifest_writer: Arc<RwLock<Option<wal::Writer>>>,
    /// MANIFEST file number
    _manifest_file_number: u64,
}

impl VersionSet {
    /// Create a new VersionSet
    pub fn new(db_path: &Path) -> Self {
        VersionSet {
            db_path: db_path.to_path_buf(),
            current: Arc::new(RwLock::new(Version::new())),
            next_file_number: Arc::new(AtomicU64::new(1)),
            last_sequence: Arc::new(AtomicU64::new(0)),
            manifest_writer: Arc::new(RwLock::new(None)),
            _manifest_file_number: 0,
        }
    }

    /// Open or create MANIFEST file
    pub fn open_or_create(&mut self) -> Result<()> {
        let manifest_path = self.db_path.join("MANIFEST");

        // Try to recover from existing MANIFEST
        if manifest_path.exists() {
            self.recover_from_manifest()?;
        } else {
            // Create new MANIFEST
            self.create_new_manifest()?;
        }

        Ok(())
    }

    /// Recover state from existing MANIFEST file
    fn recover_from_manifest(&mut self) -> Result<()> {
        let manifest_path = self.db_path.join("MANIFEST");
        let mut reader = wal::Reader::new(&manifest_path)?;

        let mut version = Version::new();
        let mut next_file_num = 1u64;
        let mut last_seq = 0u64;

        while let Some(record) = reader.read_record()? {
            if record.is_empty() {
                continue;
            }

            let edit = VersionEdit::decode(&record)?;

            // Apply edit to current version
            for (level, file) in &edit.new_files {
                version.add_file(*level, file.clone());
            }

            for (level, file_number) in &edit.deleted_files {
                version.remove_file(*level, *file_number);
            }

            // Update metadata
            if let Some(num) = edit.next_file_number {
                next_file_num = next_file_num.max(num);
            }

            if let Some(seq) = edit.last_sequence {
                last_seq = last_seq.max(seq);
            }
        }

        *self.current.write() = version;
        self.next_file_number.store(next_file_num, Ordering::SeqCst);
        self.last_sequence.store(last_seq, Ordering::SeqCst);

        // Open MANIFEST for appending
        let mut manifest_writer = wal::Writer::new(&manifest_path)?;
        manifest_writer.sync()?;
        *self.manifest_writer.write() = Some(manifest_writer);

        Ok(())
    }

    /// Create a new MANIFEST file
    fn create_new_manifest(&mut self) -> Result<()> {
        let manifest_path = self.db_path.join("MANIFEST");

        // Write initial VersionEdit
        let mut edit = VersionEdit::new();
        edit.set_comparator("bytewise".to_string());
        edit.set_next_file_number(1);
        edit.set_last_sequence(0);

        let manifest_writer = wal::Writer::new(&manifest_path)?;
        *self.manifest_writer.write() = Some(manifest_writer);

        self.log_and_apply(edit)?;

        Ok(())
    }

    /// Apply a VersionEdit and log it to MANIFEST
    pub fn log_and_apply(&self, mut edit: VersionEdit) -> Result<()> {
        // Set metadata if not already set
        if edit.next_file_number.is_none() {
            edit.set_next_file_number(self.next_file_number.load(Ordering::SeqCst));
        }

        if edit.last_sequence.is_none() {
            edit.set_last_sequence(self.last_sequence.load(Ordering::SeqCst));
        }

        // Apply edit to create new version
        let new_version = {
            let current = self.current.read();
            let mut new_version = Version::new();

            // Copy all files from current version
            for level in 0..current.files.len() {
                for file in &current.files[level] {
                    new_version.add_file(level, file.clone());
                }
            }

            // Apply deletions
            for (level, file_number) in &edit.deleted_files {
                new_version.remove_file(*level, *file_number);
            }

            // Apply additions
            for (level, file) in &edit.new_files {
                new_version.add_file(*level, file.clone());
            }

            new_version
        };

        // Write edit to MANIFEST
        let encoded = edit.encode();
        {
            let mut writer_guard = self.manifest_writer.write();
            if let Some(writer) = writer_guard.as_mut() {
                writer.add_record(&encoded)?;
                writer.sync()?;
            }
        }

        // Update current version
        *self.current.write() = new_version;

        // Update metadata
        if let Some(num) = edit.next_file_number {
            self.next_file_number.store(num, Ordering::SeqCst);
        }

        if let Some(seq) = edit.last_sequence {
            self.last_sequence.store(seq, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Get the current version
    pub fn current(&self) -> Arc<RwLock<Version>> {
        Arc::clone(&self.current)
    }

    /// Allocate a new file number
    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the last sequence number
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    /// Set the last sequence number
    pub fn set_last_sequence(&self, seq: u64) {
        self.last_sequence.store(seq, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::Slice;
    use crate::version::version_edit::FileMetaData;
    use tempfile::TempDir;

    #[test]
    fn test_version_set_new() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        std::fs::create_dir_all(&db_path).unwrap();

        let mut vset = VersionSet::new(&db_path);
        vset.open_or_create().unwrap();

        let current = vset.current();
        let version = current.read();
        assert_eq!(version.num_files(), 0);
    }

    #[test]
    fn test_version_set_log_and_apply() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        std::fs::create_dir_all(&db_path).unwrap();

        let mut vset = VersionSet::new(&db_path);
        vset.open_or_create().unwrap();

        // Add a file
        let mut edit = VersionEdit::new();
        edit.add_file(
            0,
            FileMetaData::new(1, 4096, Slice::from("a"), Slice::from("z")),
        );

        vset.log_and_apply(edit).unwrap();

        let current = vset.current();
        let version = current.read();
        assert_eq!(version.num_files(), 1);
        assert_eq!(version.num_level_files(0), 1);
    }

    #[test]
    fn test_version_set_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        std::fs::create_dir_all(&db_path).unwrap();

        // Create version set and add files
        {
            let mut vset = VersionSet::new(&db_path);
            vset.open_or_create().unwrap();

            let mut edit = VersionEdit::new();
            edit.add_file(
                0,
                FileMetaData::new(1, 4096, Slice::from("a"), Slice::from("m")),
            );
            edit.add_file(
                0,
                FileMetaData::new(2, 4096, Slice::from("n"), Slice::from("z")),
            );

            vset.log_and_apply(edit).unwrap();
        }

        // Reopen and verify recovery
        {
            let mut vset = VersionSet::new(&db_path);
            vset.open_or_create().unwrap();

            let current = vset.current();
            let version = current.read();
            assert_eq!(version.num_files(), 2);
            assert_eq!(version.num_level_files(0), 2);
        }
    }

    #[test]
    fn test_file_number_allocation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        std::fs::create_dir_all(&db_path).unwrap();

        let mut vset = VersionSet::new(&db_path);
        vset.open_or_create().unwrap();

        let num1 = vset.new_file_number();
        let num2 = vset.new_file_number();
        let num3 = vset.new_file_number();

        assert!(num2 > num1);
        assert!(num3 > num2);
    }
}
