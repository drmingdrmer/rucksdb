use std::{fs, path::Path};

use crate::{
    DB,
    util::{Result, Status},
};

/// Checkpoint creates a consistent point-in-time snapshot of the database
///
/// A checkpoint is a consistent snapshot created by:
/// 1. Flushing all MemTables to SSTables
/// 2. Hard-linking (or copying) all SSTable files
/// 3. Copying MANIFEST and metadata files
///
/// Checkpoints are useful for:
/// - Creating backups without stopping writes
/// - Creating read-only replicas
/// - Point-in-time recovery
pub struct Checkpoint;

impl Checkpoint {
    /// Create a checkpoint of the database at the specified directory
    ///
    /// This creates a consistent snapshot by:
    /// 1. Flushing all active MemTables (ensures all data is in SSTables)
    /// 2. Listing all live SSTable files from VersionSet
    /// 3. Hard-linking SSTable files (falls back to copy if hard link fails)
    /// 4. Copying MANIFEST and CURRENT files
    ///
    /// The checkpoint directory will contain a complete, consistent copy of the
    /// database that can be opened independently.
    ///
    /// # Arguments
    /// * `db` - The database to checkpoint
    /// * `checkpoint_dir` - Directory to create checkpoint in (must not exist)
    ///
    /// # Returns
    /// Ok(()) on success, or an error if checkpoint creation fails
    pub fn create(db: &DB, checkpoint_dir: &Path) -> Result<()> {
        // Create checkpoint directory
        if checkpoint_dir.exists() {
            return Err(Status::invalid_argument(
                "Checkpoint directory already exists",
            ));
        }

        fs::create_dir_all(checkpoint_dir)
            .map_err(|e| Status::io_error(format!("Failed to create checkpoint directory: {e}")))?;

        // Step 1: Flush all MemTables to ensure all data is in SSTables
        db.flush_all_column_families()?;

        // Step 2: Get list of all live files
        let db_path = db.db_path();
        let live_files = Self::get_live_files(db)?;

        // Step 3: Hard link or copy all SSTable files
        for file in &live_files {
            let src = db_path.join(file);
            let dst = checkpoint_dir.join(file);

            if !src.exists() {
                continue;
            }

            // Try hard link first (fast), fall back to copy
            if let Err(_e) = fs::hard_link(&src, &dst) {
                fs::copy(&src, &dst)
                    .map_err(|e| Status::io_error(format!("Failed to copy file {file}: {e}")))?;
            }
        }

        // Step 4: Copy MANIFEST file (can't hard link as it's being written)
        let manifest_files: Vec<_> = fs::read_dir(db_path)
            .map_err(|e| Status::io_error(format!("Failed to read db directory: {e}")))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.starts_with("MANIFEST"))
                    .unwrap_or(false)
            })
            .collect();

        for manifest_entry in manifest_files {
            let src = manifest_entry.path();
            let dst = checkpoint_dir.join(manifest_entry.file_name());
            fs::copy(&src, &dst)
                .map_err(|e| Status::io_error(format!("Failed to copy MANIFEST: {e}")))?;
        }

        // Step 5: Copy CURRENT file
        let current_file = db_path.join("CURRENT");
        if current_file.exists() {
            let dst = checkpoint_dir.join("CURRENT");
            fs::copy(&current_file, &dst)
                .map_err(|e| Status::io_error(format!("Failed to copy CURRENT: {e}")))?;
        }

        Ok(())
    }

    /// Get list of all live SSTable files from the database
    fn get_live_files(db: &DB) -> Result<Vec<String>> {
        let mut files = Vec::new();

        // Get all column families
        let cf_set = db.column_families();
        let default_cf = cf_set.default_cf();

        // For now, just get files from default CF
        // TODO: Extend to all CFs
        let version_set = default_cf.version_set();
        let version_set_guard = version_set.read();
        let current = version_set_guard.current();
        let version = current.read();

        // Collect all SSTable files from all levels
        for level in 0..version.files.len() {
            for file in version.get_level_files(level) {
                files.push(format!("{:06}.sst", file.number));
            }
        }

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{DBOptions, ReadOptions, Slice, WriteOptions};

    #[test]
    fn test_checkpoint_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let checkpoint_path = temp_dir.path().join("checkpoint");

        // Create database and write some data
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

        // Create checkpoint
        Checkpoint::create(&db, &checkpoint_path).unwrap();

        // Verify checkpoint directory exists and has files
        assert!(checkpoint_path.exists());
        assert!(checkpoint_path.is_dir());

        // Open checkpoint as a new database
        let checkpoint_db =
            DB::open(checkpoint_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Verify data exists in checkpoint
        let value1 = checkpoint_db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value1, Some(Slice::from("value1")));

        let value2 = checkpoint_db
            .get(&ReadOptions::default(), &Slice::from("key2"))
            .unwrap();
        assert_eq!(value2, Some(Slice::from("value2")));
    }

    #[test]
    fn test_checkpoint_with_flush() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let checkpoint_path = temp_dir.path().join("checkpoint");

        // Create database with small write buffer to trigger flush
        let options = DBOptions {
            write_buffer_size: 1024, // 1KB
            ..Default::default()
        };

        let db = DB::open(db_path.to_str().unwrap(), options).unwrap();

        // Write enough data to trigger flush
        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}_with_some_extra_data_to_fill_buffer");
            db.put(
                &WriteOptions::default(),
                Slice::from(key),
                Slice::from(value),
            )
            .unwrap();
        }

        // Create checkpoint
        Checkpoint::create(&db, &checkpoint_path).unwrap();

        // Open checkpoint
        let checkpoint_db =
            DB::open(checkpoint_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Verify all data exists
        for i in 0..100 {
            let key = format!("key{i:04}");
            let expected_value = format!("value{i:04}_with_some_extra_data_to_fill_buffer");
            let value = checkpoint_db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value, Some(Slice::from(expected_value.as_str())));
        }
    }

    #[test]
    fn test_checkpoint_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let checkpoint_path = temp_dir.path().join("checkpoint");

        let db = DB::open(db_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Write initial data
        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();

        // Create checkpoint
        Checkpoint::create(&db, &checkpoint_path).unwrap();

        // Write more data to original DB
        db.put(
            &WriteOptions::default(),
            Slice::from("key2"),
            Slice::from("value2"),
        )
        .unwrap();

        // Open checkpoint
        let checkpoint_db =
            DB::open(checkpoint_path.to_str().unwrap(), DBOptions::default()).unwrap();

        // Checkpoint should have key1 but not key2
        let value1 = checkpoint_db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value1, Some(Slice::from("value1")));

        let value2 = checkpoint_db
            .get(&ReadOptions::default(), &Slice::from("key2"))
            .unwrap();
        assert_eq!(value2, None);

        // Original DB should have both
        let value1 = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value1, Some(Slice::from("value1")));

        let value2 = db
            .get(&ReadOptions::default(), &Slice::from("key2"))
            .unwrap();
        assert_eq!(value2, Some(Slice::from("value2")));
    }
}
