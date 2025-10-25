use std::{
    fs::{self, File},
    io::Write as _,
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

use crate::{DB, DBOptions, Result};

/// Metadata for a single backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub backup_id: u64,
    pub timestamp: u64,
    pub db_path: String,
    pub sst_files: Vec<String>,
    pub wal_files: Vec<String>,
    pub manifest_file: String,
}

/// BackupEngine manages database backups
pub struct BackupEngine {
    backup_dir: PathBuf,
    next_backup_id: u64,
}

impl BackupEngine {
    /// Open or create a backup engine at the specified directory
    pub fn open<P: AsRef<Path>>(backup_dir: P) -> Result<Self> {
        let backup_dir = backup_dir.as_ref().to_path_buf();

        // Create backup directory if it doesn't exist
        if !backup_dir.exists() {
            fs::create_dir_all(&backup_dir)?;
        }

        // Find the next backup ID by scanning existing backups
        let mut max_id = 0u64;
        if let Ok(entries) = fs::read_dir(&backup_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(id_str) = name.strip_prefix("backup_")
                    && let Ok(id) = id_str.parse::<u64>()
                {
                    max_id = max_id.max(id);
                }
            }
        }

        Ok(BackupEngine {
            backup_dir,
            next_backup_id: max_id + 1,
        })
    }

    /// Create a new backup of the database
    pub fn create_backup(&mut self, db: &DB) -> Result<u64> {
        let backup_id = self.next_backup_id;
        self.next_backup_id += 1;

        let backup_path = self.backup_dir.join(format!("backup_{}", backup_id));
        fs::create_dir_all(&backup_path)?;

        // Create subdirectories
        let sst_dir = backup_path.join("sst");
        let wal_dir = backup_path.join("wal");
        let manifest_dir = backup_path.join("manifest");
        fs::create_dir_all(&sst_dir)?;
        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&manifest_dir)?;

        let db_path = db.get_db_path();
        let db_path_str = db_path.to_str().unwrap_or("");

        // Collect file lists
        let mut sst_files = Vec::new();
        let mut wal_files = Vec::new();
        let mut manifest_file = String::new();

        // Copy SST files
        if let Ok(entries) = fs::read_dir(&db_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.ends_with(".sst") {
                        // Use hard link if possible, otherwise copy
                        let dest = sst_dir.join(filename);
                        if fs::hard_link(&path, &dest).is_err() {
                            fs::copy(&path, &dest)?;
                        }
                        sst_files.push(filename.to_string());
                    } else if filename.ends_with(".log") {
                        // Copy WAL files
                        let dest = wal_dir.join(filename);
                        if fs::hard_link(&path, &dest).is_err() {
                            fs::copy(&path, &dest)?;
                        }
                        wal_files.push(filename.to_string());
                    } else if filename.starts_with("MANIFEST-") {
                        // Copy MANIFEST file
                        let dest = manifest_dir.join(filename);
                        if fs::hard_link(&path, &dest).is_err() {
                            fs::copy(&path, &dest)?;
                        }
                        manifest_file = filename.to_string();
                    }
                }
            }
        }

        // Create metadata
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metadata = BackupMetadata {
            backup_id,
            timestamp,
            db_path: db_path_str.to_string(),
            sst_files,
            wal_files,
            manifest_file,
        };

        // Write metadata to JSON file
        let meta_path = backup_path.join("meta.json");
        let meta_json = serde_json::to_string_pretty(&metadata)?;
        let mut file = File::create(meta_path)?;
        file.write_all(meta_json.as_bytes())?;
        file.sync_all()?;

        Ok(backup_id)
    }

    /// Restore a backup to a target directory
    pub fn restore_backup<P: AsRef<Path>>(&self, backup_id: u64, target_dir: P) -> Result<()> {
        let backup_path = self.backup_dir.join(format!("backup_{}", backup_id));
        if !backup_path.exists() {
            return Err(crate::util::Status::not_found(format!(
                "Backup {} not found",
                backup_id
            )));
        }

        // Read metadata
        let meta_path = backup_path.join("meta.json");
        let meta_content = fs::read_to_string(meta_path)?;
        let metadata: BackupMetadata = serde_json::from_str(&meta_content)?;

        let target_dir = target_dir.as_ref();
        fs::create_dir_all(target_dir)?;

        // Restore SST files
        let sst_dir = backup_path.join("sst");
        for filename in &metadata.sst_files {
            let src = sst_dir.join(filename);
            let dest = target_dir.join(filename);
            if fs::hard_link(&src, &dest).is_err() {
                fs::copy(&src, &dest)?;
            }
        }

        // Restore WAL files
        let wal_dir = backup_path.join("wal");
        for filename in &metadata.wal_files {
            let src = wal_dir.join(filename);
            let dest = target_dir.join(filename);
            if fs::hard_link(&src, &dest).is_err() {
                fs::copy(&src, &dest)?;
            }
        }

        // Restore MANIFEST file
        if !metadata.manifest_file.is_empty() {
            let manifest_dir = backup_path.join("manifest");
            let src = manifest_dir.join(&metadata.manifest_file);
            let dest = target_dir.join(&metadata.manifest_file);
            if fs::hard_link(&src, &dest).is_err() {
                fs::copy(&src, &dest)?;
            }
        }

        Ok(())
    }

    /// Get information about a specific backup
    pub fn get_backup_info(&self, backup_id: u64) -> Result<BackupMetadata> {
        let backup_path = self.backup_dir.join(format!("backup_{}", backup_id));
        if !backup_path.exists() {
            return Err(crate::util::Status::not_found(format!(
                "Backup {} not found",
                backup_id
            )));
        }

        let meta_path = backup_path.join("meta.json");
        let meta_content = fs::read_to_string(meta_path)?;
        let metadata: BackupMetadata = serde_json::from_str(&meta_content)?;
        Ok(metadata)
    }

    /// List all available backups
    pub fn list_backups(&self) -> Result<Vec<BackupMetadata>> {
        let mut backups = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.backup_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(id_str) = name.strip_prefix("backup_")
                    && let Ok(id) = id_str.parse::<u64>()
                    && let Ok(info) = self.get_backup_info(id)
                {
                    backups.push(info);
                }
            }
        }

        backups.sort_by_key(|b| b.backup_id);
        Ok(backups)
    }

    /// Delete a specific backup
    pub fn delete_backup(&self, backup_id: u64) -> Result<()> {
        let backup_path = self.backup_dir.join(format!("backup_{}", backup_id));
        if !backup_path.exists() {
            return Err(crate::util::Status::not_found(format!(
                "Backup {} not found",
                backup_id
            )));
        }

        fs::remove_dir_all(backup_path)?;
        Ok(())
    }
}

impl DB {
    /// Get the database path
    pub(crate) fn get_db_path(&self) -> PathBuf {
        self.db_path().clone()
    }

    /// Create a backup using the provided backup engine
    pub fn backup(&self, backup_engine: &mut BackupEngine) -> Result<u64> {
        backup_engine.create_backup(self)
    }

    /// Restore from a backup and open the database
    pub fn restore_and_open<P: AsRef<Path>>(
        backup_engine: &BackupEngine,
        backup_id: u64,
        restore_path: P,
        options: DBOptions,
    ) -> Result<DB> {
        backup_engine.restore_backup(backup_id, &restore_path)?;
        DB::open(restore_path.as_ref().to_str().unwrap(), options)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{ReadOptions, Slice, WriteOptions};

    #[test]
    fn test_backup_and_restore() {
        let db_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let restore_dir = TempDir::new().unwrap();

        // Create and populate database
        let db = DB::open(db_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();
        for i in 0..10 {
            db.put(
                &WriteOptions::default(),
                Slice::from(format!("key{}", i)),
                Slice::from(format!("value{}", i)),
            )
            .unwrap();
        }

        // Create backup
        let mut backup_engine = BackupEngine::open(backup_dir.path().to_str().unwrap()).unwrap();
        let backup_id = db.backup(&mut backup_engine).unwrap();
        assert_eq!(backup_id, 1);

        // Verify backup metadata
        let info = backup_engine.get_backup_info(backup_id).unwrap();
        assert_eq!(info.backup_id, 1);
        assert!(!info.sst_files.is_empty() || !info.wal_files.is_empty());

        // Restore backup
        let restored_db = DB::restore_and_open(
            &backup_engine,
            backup_id,
            restore_dir.path(),
            DBOptions::default(),
        )
        .unwrap();

        // Verify restored data
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected = format!("value{}", i);
            let value = restored_db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(value.as_ref().map(|v| v.to_string()), Some(expected));
        }
    }

    #[test]
    fn test_multiple_backups() {
        let db_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        let db = DB::open(db_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();
        let mut backup_engine = BackupEngine::open(backup_dir.path().to_str().unwrap()).unwrap();

        // Create first backup
        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();
        let backup1 = db.backup(&mut backup_engine).unwrap();
        assert_eq!(backup1, 1);

        // Create second backup
        db.put(
            &WriteOptions::default(),
            Slice::from("key2"),
            Slice::from("value2"),
        )
        .unwrap();
        let backup2 = db.backup(&mut backup_engine).unwrap();
        assert_eq!(backup2, 2);

        // List backups
        let backups = backup_engine.list_backups().unwrap();
        assert_eq!(backups.len(), 2);
        assert_eq!(backups[0].backup_id, 1);
        assert_eq!(backups[1].backup_id, 2);
    }

    #[test]
    fn test_delete_backup() {
        let db_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        let db = DB::open(db_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();
        let mut backup_engine = BackupEngine::open(backup_dir.path().to_str().unwrap()).unwrap();

        let backup_id = db.backup(&mut backup_engine).unwrap();
        assert!(backup_engine.get_backup_info(backup_id).is_ok());

        backup_engine.delete_backup(backup_id).unwrap();
        assert!(backup_engine.get_backup_info(backup_id).is_err());
    }
}
