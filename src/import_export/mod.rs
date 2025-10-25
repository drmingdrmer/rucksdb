use std::{fs, path::Path};

use crate::{
    DB, Result, Slice,
    column_family::ColumnFamilyHandle,
    version::{FileMetaData, VersionEdit},
};

/// Options for ingesting external SST files
#[derive(Debug, Clone)]
pub struct IngestExternalFileOptions {
    /// Move files instead of copying them
    pub move_files: bool,
    /// Verify file checksum before ingestion
    pub verify_checksums_before_ingest: bool,
}

impl Default for IngestExternalFileOptions {
    fn default() -> Self {
        IngestExternalFileOptions {
            move_files: false,
            verify_checksums_before_ingest: true,
        }
    }
}

/// Information about an external SST file
#[derive(Debug, Clone)]
pub struct ExternalFileInfo {
    pub file_size: u64,
    pub num_entries: u64,
    pub smallest_key: Slice,
    pub largest_key: Slice,
}

/// Validates an external SST file
///
/// Checks that the file exists, is readable, and contains valid SST data.
/// Returns file metadata if validation succeeds.
pub fn validate_external_file<P: AsRef<Path>>(
    path: P,
    _options: &IngestExternalFileOptions,
) -> Result<ExternalFileInfo> {
    let path = path.as_ref();

    // Check file exists
    if !path.exists() {
        return Err(crate::util::Status::not_found(format!(
            "File not found: {}",
            path.display()
        )));
    }

    // Get file size
    let metadata = fs::metadata(path)?;
    let file_size = metadata.len();

    // Open SST file to validate structure
    // Use dummy file_number (0) and no block cache for validation
    let mut table_reader = crate::table::TableReader::open(path, 0, None)?;

    // Scan all entries to get key range and count
    let entries = table_reader.scan_all()?;

    if entries.is_empty() {
        return Err(crate::util::Status::corruption("Empty SST file"));
    }

    let num_entries = entries.len() as u64;
    let smallest_key = entries.first().unwrap().0.clone();
    let largest_key = entries.last().unwrap().0.clone();

    Ok(ExternalFileInfo {
        file_size,
        num_entries,
        smallest_key,
        largest_key,
    })
}

/// Copies an SST file to a target location
///
/// Can either copy or move the file depending on the move_files option.
pub fn copy_external_file<P: AsRef<Path>, Q: AsRef<Path>>(
    source: P,
    target: Q,
    move_files: bool,
) -> Result<()> {
    let source = source.as_ref();
    let target = target.as_ref();

    // Create parent directories if needed
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }

    if move_files {
        fs::rename(source, target)?;
    } else {
        fs::copy(source, target)?;
    }

    Ok(())
}

impl DB {
    /// Ingests an external SST file into the default column family
    ///
    /// This function validates the external SST file, copies/moves it to the
    /// database directory, and adds it to level 0 of the LSM tree.
    ///
    /// # Arguments
    /// * `file_path` - Path to the external SST file
    /// * `options` - Options controlling the ingestion behavior
    ///
    /// # Returns
    /// * `Ok(())` if ingestion succeeds
    /// * `Err(Status)` if validation fails or file cannot be added
    pub fn ingest_external_file<P: AsRef<Path>>(
        &self,
        file_path: P,
        options: &IngestExternalFileOptions,
    ) -> Result<()> {
        let default_cf = self.column_families().default_cf();
        self.ingest_external_file_cf(default_cf.handle(), file_path, options)
    }

    /// Ingests an external SST file into a specific column family
    ///
    /// # Arguments
    /// * `cf_handle` - Handle to the target column family
    /// * `file_path` - Path to the external SST file
    /// * `options` - Options controlling the ingestion behavior
    pub fn ingest_external_file_cf<P: AsRef<Path>>(
        &self,
        cf_handle: &ColumnFamilyHandle,
        file_path: P,
        options: &IngestExternalFileOptions,
    ) -> Result<()> {
        let file_path = file_path.as_ref();

        // 1. Validate the external SST file
        let file_info = validate_external_file(file_path, options)?;

        // 2. Get column family data
        let cf_data = self
            .column_families()
            .get_cf(cf_handle)
            .ok_or_else(|| crate::util::Status::invalid_argument("Column family not found"))?;

        // 3. Allocate a new file number for this SST
        let file_number = {
            let version_set = cf_data.version_set();
            let vs = version_set.write();
            vs.new_file_number()
        };

        // 4. Copy/move the file to the DB directory with new file number
        let target_path = self.db_path().join(format!("{:06}.sst", file_number));
        copy_external_file(file_path, &target_path, options.move_files)?;

        // 5. Create file metadata
        let file_meta = FileMetaData {
            number: file_number,
            file_size: file_info.file_size,
            smallest: file_info.smallest_key,
            largest: file_info.largest_key,
        };

        // 6. Add file to LSM tree at level 0 via VersionEdit
        let mut edit = VersionEdit::default();
        edit.new_files.push((0, file_meta)); // Always add to level 0

        // 7. Apply the edit to the version set
        let version_set = cf_data.version_set();
        let vs = version_set.read();
        vs.log_and_apply(edit)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{NamedTempFile, TempDir};

    use super::*;
    use crate::table::{CompressionType, TableBuilder};

    #[test]
    fn test_validate_external_file() {
        let temp_file = NamedTempFile::with_suffix(".sst").unwrap();

        // Create an SST file directly using TableBuilder
        {
            let mut builder = TableBuilder::new(temp_file.path()).unwrap();
            for i in 0..100 {
                let key = format!("key{:03}", i);
                let value = format!("value{}", i);
                builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
            }
            builder.finish(CompressionType::None).unwrap();
        }

        // Validate the SST file
        let options = IngestExternalFileOptions::default();
        let info = validate_external_file(temp_file.path(), &options).unwrap();

        assert!(info.file_size > 0);
        assert_eq!(info.num_entries, 100);
        assert_eq!(info.smallest_key.data(), b"key000");
        assert_eq!(info.largest_key.data(), b"key099");
    }

    #[test]
    fn test_copy_external_file() {
        let source_file = NamedTempFile::with_suffix(".sst").unwrap();
        let target_dir = TempDir::new().unwrap();

        // Create a test SST file using TableBuilder
        {
            let mut builder = TableBuilder::new(source_file.path()).unwrap();
            for i in 0..50 {
                let key = format!("k{:02}", i);
                let value = format!("v{}", i);
                builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
            }
            builder.finish(CompressionType::None).unwrap();
        }

        let source_sst = source_file.path();
        let target_sst = target_dir.path().join("copied.sst");

        // Copy file
        copy_external_file(source_sst, &target_sst, false).unwrap();

        // Verify both files exist
        assert!(source_sst.exists());
        assert!(target_sst.exists());

        // Verify sizes match
        let source_size = fs::metadata(source_sst).unwrap().len();
        let target_size = fs::metadata(&target_sst).unwrap().len();
        assert_eq!(source_size, target_size);
    }

    #[test]
    fn test_move_external_file() {
        let source_dir = TempDir::new().unwrap();
        let target_dir = TempDir::new().unwrap();

        // Create a test SST file using TableBuilder
        let source_sst = source_dir.path().join("test.sst");
        {
            let mut builder = TableBuilder::new(&source_sst).unwrap();
            for i in 0..30 {
                let key = format!("key{:02}", i);
                let value = format!("val{}", i);
                builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
            }
            builder.finish(CompressionType::None).unwrap();
        }

        let original_size = fs::metadata(&source_sst).unwrap().len();

        // Move file
        let target_sst = target_dir.path().join("moved.sst");
        copy_external_file(&source_sst, &target_sst, true).unwrap();

        // Verify source no longer exists
        assert!(!source_sst.exists());
        // Verify target exists
        assert!(target_sst.exists());
        // Verify size matches
        assert_eq!(fs::metadata(&target_sst).unwrap().len(), original_size);
    }

    #[test]
    fn test_validate_nonexistent_file() {
        let options = IngestExternalFileOptions::default();
        let result = validate_external_file("/nonexistent/file.sst", &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_external_file() {
        use crate::{DB, DBOptions, ReadOptions, WriteOptions};

        let db_dir = TempDir::new().unwrap();
        let external_dir = TempDir::new().unwrap();

        // 1. Create an external SST file
        let external_sst = external_dir.path().join("external.sst");
        {
            let mut builder = TableBuilder::new(&external_sst).unwrap();
            for i in 100..120 {
                let key = format!("ext_key{:03}", i);
                let value = format!("ext_value{}", i);
                builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
            }
            builder.finish(CompressionType::None).unwrap();
        }

        // 2. Create a database and add some initial data
        let db = DB::open(db_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();
        for i in 0..10 {
            db.put(
                &WriteOptions::default(),
                Slice::from(format!("key{:02}", i)),
                Slice::from(format!("value{}", i)),
            )
            .unwrap();
        }

        // 3. Ingest the external SST file
        let options = IngestExternalFileOptions {
            move_files: false,
            verify_checksums_before_ingest: true,
        };
        db.ingest_external_file(&external_sst, &options).unwrap();

        // 4. Verify that data from both sources is readable
        // Check original data
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value.as_ref().map(|v| v.to_string()),
                Some(format!("value{}", i))
            );
        }

        // Check ingested data
        for i in 100..120 {
            let key = format!("ext_key{:03}", i);
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value.as_ref().map(|v| v.to_string()),
                Some(format!("ext_value{}", i))
            );
        }

        // 5. Verify external file still exists (copy mode)
        assert!(external_sst.exists());
    }

    #[test]
    fn test_ingest_external_file_with_move() {
        use crate::{DB, DBOptions, ReadOptions};

        let db_dir = TempDir::new().unwrap();
        let external_dir = TempDir::new().unwrap();

        // Create an external SST file
        let external_sst = external_dir.path().join("to_move.sst");
        {
            let mut builder = TableBuilder::new(&external_sst).unwrap();
            for i in 200..210 {
                let key = format!("move_key{:03}", i);
                let value = format!("move_value{}", i);
                builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
            }
            builder.finish(CompressionType::None).unwrap();
        }

        let db = DB::open(db_dir.path().to_str().unwrap(), DBOptions::default()).unwrap();

        // Ingest with move
        let options = IngestExternalFileOptions {
            move_files: true,
            verify_checksums_before_ingest: true,
        };
        db.ingest_external_file(&external_sst, &options).unwrap();

        // Verify data is readable
        for i in 200..210 {
            let key = format!("move_key{:03}", i);
            let value = db
                .get(&ReadOptions::default(), &Slice::from(key.as_str()))
                .unwrap();
            assert_eq!(
                value.as_ref().map(|v| v.to_string()),
                Some(format!("move_value{}", i))
            );
        }

        // Verify external file no longer exists (move mode)
        assert!(!external_sst.exists());
    }
}
