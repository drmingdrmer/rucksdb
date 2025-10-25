use std::{fs, path::Path};

use crate::{Result, Slice};

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
}
