use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use rayon::prelude::*;

use crate::{
    filter::FilterPolicy,
    memtable::memtable::InternalKey,
    table::{format::CompressionType, table_builder::TableBuilder, table_reader::TableReader},
    util::{Result, Slice, Status},
    version::{
        subcompaction::{Subcompaction, SubcompactionConfig, SubcompactionPlanner},
        version_edit::FileMetaData,
    },
};

/// Result from executing a single subcompaction
#[derive(Debug)]
pub struct SubcompactionResult {
    pub file_meta: Option<FileMetaData>,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Configuration for parallel compaction execution
#[derive(Debug, Clone)]
pub struct ParallelCompactionConfig {
    pub max_threads: usize,
    pub subcompaction_config: SubcompactionConfig,
    pub enable_parallel: bool,
}

impl Default for ParallelCompactionConfig {
    fn default() -> Self {
        Self {
            max_threads: 4,
            subcompaction_config: SubcompactionConfig::new(),
            enable_parallel: true,
        }
    }
}

/// Parallel compaction executor
pub struct ParallelCompactionExecutor {
    config: ParallelCompactionConfig,
    db_path: PathBuf,
    compression: CompressionType,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
}

impl ParallelCompactionExecutor {
    pub fn new(
        config: ParallelCompactionConfig,
        db_path: PathBuf,
        compression: CompressionType,
        filter_policy: Option<Arc<dyn FilterPolicy>>,
    ) -> Self {
        Self {
            config,
            db_path,
            compression,
            filter_policy,
        }
    }

    /// Execute compaction with parallel subcompactions
    pub fn execute_compaction(
        &self,
        level: usize,
        level_files: Vec<FileMetaData>,
        next_level_files: Vec<FileMetaData>,
        next_file_number: &dyn Fn() -> u64,
    ) -> Result<Vec<SubcompactionResult>> {
        // If parallel execution is disabled or too few files, use sequential
        if !self.config.enable_parallel || level_files.len() + next_level_files.len() < 4 {
            return self.execute_sequential(level, level_files, next_level_files, next_file_number);
        }

        // Create subcompaction planner
        let planner = SubcompactionPlanner::new(self.config.subcompaction_config.clone());
        let subcompactions_opt = planner.plan(&level_files, &next_level_files);

        // If planner returns None or only one subcompaction, execute sequentially
        let subcompactions = match subcompactions_opt {
            Some(subs) if subs.len() > 1 => subs,
            _ => {
                return self.execute_sequential(
                    level,
                    level_files,
                    next_level_files,
                    next_file_number,
                );
            },
        };

        // Pre-allocate file numbers for all subcompactions
        let file_numbers: Vec<u64> = (0..subcompactions.len())
            .map(|_| next_file_number())
            .collect();

        // Execute subcompactions in parallel using rayon
        let results: Result<Vec<SubcompactionResult>> = subcompactions
            .par_iter()
            .zip(file_numbers.par_iter())
            .map(|(subcompaction, &file_number)| {
                self.execute_subcompaction(level, subcompaction, file_number)
            })
            .collect();

        results
    }

    /// Execute a single subcompaction
    fn execute_subcompaction(
        &self,
        level: usize,
        subcompaction: &Subcompaction,
        file_number: u64,
    ) -> Result<SubcompactionResult> {
        // Collect all entries from files in this subcompaction's range
        let mut all_entries: Vec<(Slice, Slice)> = Vec::new();
        let mut bytes_read = 0u64;

        // Read from level files
        for file in &subcompaction.level_files {
            bytes_read += file.file_size;
            let entries = self.read_file_in_range(file.number, &subcompaction.range)?;
            all_entries.extend(entries);
        }

        // Read from next level files
        for file in &subcompaction.next_level_files {
            bytes_read += file.file_size;
            let entries = self.read_file_in_range(file.number, &subcompaction.range)?;
            all_entries.extend(entries);
        }

        // If no entries in this range, return empty result
        if all_entries.is_empty() {
            return Ok(SubcompactionResult {
                file_meta: None,
                bytes_read,
                bytes_written: 0,
            });
        }

        // Sort and merge entries
        let merged = self.merge_entries(all_entries, level)?;

        // If all entries were deleted/filtered out
        if merged.is_empty() {
            return Ok(SubcompactionResult {
                file_meta: None,
                bytes_read,
                bytes_written: 0,
            });
        }

        // Write output SSTable
        let sst_path = self.db_path.join(format!("{file_number:06}.sst"));
        let bytes_written = self.write_sstable(&sst_path, &merged)?;

        // Extract user keys for file metadata
        let smallest_internal = InternalKey::decode(&merged.first().unwrap().0)?;
        let largest_internal = InternalKey::decode(&merged.last().unwrap().0)?;
        let smallest = smallest_internal.user_key().clone();
        let largest = largest_internal.user_key().clone();

        let file_meta = FileMetaData::new(file_number, bytes_written, smallest, largest);

        Ok(SubcompactionResult {
            file_meta: Some(file_meta),
            bytes_read,
            bytes_written,
        })
    }

    /// Read entries from a file within a specific key range
    fn read_file_in_range(
        &self,
        file_number: u64,
        range: &crate::version::subcompaction::KeyRange,
    ) -> Result<Vec<(Slice, Slice)>> {
        let sst_path = self.db_path.join(format!("{file_number:06}.sst"));
        let mut reader = TableReader::open(&sst_path, file_number, None)?;

        // Read all entries from the file
        let all_entries = reader.scan_all()?;

        // Filter entries by key range
        let filtered: Vec<(Slice, Slice)> = all_entries
            .into_iter()
            .filter(|(key, _)| {
                // Decode InternalKey to get user key
                if let Ok(internal_key) = InternalKey::decode(key) {
                    let user_key = internal_key.user_key();
                    range.contains(user_key.data())
                } else {
                    // If we can't decode, include it (shouldn't happen)
                    true
                }
            })
            .collect();

        Ok(filtered)
    }

    /// Merge and deduplicate entries
    fn merge_entries(
        &self,
        mut all_entries: Vec<(Slice, Slice)>,
        level: usize,
    ) -> Result<Vec<(Slice, Slice)>> {
        // Sort by InternalKey (user_key, sequence descending, type)
        all_entries.sort_by(|a, b| {
            let key_a = match InternalKey::decode(&a.0) {
                Ok(k) => k,
                Err(_) => return a.0.data().cmp(b.0.data()),
            };
            let key_b = match InternalKey::decode(&b.0) {
                Ok(k) => k,
                Err(_) => return a.0.data().cmp(b.0.data()),
            };

            // Compare user keys first
            match key_a.user_key().data().cmp(key_b.user_key().data()) {
                std::cmp::Ordering::Equal => {
                    // Same user key: sort by sequence descending (higher first)
                    match key_b.sequence().cmp(&key_a.sequence()) {
                        std::cmp::Ordering::Equal => key_a.value_type.cmp(&key_b.value_type),
                        other => other,
                    }
                },
                other => other,
            }
        });

        // Deduplicate: keep only the first (highest sequence) for each user key
        let mut merged: Vec<(Slice, Slice)> = Vec::new();
        let mut last_user_key: Option<Slice> = None;
        let is_bottom_level = level + 1 >= 6;

        for (key, value) in all_entries {
            if let Ok(internal_key) = InternalKey::decode(&key) {
                let user_key = internal_key.user_key().clone();

                // Skip duplicates (keep first = highest sequence)
                if let Some(ref last) = last_user_key
                    && last == &user_key
                {
                    continue;
                }

                // Drop deletion markers at bottom level
                if internal_key.is_deletion() && is_bottom_level {
                    last_user_key = Some(user_key);
                    continue;
                }

                // Keep this entry
                merged.push((key, value));
                last_user_key = Some(user_key);
            }
        }

        Ok(merged)
    }

    /// Write entries to SSTable
    fn write_sstable(&self, path: &Path, entries: &[(Slice, Slice)]) -> Result<u64> {
        let mut builder = TableBuilder::new_with_filter(path, self.filter_policy.clone())?;

        for (key, value) in entries {
            builder.add(key, value)?;
        }

        builder.finish(self.compression)?;

        // Get file size
        let file_size = std::fs::metadata(path)
            .map_err(|e| Status::io_error(format!("Failed to get file size: {e}")))?
            .len();

        Ok(file_size)
    }

    /// Execute compaction sequentially (fallback)
    fn execute_sequential(
        &self,
        level: usize,
        level_files: Vec<FileMetaData>,
        next_level_files: Vec<FileMetaData>,
        next_file_number: &dyn Fn() -> u64,
    ) -> Result<Vec<SubcompactionResult>> {
        let mut all_entries: Vec<(Slice, Slice)> = Vec::new();
        let mut bytes_read = 0u64;

        // Read all files
        for file in &level_files {
            bytes_read += file.file_size;
            let sst_path = self.db_path.join(format!("{:06}.sst", file.number));
            let mut reader = TableReader::open(&sst_path, file.number, None)?;
            all_entries.extend(reader.scan_all()?);
        }

        for file in &next_level_files {
            bytes_read += file.file_size;
            let sst_path = self.db_path.join(format!("{:06}.sst", file.number));
            let mut reader = TableReader::open(&sst_path, file.number, None)?;
            all_entries.extend(reader.scan_all()?);
        }

        // Merge entries
        let merged = self.merge_entries(all_entries, level)?;

        if merged.is_empty() {
            return Ok(vec![SubcompactionResult {
                file_meta: None,
                bytes_read,
                bytes_written: 0,
            }]);
        }

        // Write output file
        let file_number = next_file_number();
        let sst_path = self.db_path.join(format!("{file_number:06}.sst"));
        let bytes_written = self.write_sstable(&sst_path, &merged)?;

        // Extract user keys for file metadata
        let smallest_internal = InternalKey::decode(&merged.first().unwrap().0)?;
        let largest_internal = InternalKey::decode(&merged.last().unwrap().0)?;
        let smallest = smallest_internal.user_key().clone();
        let largest = largest_internal.user_key().clone();

        let file_meta = FileMetaData::new(file_number, bytes_written, smallest, largest);

        Ok(vec![SubcompactionResult {
            file_meta: Some(file_meta),
            bytes_read,
            bytes_written,
        }])
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::memtable::memtable::VALUE_TYPE_VALUE;

    fn create_test_file(
        db_path: &Path,
        file_number: u64,
        entries: &[(&str, &str, u64)], // (key, value, sequence)
    ) -> Result<FileMetaData> {
        let sst_path = db_path.join(format!("{file_number:06}.sst"));
        let mut builder = TableBuilder::new(&sst_path)?;

        let mut smallest = None;
        let mut largest = None;

        for (key, value, seq) in entries {
            let internal_key = InternalKey::new(Slice::from(*key), *seq, VALUE_TYPE_VALUE).encode();
            builder.add(&internal_key, &Slice::from(*value))?;

            if smallest.is_none() {
                smallest = Some(Slice::from(*key));
            }
            largest = Some(Slice::from(*key));
        }

        builder.finish(CompressionType::None)?;

        let file_size = std::fs::metadata(&sst_path)?.len();

        Ok(FileMetaData::new(
            file_number,
            file_size,
            smallest.unwrap(),
            largest.unwrap(),
        ))
    }

    #[test]
    fn test_sequential_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path();

        // Create test files
        let file1 =
            create_test_file(db_path, 1, &[("key1", "value1", 1), ("key2", "value2", 1)]).unwrap();

        let file2 = create_test_file(
            db_path,
            2,
            &[("key2", "value2_new", 2), ("key3", "value3", 2)],
        )
        .unwrap();

        let config = ParallelCompactionConfig {
            enable_parallel: false,
            ..Default::default()
        };

        let executor = ParallelCompactionExecutor::new(
            config,
            db_path.to_path_buf(),
            CompressionType::None,
            None,
        );

        use std::sync::atomic::{AtomicU64, Ordering};
        let next_file = AtomicU64::new(100);
        let results = executor
            .execute_compaction(0, vec![file1], vec![file2], &|| {
                next_file.fetch_add(1, Ordering::SeqCst) + 1
            })
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].file_meta.is_some());
    }
}
