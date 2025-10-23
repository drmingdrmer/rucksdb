use std::{fs::File, io::Write, path::Path, sync::Arc};

use crate::{
    filter::FilterPolicy,
    table::{
        block_builder::BlockBuilder,
        format::{BlockHandle, CompressionType, DEFAULT_BLOCK_SIZE, Footer},
    },
    util::{Result, Slice, Status},
};

/// Table builder for creating SSTable files
pub struct TableBuilder {
    file: File,
    offset: u64,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    last_key: Vec<u8>,
    num_entries: u64,
    block_size: usize,
    pending_index_entry: bool,
    pending_handle: BlockHandle,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
    filter_keys: Vec<Vec<u8>>, // Keys to build filter from
    compression_type: CompressionType,
}

impl TableBuilder {
    /// Create a new table builder without filter
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new_with_filter(path, None)
    }

    /// Create a new table builder with optional filter policy
    pub fn new_with_filter<P: AsRef<Path>>(
        path: P,
        filter_policy: Option<Arc<dyn FilterPolicy>>,
    ) -> Result<Self> {
        let file = File::create(path)
            .map_err(|e| Status::io_error(format!("Failed to create table file: {e}")))?;

        Ok(TableBuilder {
            file,
            offset: 0,
            data_block: BlockBuilder::default(),
            index_block: BlockBuilder::default(),
            last_key: Vec::new(),
            num_entries: 0,
            block_size: DEFAULT_BLOCK_SIZE,
            pending_index_entry: false,
            pending_handle: BlockHandle::new(0, 0),
            filter_policy,
            filter_keys: Vec::new(),
            compression_type: CompressionType::None, // Default, will be set in finish()
        })
    }

    /// Add a key-value pair to the table
    /// Keys must be added in sorted order
    pub fn add(&mut self, key: &Slice, value: &Slice) -> Result<()> {
        if !self.last_key.is_empty() && key.data() <= self.last_key.as_slice() {
            return Err(Status::invalid_argument(
                "Keys must be added in sorted order",
            ));
        }

        // If there's a pending index entry, add it now
        if self.pending_index_entry {
            let separator = self.find_shortest_separator(&self.last_key, key.data());
            let handle_encoded = self.pending_handle.encode();
            self.index_block
                .add(&Slice::from(separator), &Slice::from(handle_encoded));
            self.pending_index_entry = false;
        }

        // Collect key for filter if filter policy is set
        if self.filter_policy.is_some() {
            self.filter_keys.push(key.data().to_vec());
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key.data());
        self.num_entries += 1;
        self.data_block.add(key, value);

        // Flush block if it's large enough
        if self.data_block.current_size_estimate() >= self.block_size {
            self.flush_data_block()?;
        }

        Ok(())
    }

    /// Flush the current data block to file
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }

        let block_data = self
            .data_block
            .finish_with_compression(self.compression_type);
        let block_size = block_data.len();

        // Write block to file
        self.file
            .write_all(&block_data)
            .map_err(|e| Status::io_error(format!("Failed to write data block: {e}")))?;

        // Record block handle for index
        self.pending_handle = BlockHandle::new(self.offset, block_size as u64);
        self.pending_index_entry = true;

        self.offset += block_size as u64;
        self.data_block.reset();

        Ok(())
    }

    /// Finish building the table with specified compression
    pub fn finish(&mut self, compression: CompressionType) -> Result<()> {
        // Set compression type
        self.compression_type = compression;

        // Flush any pending data block
        self.flush_data_block()?;

        // Add final index entry if needed
        if self.pending_index_entry {
            let separator = self.find_short_successor(&self.last_key);
            let handle_encoded = self.pending_handle.encode();
            self.index_block
                .add(&Slice::from(separator), &Slice::from(handle_encoded));
            self.pending_index_entry = false;
        }

        // Write filter block if filter policy is set
        let filter_block_handle = if let Some(ref policy) = self.filter_policy {
            let filter_data = policy.create_filter(&self.filter_keys);
            let handle = BlockHandle::new(self.offset, filter_data.len() as u64);
            self.file
                .write_all(&filter_data)
                .map_err(|e| Status::io_error(format!("Failed to write filter block: {e}")))?;
            self.offset += filter_data.len() as u64;
            handle
        } else {
            // No filter, use empty handle
            BlockHandle::new(0, 0)
        };

        // Use filter_block_handle as meta_index_handle in footer
        let meta_index_handle = filter_block_handle;

        // Write index block
        let index_block_data = self.index_block.finish();
        let index_handle = BlockHandle::new(self.offset, index_block_data.len() as u64);
        self.file
            .write_all(&index_block_data)
            .map_err(|e| Status::io_error(format!("Failed to write index block: {e}")))?;
        self.offset += index_block_data.len() as u64;

        // Write footer
        let footer = Footer::new(meta_index_handle, index_handle);
        let footer_data = footer.encode();
        self.file
            .write_all(&footer_data)
            .map_err(|e| Status::io_error(format!("Failed to write footer: {e}")))?;

        // Sync file
        self.file
            .sync_all()
            .map_err(|e| Status::io_error(format!("Failed to sync file: {e}")))?;

        Ok(())
    }

    /// Find shortest separator between two keys
    fn find_shortest_separator(&self, start: &[u8], _limit: &[u8]) -> Vec<u8> {
        // Simple implementation: just use start key
        // A better implementation would find the shortest key > start and < limit
        start.to_vec()
    }

    /// Find short successor of a key
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        // Simple implementation: just use the key
        // A better implementation would find the shortest key > key
        key.to_vec()
    }

    /// Get number of entries added
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Get current file size
    pub fn file_size(&self) -> u64 {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_table_builder_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let builder = TableBuilder::new(temp_file.path());
        assert!(builder.is_ok());
    }

    #[test]
    fn test_table_builder_add_single() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        builder
            .add(&Slice::from("key1"), &Slice::from("value1"))
            .unwrap();
        builder.finish(CompressionType::None).unwrap();

        assert_eq!(builder.num_entries(), 1);
        assert!(builder.file_size() > 0);
    }

    #[test]
    fn test_table_builder_add_multiple() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish(CompressionType::None).unwrap();
        assert_eq!(builder.num_entries(), 100);
    }

    #[test]
    fn test_table_builder_sorted_keys() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        builder
            .add(&Slice::from("aaa"), &Slice::from("v1"))
            .unwrap();
        builder
            .add(&Slice::from("bbb"), &Slice::from("v2"))
            .unwrap();
        builder
            .add(&Slice::from("ccc"), &Slice::from("v3"))
            .unwrap();

        builder.finish(CompressionType::None).unwrap();
        assert_eq!(builder.num_entries(), 3);
    }

    #[test]
    fn test_table_builder_unsorted_keys() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        builder
            .add(&Slice::from("bbb"), &Slice::from("v1"))
            .unwrap();
        let result = builder.add(&Slice::from("aaa"), &Slice::from("v2"));

        assert!(result.is_err());
    }
}
