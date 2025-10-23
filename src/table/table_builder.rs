use crate::table::block_builder::BlockBuilder;
use crate::table::format::{BlockHandle, Footer, DEFAULT_BLOCK_SIZE, FOOTER_SIZE};
use crate::util::{Result, Slice, Status};
use std::fs::File;
use std::io::Write;
use std::path::Path;

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
}

impl TableBuilder {
    /// Create a new table builder
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)
            .map_err(|e| Status::io_error(format!("Failed to create table file: {}", e)))?;

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
        })
    }

    /// Add a key-value pair to the table
    /// Keys must be added in sorted order
    pub fn add(&mut self, key: &Slice, value: &Slice) -> Result<()> {
        if !self.last_key.is_empty() && key.data() <= self.last_key.as_slice() {
            return Err(Status::invalid_argument("Keys must be added in sorted order"));
        }

        // If there's a pending index entry, add it now
        if self.pending_index_entry {
            let separator = self.find_shortest_separator(&self.last_key, key.data());
            let handle_encoded = self.pending_handle.encode();
            self.index_block.add(&Slice::from(separator), &Slice::from(handle_encoded));
            self.pending_index_entry = false;
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

        let block_data = self.data_block.finish();
        let block_size = block_data.len();

        // Write block to file
        self.file
            .write_all(&block_data)
            .map_err(|e| Status::io_error(format!("Failed to write data block: {}", e)))?;

        // Record block handle for index
        self.pending_handle = BlockHandle::new(self.offset, block_size as u64);
        self.pending_index_entry = true;

        self.offset += block_size as u64;
        self.data_block.reset();

        Ok(())
    }

    /// Finish building the table
    pub fn finish(&mut self) -> Result<()> {
        // Flush any pending data block
        self.flush_data_block()?;

        // Add final index entry if needed
        if self.pending_index_entry {
            let separator = self.find_short_successor(&self.last_key);
            let handle_encoded = self.pending_handle.encode();
            self.index_block.add(&Slice::from(separator), &Slice::from(handle_encoded));
            self.pending_index_entry = false;
        }

        // Write meta index block (empty for now)
        let meta_index_block_data = BlockBuilder::default().finish();
        let meta_index_handle = BlockHandle::new(self.offset, meta_index_block_data.len() as u64);
        self.file
            .write_all(&meta_index_block_data)
            .map_err(|e| Status::io_error(format!("Failed to write meta index block: {}", e)))?;
        self.offset += meta_index_block_data.len() as u64;

        // Write index block
        let index_block_data = self.index_block.finish();
        let index_handle = BlockHandle::new(self.offset, index_block_data.len() as u64);
        self.file
            .write_all(&index_block_data)
            .map_err(|e| Status::io_error(format!("Failed to write index block: {}", e)))?;
        self.offset += index_block_data.len() as u64;

        // Write footer
        let footer = Footer::new(meta_index_handle, index_handle);
        let footer_data = footer.encode();
        self.file
            .write_all(&footer_data)
            .map_err(|e| Status::io_error(format!("Failed to write footer: {}", e)))?;

        // Sync file
        self.file
            .sync_all()
            .map_err(|e| Status::io_error(format!("Failed to sync file: {}", e)))?;

        Ok(())
    }

    /// Find shortest separator between two keys
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
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
    use super::*;
    use tempfile::NamedTempFile;

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

        builder.add(&Slice::from("key1"), &Slice::from("value1")).unwrap();
        builder.finish().unwrap();

        assert_eq!(builder.num_entries(), 1);
        assert!(builder.file_size() > 0);
    }

    #[test]
    fn test_table_builder_add_multiple() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            builder.add(&Slice::from(key), &Slice::from(value)).unwrap();
        }

        builder.finish().unwrap();
        assert_eq!(builder.num_entries(), 100);
    }

    #[test]
    fn test_table_builder_sorted_keys() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        builder.add(&Slice::from("aaa"), &Slice::from("v1")).unwrap();
        builder.add(&Slice::from("bbb"), &Slice::from("v2")).unwrap();
        builder.add(&Slice::from("ccc"), &Slice::from("v3")).unwrap();

        builder.finish().unwrap();
        assert_eq!(builder.num_entries(), 3);
    }

    #[test]
    fn test_table_builder_unsorted_keys() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        builder.add(&Slice::from("bbb"), &Slice::from("v1")).unwrap();
        let result = builder.add(&Slice::from("aaa"), &Slice::from("v2"));

        assert!(result.is_err());
    }
}
