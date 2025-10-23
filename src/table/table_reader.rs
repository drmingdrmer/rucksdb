use crate::cache::LRUCache;
use crate::filter::FilterPolicy;
use crate::table::block::Block;
use crate::table::format::{BlockHandle, Footer, FOOTER_SIZE};
use crate::util::{Result, Slice, Status};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

/// Table reader for reading SSTable files
pub struct TableReader {
    file: File,
    file_number: u64,
    file_size: u64,
    index_block: Block,
    _footer: Footer,
    block_cache: Option<LRUCache<(u64, u64), Vec<u8>>>,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
    filter_data: Option<Vec<u8>>, // Filter block data
}

impl TableReader {
    /// Open an SSTable file for reading
    pub fn open<P: AsRef<Path>>(
        path: P,
        file_number: u64,
        block_cache: Option<LRUCache<(u64, u64), Vec<u8>>>,
    ) -> Result<Self> {
        Self::open_with_filter(path, file_number, block_cache, None)
    }

    /// Open an SSTable file for reading with optional filter policy
    pub fn open_with_filter<P: AsRef<Path>>(
        path: P,
        file_number: u64,
        block_cache: Option<LRUCache<(u64, u64), Vec<u8>>>,
        filter_policy: Option<Arc<dyn FilterPolicy>>,
    ) -> Result<Self> {
        let mut file = File::open(path)
            .map_err(|e| Status::io_error(format!("Failed to open table file: {e}")))?;

        // Get file size
        let file_size = file
            .seek(SeekFrom::End(0))
            .map_err(|e| Status::io_error(format!("Failed to seek to end: {e}")))?;

        if file_size < FOOTER_SIZE as u64 {
            return Err(Status::corruption("File too small to be a valid SSTable"));
        }

        // Read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(|e| Status::io_error(format!("Failed to seek to footer: {e}")))?;

        let mut footer_data = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_data)
            .map_err(|e| Status::io_error(format!("Failed to read footer: {e}")))?;

        let footer =
            Footer::decode(&footer_data).ok_or_else(|| Status::corruption("Invalid footer"))?;

        // Read filter block if present
        let filter_data = if filter_policy.is_some() && footer.meta_index_handle.size > 0 {
            let filter_block_data =
                Self::read_block_uncached(&mut file, &footer.meta_index_handle)?;
            Some(filter_block_data)
        } else {
            None
        };

        // Read index block (not cached, as it's small and accessed once)
        let index_block_data = Self::read_block_uncached(&mut file, &footer.index_handle)?;
        let index_block = Block::new(index_block_data)?;

        Ok(TableReader {
            file,
            file_number,
            file_size,
            index_block,
            _footer: footer,
            block_cache,
            filter_policy,
            filter_data,
        })
    }

    /// Read a block from file without caching (for index blocks)
    fn read_block_uncached(file: &mut File, handle: &BlockHandle) -> Result<Vec<u8>> {
        file.seek(SeekFrom::Start(handle.offset))
            .map_err(|e| Status::io_error(format!("Failed to seek to block: {e}")))?;

        let mut data = vec![0u8; handle.size as usize];
        file.read_exact(&mut data)
            .map_err(|e| Status::io_error(format!("Failed to read block: {e}")))?;

        Ok(data)
    }

    /// Read a block with caching support
    fn read_block(&mut self, handle: &BlockHandle) -> Result<Vec<u8>> {
        let cache_key = (self.file_number, handle.offset);

        // Check cache first
        if let Some(ref cache) = self.block_cache {
            if let Some(cached_data) = cache.get(&cache_key) {
                return Ok(cached_data);
            }
        }

        // Not in cache, read from file
        let data = Self::read_block_uncached(&mut self.file, handle)?;

        // Insert into cache
        if let Some(ref cache) = self.block_cache {
            cache.insert(cache_key, data.clone());
        }

        Ok(data)
    }

    /// Get a value by key
    pub fn get(&mut self, key: &Slice) -> Result<Option<Slice>> {
        // Check filter first to avoid unnecessary disk I/O
        if let (Some(ref policy), Some(ref filter_data)) = (&self.filter_policy, &self.filter_data)
        {
            if !policy.may_contain(filter_data, key.data()) {
                // Filter says key definitely doesn't exist
                return Ok(None);
            }
            // Filter says key might exist, continue with search
        }

        // Search index block for the data block containing the key
        let mut iter = self.index_block.iter();
        iter.seek_to_first()?;

        loop {
            let index_key = iter.key();

            // If index_key >= key, this data block might contain our key
            if index_key.data() >= key.data() {
                // Decode block handle
                let handle_data = iter.value();
                let handle = BlockHandle::decode(handle_data.data())
                    .ok_or_else(|| Status::corruption("Invalid block handle in index"))?;

                // Read data block (with caching)
                let block_data = self.read_block(&handle)?;
                let data_block = Block::new(block_data)?;

                // Search in data block
                return self.search_data_block(&data_block, key);
            }

            if !iter.next()? {
                break;
            }
        }

        Ok(None)
    }

    /// Search for key in a data block
    fn search_data_block(&self, block: &Block, key: &Slice) -> Result<Option<Slice>> {
        let mut iter = block.iter();
        iter.seek_to_first()?;

        loop {
            let current_key = iter.key();

            if current_key == *key {
                return Ok(Some(iter.value()));
            }

            if current_key.data() > key.data() {
                // Key not found
                return Ok(None);
            }

            if !iter.next()? {
                break;
            }
        }

        Ok(None)
    }

    /// Get file size
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Scan all entries in the table (for compaction)
    pub fn scan_all(&mut self) -> Result<Vec<(Slice, Slice)>> {
        let mut all_entries = Vec::new();

        // First, collect all block handles from the index
        let mut handles = Vec::new();
        {
            let mut index_iter = self.index_block.iter();
            if !index_iter.seek_to_first()? {
                return Ok(all_entries);
            }

            loop {
                let handle_data = index_iter.value();
                let handle = BlockHandle::decode(handle_data.data())
                    .ok_or_else(|| Status::corruption("Invalid block handle in index"))?;
                handles.push(handle);

                if !index_iter.next()? {
                    break;
                }
            }
        }

        // Now read data blocks using the collected handles
        for handle in handles {
            let block_data = self.read_block(&handle)?;
            let data_block = Block::new(block_data)?;

            // Read all entries from data block
            let mut data_iter = data_block.iter();
            if data_iter.seek_to_first()? {
                loop {
                    all_entries.push((data_iter.key(), data_iter.value()));
                    if !data_iter.next()? {
                        break;
                    }
                }
            }
        }

        Ok(all_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::format::CompressionType;
    use crate::table::table_builder::TableBuilder;
    use tempfile::NamedTempFile;

    fn build_test_table(entries: &[(&str, &str)]) -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for (key, value) in entries {
            builder
                .add(&Slice::from(*key), &Slice::from(*value))
                .unwrap();
        }

        builder.finish(CompressionType::None).unwrap();
        temp_file
    }

    #[test]
    fn test_table_reader_open() {
        let temp_file = build_test_table(&[("key1", "value1")]);
        let reader = TableReader::open(temp_file.path(), 1, None);
        assert!(reader.is_ok());
    }

    #[test]
    fn test_table_reader_get_single() {
        let temp_file = build_test_table(&[("key1", "value1")]);
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        let value = reader.get(&Slice::from("key1")).unwrap();
        assert_eq!(value, Some(Slice::from("value1")));
    }

    #[test]
    fn test_table_reader_get_multiple() {
        let entries = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
        let temp_file = build_test_table(&entries);
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        for (key, value) in entries {
            let result = reader.get(&Slice::from(key)).unwrap();
            assert_eq!(result, Some(Slice::from(value)));
        }
    }

    #[test]
    fn test_table_reader_get_not_found() {
        let temp_file = build_test_table(&[("key1", "value1")]);
        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        let value = reader.get(&Slice::from("nonexistent")).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_table_reader_many_keys() {
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push((format!("key{i:04}"), format!("value{i:04}")));
        }

        let temp_file = {
            let temp_file = NamedTempFile::new().unwrap();
            let mut builder = TableBuilder::new(temp_file.path()).unwrap();

            for (key, value) in &entries {
                builder
                    .add(&Slice::from(key.as_str()), &Slice::from(value.as_str()))
                    .unwrap();
            }

            builder.finish(CompressionType::None).unwrap();
            temp_file
        };

        let mut reader = TableReader::open(temp_file.path(), 1, None).unwrap();

        for (key, value) in entries {
            let result = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(result, Some(Slice::from(value.as_str())));
        }
    }
}
