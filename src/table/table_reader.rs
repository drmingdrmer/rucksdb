use crate::table::block::Block;
use crate::table::format::{BlockHandle, Footer, FOOTER_SIZE};
use crate::util::{Result, Slice, Status};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Table reader for reading SSTable files
pub struct TableReader {
    file: File,
    file_size: u64,
    index_block: Block,
    footer: Footer,
}

impl TableReader {
    /// Open an SSTable file for reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)
            .map_err(|e| Status::io_error(format!("Failed to open table file: {}", e)))?;

        // Get file size
        let file_size = file
            .seek(SeekFrom::End(0))
            .map_err(|e| Status::io_error(format!("Failed to seek to end: {}", e)))?;

        if file_size < FOOTER_SIZE as u64 {
            return Err(Status::corruption("File too small to be a valid SSTable"));
        }

        // Read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(|e| Status::io_error(format!("Failed to seek to footer: {}", e)))?;

        let mut footer_data = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_data)
            .map_err(|e| Status::io_error(format!("Failed to read footer: {}", e)))?;

        let footer = Footer::decode(&footer_data)
            .ok_or_else(|| Status::corruption("Invalid footer"))?;

        // Read index block
        let index_block_data = Self::read_block(&mut file, &footer.index_handle)?;
        let index_block = Block::new(index_block_data)?;

        Ok(TableReader {
            file,
            file_size,
            index_block,
            footer,
        })
    }

    /// Read a block from file
    fn read_block(file: &mut File, handle: &BlockHandle) -> Result<Vec<u8>> {
        file.seek(SeekFrom::Start(handle.offset))
            .map_err(|e| Status::io_error(format!("Failed to seek to block: {}", e)))?;

        let mut data = vec![0u8; handle.size as usize];
        file.read_exact(&mut data)
            .map_err(|e| Status::io_error(format!("Failed to read block: {}", e)))?;

        Ok(data)
    }

    /// Get a value by key
    pub fn get(&mut self, key: &Slice) -> Result<Option<Slice>> {
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

                // Read data block
                let block_data = Self::read_block(&mut self.file, &handle)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::table_builder::TableBuilder;
    use tempfile::NamedTempFile;

    fn build_test_table(entries: &[(&str, &str)]) -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for (key, value) in entries {
            builder.add(&Slice::from(*key), &Slice::from(*value)).unwrap();
        }

        builder.finish().unwrap();
        temp_file
    }

    #[test]
    fn test_table_reader_open() {
        let temp_file = build_test_table(&[("key1", "value1")]);
        let reader = TableReader::open(temp_file.path());
        assert!(reader.is_ok());
    }

    #[test]
    fn test_table_reader_get_single() {
        let temp_file = build_test_table(&[("key1", "value1")]);
        let mut reader = TableReader::open(temp_file.path()).unwrap();

        let value = reader.get(&Slice::from("key1")).unwrap();
        assert_eq!(value, Some(Slice::from("value1")));
    }

    #[test]
    fn test_table_reader_get_multiple() {
        let entries = vec![
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ];
        let temp_file = build_test_table(&entries);
        let mut reader = TableReader::open(temp_file.path()).unwrap();

        for (key, value) in entries {
            let result = reader.get(&Slice::from(key)).unwrap();
            assert_eq!(result, Some(Slice::from(value)));
        }
    }

    #[test]
    fn test_table_reader_get_not_found() {
        let temp_file = build_test_table(&[("key1", "value1")]);
        let mut reader = TableReader::open(temp_file.path()).unwrap();

        let value = reader.get(&Slice::from("nonexistent")).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_table_reader_many_keys() {
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push((format!("key{:04}", i), format!("value{:04}", i)));
        }

        let temp_file = {
            let temp_file = NamedTempFile::new().unwrap();
            let mut builder = TableBuilder::new(temp_file.path()).unwrap();

            for (key, value) in &entries {
                builder.add(&Slice::from(key.as_str()), &Slice::from(value.as_str())).unwrap();
            }

            builder.finish().unwrap();
            temp_file
        };

        let mut reader = TableReader::open(temp_file.path()).unwrap();

        for (key, value) in entries {
            let result = reader.get(&Slice::from(key.as_str())).unwrap();
            assert_eq!(result, Some(Slice::from(value.as_str())));
        }
    }
}
