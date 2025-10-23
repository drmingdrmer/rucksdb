use std::sync::{Arc, Mutex};

use crate::{
    iterator::Iterator,
    table::{block::Block, format::BlockHandle, table_reader::TableReader},
    util::{Result, Slice, Status},
};

/// Iterator for SSTable
///
/// Navigates across data blocks using the index block, maintaining a
/// BlockIterator for the current data block.
///
/// # Architecture
///
/// ```text
/// TableIterator
///     ├─→ Index Block (list of BlockHandles)
///     └─→ Current Data Block + BlockIterator
///         └─→ When exhausted, load next block
/// ```
///
/// # Implementation Notes
///
/// - Holds Arc<Mutex<TableReader>> for shared access to file I/O
/// - Caches list of BlockHandles from index on first seek
/// - Owns current data block to avoid lifetime issues
/// - Creates new BlockIterator when moving between blocks
pub struct TableIterator {
    reader: Arc<Mutex<TableReader>>,
    block_handles: Vec<BlockHandle>,
    current_block_index: Option<usize>,
    current_block: Option<Block>,
    current_block_iter_key: Option<Slice>,
    current_block_iter_value: Option<Slice>,
    current_block_iter_offset: usize,
    valid: bool,
}

impl TableIterator {
    pub fn new(reader: Arc<Mutex<TableReader>>) -> Result<Self> {
        // Load all block handles from index
        let handles = {
            let mut reader_guard = reader.lock().unwrap();
            Self::load_block_handles(&mut reader_guard)?
        };

        Ok(TableIterator {
            reader,
            block_handles: handles,
            current_block_index: None,
            current_block: None,
            current_block_iter_key: None,
            current_block_iter_value: None,
            current_block_iter_offset: 0,
            valid: false,
        })
    }

    /// Load all BlockHandles from the index block
    fn load_block_handles(reader: &mut TableReader) -> Result<Vec<BlockHandle>> {
        let mut handles = Vec::new();
        let index_block = reader.index_block();

        let mut index_iter = index_block.iter();
        if !index_iter.seek_to_first()? {
            return Ok(handles);
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

        Ok(handles)
    }

    /// Load a specific data block by index
    fn load_block(&mut self, block_index: usize) -> Result<()> {
        if block_index >= self.block_handles.len() {
            return Err(Status::invalid_argument("Block index out of range"));
        }

        let handle = self.block_handles[block_index];
        let block_data = {
            let mut reader = self.reader.lock().unwrap();
            reader.read_block_for_iter(&handle)?
        };

        let block = Block::new(block_data)?;
        self.current_block = Some(block);
        self.current_block_index = Some(block_index);
        Ok(())
    }

    /// Position the internal BlockIterator at the first entry of current block
    fn position_at_block_start(&mut self) -> Result<bool> {
        if let Some(ref block) = self.current_block {
            let mut iter = block.iter();
            if iter.seek_to_first()? {
                self.current_block_iter_key = Some(iter.key());
                self.current_block_iter_value = Some(iter.value());
                self.current_block_iter_offset = 0;
                self.valid = true;
                return Ok(true);
            }
        }
        self.valid = false;
        Ok(false)
    }

    /// Advance to next entry within current block or move to next block
    fn advance_forward(&mut self) -> Result<bool> {
        if let Some(ref block) = self.current_block {
            // Try to advance within current block
            let mut iter = block.iter();
            // Reposition iterator to current position
            if !iter.seek_to_first()? {
                self.valid = false;
                return Ok(false);
            }

            // Skip to current offset
            for _ in 0..=self.current_block_iter_offset {
                if !iter.next()? {
                    // Reached end of current block, try next block
                    if let Some(current_idx) = self.current_block_index
                        && current_idx + 1 < self.block_handles.len()
                    {
                        self.load_block(current_idx + 1)?;
                        return self.position_at_block_start();
                    }
                    self.valid = false;
                    return Ok(false);
                }
            }

            self.current_block_iter_key = Some(iter.key());
            self.current_block_iter_value = Some(iter.value());
            self.current_block_iter_offset += 1;
            self.valid = true;
            return Ok(true);
        }

        self.valid = false;
        Ok(false)
    }
}

impl Iterator for TableIterator {
    fn seek_to_first(&mut self) -> Result<bool> {
        if self.block_handles.is_empty() {
            self.valid = false;
            return Ok(false);
        }

        // Load first data block
        self.load_block(0)?;
        self.position_at_block_start()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        if self.block_handles.is_empty() {
            self.valid = false;
            return Ok(false);
        }

        // Load last data block
        let last_index = self.block_handles.len() - 1;
        self.load_block(last_index)?;

        // Position at last entry of the block
        if let Some(ref block) = self.current_block {
            let mut iter = block.iter();
            if !iter.seek_to_first()? {
                self.valid = false;
                return Ok(false);
            }

            // Advance to last entry
            let mut offset = 0;
            loop {
                let has_next = iter.next()?;
                if !has_next {
                    break;
                }
                offset += 1;
            }

            // Now reposition to last entry
            let mut iter = block.iter();
            iter.seek_to_first()?;
            for _ in 0..offset {
                iter.next()?;
            }

            self.current_block_iter_key = Some(iter.key());
            self.current_block_iter_value = Some(iter.value());
            self.current_block_iter_offset = offset;
            self.valid = true;
            return Ok(true);
        }

        self.valid = false;
        Ok(false)
    }

    fn seek(&mut self, target: &Slice) -> Result<bool> {
        // Binary search through blocks using index block keys
        // For now, linear search (can optimize later)
        for idx in 0..self.block_handles.len() {
            self.load_block(idx)?;

            if let Some(ref block) = self.current_block {
                let mut iter = block.iter();
                if !iter.seek_to_first()? {
                    continue;
                }

                // Check if target might be in this block
                // The index key is >= all keys in the block
                let first_key = iter.key();
                if target < &first_key {
                    // Target would be before this block, not found
                    self.valid = false;
                    return Ok(false);
                }

                // Search within this block
                let mut offset = 0;
                loop {
                    let current_key = iter.key();
                    if &current_key >= target {
                        self.current_block_iter_key = Some(current_key);
                        self.current_block_iter_value = Some(iter.value());
                        self.current_block_iter_offset = offset;
                        self.valid = true;
                        return Ok(true);
                    }

                    if !iter.next()? {
                        break;
                    }
                    offset += 1;
                }
            }
        }

        // Not found in any block
        self.valid = false;
        Ok(false)
    }

    fn seek_for_prev(&mut self, target: &Slice) -> Result<bool> {
        // Find last key <= target
        let mut last_valid: Option<(Slice, Slice, usize, usize)> = None;

        for block_idx in 0..self.block_handles.len() {
            self.load_block(block_idx)?;

            if let Some(ref block) = self.current_block {
                let mut iter = block.iter();
                if !iter.seek_to_first()? {
                    continue;
                }

                let mut offset = 0;
                loop {
                    let current_key = iter.key();
                    if &current_key <= target {
                        last_valid = Some((current_key, iter.value(), block_idx, offset));
                    } else {
                        break;
                    }

                    if !iter.next()? {
                        break;
                    }
                    offset += 1;
                }
            }
        }

        if let Some((key, value, block_idx, offset)) = last_valid {
            self.load_block(block_idx)?;
            self.current_block_iter_key = Some(key);
            self.current_block_iter_value = Some(value);
            self.current_block_iter_offset = offset;
            self.valid = true;
            Ok(true)
        } else {
            self.valid = false;
            Ok(false)
        }
    }

    fn next(&mut self) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }
        self.advance_forward()
    }

    fn prev(&mut self) -> Result<bool> {
        // Backward iteration is expensive - need to restart from beginning
        unimplemented!("TableIterator::prev() not yet implemented - use seek_for_prev() instead")
    }

    fn key(&self) -> Slice {
        self.current_block_iter_key
            .clone()
            .unwrap_or_else(Slice::empty)
    }

    fn value(&self) -> Slice {
        self.current_block_iter_value
            .clone()
            .unwrap_or_else(Slice::empty)
    }

    fn valid(&self) -> bool {
        self.valid
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::NamedTempFile;

    use super::*;
    use crate::table::{format::CompressionType, table_builder::TableBuilder};

    fn build_test_table(entries: &[(&str, &str)]) -> (NamedTempFile, Arc<Mutex<TableReader>>) {
        let temp_file = NamedTempFile::new().unwrap();
        let mut builder = TableBuilder::new(temp_file.path()).unwrap();

        for (key, value) in entries {
            builder
                .add(&Slice::from(*key), &Slice::from(*value))
                .unwrap();
        }

        builder.finish(CompressionType::None).unwrap();

        let reader = TableReader::open(temp_file.path(), 1, None).unwrap();
        (temp_file, Arc::new(Mutex::new(reader)))
    }

    #[test]
    fn test_table_iterator_seek_to_first() {
        let entries = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
        let (_temp, reader) = build_test_table(&entries);

        let mut iter = TableIterator::new(reader).unwrap();
        assert!(iter.seek_to_first().unwrap());
        assert!(iter.valid());
        assert_eq!(iter.key(), Slice::from("key1"));
        assert_eq!(iter.value(), Slice::from("value1"));
    }

    #[test]
    fn test_table_iterator_next() {
        let entries = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
        let (_temp, reader) = build_test_table(&entries);

        let mut iter = TableIterator::new(reader).unwrap();
        assert!(iter.seek_to_first().unwrap());

        let mut collected = Vec::new();
        loop {
            collected.push((iter.key(), iter.value()));
            if !iter.next().unwrap() {
                break;
            }
        }

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], (Slice::from("key1"), Slice::from("value1")));
        assert_eq!(collected[1], (Slice::from("key2"), Slice::from("value2")));
        assert_eq!(collected[2], (Slice::from("key3"), Slice::from("value3")));
    }

    #[test]
    fn test_table_iterator_seek() {
        let entries = vec![
            ("key1", "value1"),
            ("key3", "value3"),
            ("key5", "value5"),
            ("key7", "value7"),
        ];
        let (_temp, reader) = build_test_table(&entries);

        let mut iter = TableIterator::new(reader).unwrap();

        // Seek to existing key
        assert!(iter.seek(&Slice::from("key3")).unwrap());
        assert_eq!(iter.key(), Slice::from("key3"));

        // Seek to non-existing key (should find next)
        assert!(iter.seek(&Slice::from("key4")).unwrap());
        assert_eq!(iter.key(), Slice::from("key5"));
    }

    #[test]
    fn test_table_iterator_seek_to_last() {
        let entries = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
        let (_temp, reader) = build_test_table(&entries);

        let mut iter = TableIterator::new(reader).unwrap();
        assert!(iter.seek_to_last().unwrap());
        assert!(iter.valid());
        assert_eq!(iter.key(), Slice::from("key3"));
        assert_eq!(iter.value(), Slice::from("value3"));
    }

    #[test]
    fn test_table_iterator_empty() {
        let entries: Vec<(&str, &str)> = vec![];
        let (_temp, reader) = build_test_table(&entries);

        let mut iter = TableIterator::new(reader).unwrap();
        assert!(!iter.seek_to_first().unwrap());
        assert!(!iter.valid());
    }
}
