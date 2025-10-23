use crate::util::{Result, Status};
use crate::wal::log_format::{
    BLOCK_SIZE, HEADER_SIZE, RecordType, calculate_checksum, encode_header,
};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// WAL Writer
pub struct Writer {
    file: File,
    /// Current position in the file
    offset: usize,
    /// Current position in the current block
    block_offset: usize,
}

impl Writer {
    /// Create a new WAL writer
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| Status::io_error(format!("Failed to open WAL file: {e}")))?;

        Ok(Writer {
            file,
            offset: 0,
            block_offset: 0,
        })
    }

    /// Add a record to the log
    pub fn add_record(&mut self, data: &[u8]) -> Result<()> {
        let mut left = data.len();
        let mut ptr = 0;
        let mut begin = true;

        while left > 0 {
            let leftover = BLOCK_SIZE - self.block_offset;

            // Switch to a new block if needed
            if leftover < HEADER_SIZE {
                // Fill the rest of the block with zeros
                if leftover > 0 {
                    let padding = vec![0u8; leftover];
                    self.file
                        .write_all(&padding)
                        .map_err(|e| Status::io_error(format!("Write padding failed: {e}")))?;
                    self.offset += leftover;
                }
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_length = if left < avail { left } else { avail };

            let record_type = if begin && left == fragment_length {
                RecordType::Full
            } else if begin {
                RecordType::First
            } else if left == fragment_length {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            self.emit_physical_record(record_type, &data[ptr..ptr + fragment_length])?;

            ptr += fragment_length;
            left -= fragment_length;
            begin = false;
        }

        Ok(())
    }

    /// Emit a physical record
    fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        let length = data.len();
        if length > 0xFFFF {
            return Err(Status::invalid_argument("Record too large"));
        }

        let checksum = calculate_checksum(record_type, data);
        let header = encode_header(checksum, length as u16, record_type);

        // Write header
        self.file
            .write_all(&header)
            .map_err(|e| Status::io_error(format!("Write header failed: {e}")))?;

        // Write data
        self.file
            .write_all(data)
            .map_err(|e| Status::io_error(format!("Write data failed: {e}")))?;

        self.offset += HEADER_SIZE + length;
        self.block_offset += HEADER_SIZE + length;

        Ok(())
    }

    /// Sync the file to disk
    pub fn sync(&mut self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|e| Status::io_error(format!("Sync failed: {e}")))
    }

    /// Get current file offset
    pub fn offset(&self) -> usize {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_writer_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let writer = Writer::new(temp_file.path());
        assert!(writer.is_ok());
    }

    #[test]
    fn test_write_small_record() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut writer = Writer::new(temp_file.path()).unwrap();

        let data = b"hello world";
        writer.add_record(data).unwrap();

        assert_eq!(writer.offset(), HEADER_SIZE + data.len());
    }

    #[test]
    fn test_write_multiple_records() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut writer = Writer::new(temp_file.path()).unwrap();

        writer.add_record(b"record1").unwrap();
        writer.add_record(b"record2").unwrap();
        writer.add_record(b"record3").unwrap();

        let expected_size =
            3 * HEADER_SIZE + b"record1".len() + b"record2".len() + b"record3".len();
        assert_eq!(writer.offset(), expected_size);
    }

    #[test]
    fn test_write_large_record() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut writer = Writer::new(temp_file.path()).unwrap();

        // Create a record larger than one block
        let large_data = vec![b'A'; BLOCK_SIZE * 2];
        writer.add_record(&large_data).unwrap();

        assert!(writer.offset() > BLOCK_SIZE * 2);
    }

    #[test]
    fn test_sync() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut writer = Writer::new(temp_file.path()).unwrap();

        writer.add_record(b"test").unwrap();
        writer.sync().unwrap();
    }
}
