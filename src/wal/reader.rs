use crate::util::{Result, Status};
use crate::wal::log_format::{
    calculate_checksum, decode_header, RecordType, BLOCK_SIZE, HEADER_SIZE,
};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// WAL Reader
pub struct Reader {
    file: File,
    /// Current position in file
    offset: usize,
    /// Buffer for reading
    buffer: Vec<u8>,
    /// Whether to report corruption
    report_corruption: bool,
}

impl Reader {
    /// Create a new WAL reader
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| Status::io_error(format!("Failed to open WAL file: {e}")))?;

        Ok(Reader {
            file,
            offset: 0,
            buffer: Vec::new(),
            report_corruption: true,
        })
    }

    /// Read the next record
    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        self.buffer.clear();
        let mut in_fragmented_record = false;

        loop {
            let (record_type, fragment) = match self.read_physical_record()? {
                Some(r) => r,
                None => {
                    if in_fragmented_record {
                        return Err(Status::corruption("Incomplete record at end of file"));
                    }
                    return Ok(None);
                }
            };

            match record_type {
                RecordType::Full => {
                    if in_fragmented_record {
                        return Err(Status::corruption(
                            "Unexpected Full record in fragmented record",
                        ));
                    }
                    return Ok(Some(fragment));
                }
                RecordType::First => {
                    if in_fragmented_record {
                        return Err(Status::corruption(
                            "Unexpected First record in fragmented record",
                        ));
                    }
                    self.buffer = fragment;
                    in_fragmented_record = true;
                }
                RecordType::Middle => {
                    if !in_fragmented_record {
                        return Err(Status::corruption("Unexpected Middle record without First"));
                    }
                    self.buffer.extend_from_slice(&fragment);
                }
                RecordType::Last => {
                    if !in_fragmented_record {
                        return Err(Status::corruption("Unexpected Last record without First"));
                    }
                    self.buffer.extend_from_slice(&fragment);
                    let result = self.buffer.clone();
                    self.buffer.clear();
                    return Ok(Some(result));
                }
            }
        }
    }

    /// Read a physical record
    fn read_physical_record(&mut self) -> Result<Option<(RecordType, Vec<u8>)>> {
        loop {
            let block_offset = self.offset % BLOCK_SIZE;

            // Skip to next block if we're too close to the end
            if BLOCK_SIZE - block_offset < HEADER_SIZE {
                let skip = BLOCK_SIZE - block_offset;
                self.offset += skip;
                self.file
                    .seek(SeekFrom::Start(self.offset as u64))
                    .map_err(|e| Status::io_error(format!("Seek failed: {e}")))?;
                continue;
            }

            // Read header
            let mut header_buf = [0u8; HEADER_SIZE];
            match self.file.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(None);
                }
                Err(e) => {
                    return Err(Status::io_error(format!("Read header failed: {e}")));
                }
            }

            let (checksum, length, record_type) = decode_header(&header_buf)
                .ok_or_else(|| Status::corruption("Invalid record header"))?;

            // Read data
            let mut data = vec![0u8; length as usize];
            self.file
                .read_exact(&mut data)
                .map_err(|e| Status::io_error(format!("Read data failed: {e}")))?;

            self.offset += HEADER_SIZE + length as usize;

            // Verify checksum
            let expected_checksum = calculate_checksum(record_type, &data);
            if checksum != expected_checksum {
                if self.report_corruption {
                    return Err(Status::corruption(format!(
                        "Checksum mismatch: expected {expected_checksum}, got {checksum}"
                    )));
                }
                continue;
            }

            return Ok(Some((record_type, data)));
        }
    }

    /// Seek to a specific offset
    pub fn seek(&mut self, offset: usize) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|e| Status::io_error(format!("Seek failed: {e}")))?;
        self.offset = offset;
        self.buffer.clear();
        Ok(())
    }

    /// Get current offset
    pub fn offset(&self) -> usize {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::Writer;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_write_round_trip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write records
        {
            let mut writer = Writer::new(path).unwrap();
            writer.add_record(b"record1").unwrap();
            writer.add_record(b"record2").unwrap();
            writer.add_record(b"record3").unwrap();
            writer.sync().unwrap();
        }

        // Read records
        {
            let mut reader = Reader::new(path).unwrap();

            let r1 = reader.read_record().unwrap().unwrap();
            assert_eq!(r1, b"record1");

            let r2 = reader.read_record().unwrap().unwrap();
            assert_eq!(r2, b"record2");

            let r3 = reader.read_record().unwrap().unwrap();
            assert_eq!(r3, b"record3");

            assert!(reader.read_record().unwrap().is_none());
        }
    }

    #[test]
    fn test_read_large_record() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let large_data = vec![b'X'; BLOCK_SIZE * 2];

        // Write
        {
            let mut writer = Writer::new(path).unwrap();
            writer.add_record(&large_data).unwrap();
            writer.sync().unwrap();
        }

        // Read
        {
            let mut reader = Reader::new(path).unwrap();
            let data = reader.read_record().unwrap().unwrap();
            assert_eq!(data, large_data);
        }
    }

    #[test]
    fn test_read_multiple_records() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write
        {
            let mut writer = Writer::new(path).unwrap();
            for i in 0..100 {
                let data = format!("record_{i}");
                writer.add_record(data.as_bytes()).unwrap();
            }
            writer.sync().unwrap();
        }

        // Read
        {
            let mut reader = Reader::new(path).unwrap();
            for i in 0..100 {
                let expected = format!("record_{i}");
                let data = reader.read_record().unwrap().unwrap();
                assert_eq!(data, expected.as_bytes());
            }
            assert!(reader.read_record().unwrap().is_none());
        }
    }
}
