use crate::util::{Result, Slice, Status};

/// Maximum number of levels in LSM-Tree
pub const NUM_LEVELS: usize = 7;

/// Metadata for a single SSTable file
#[derive(Debug, Clone)]
pub struct FileMetaData {
    /// File number (used in filename: {number}.sst)
    pub number: u64,
    /// File size in bytes
    pub file_size: u64,
    /// Smallest key in this file
    pub smallest: Slice,
    /// Largest key in this file
    pub largest: Slice,
}

impl FileMetaData {
    pub fn new(number: u64, file_size: u64, smallest: Slice, largest: Slice) -> Self {
        FileMetaData {
            number,
            file_size,
            smallest,
            largest,
        }
    }
}

/// A VersionEdit represents the changes between two versions
/// It records which files were added and deleted at each level
#[derive(Debug, Default)]
pub struct VersionEdit {
    /// Comparator name
    pub comparator: Option<String>,
    /// Log file number
    pub log_number: Option<u64>,
    /// Previous log file number
    pub prev_log_number: Option<u64>,
    /// Next file number to use
    pub next_file_number: Option<u64>,
    /// Last sequence number
    pub last_sequence: Option<u64>,
    /// Files to delete: (level, file_number)
    pub deleted_files: Vec<(usize, u64)>,
    /// Files to add: (level, file_metadata)
    pub new_files: Vec<(usize, FileMetaData)>,
}

impl VersionEdit {
    pub fn new() -> Self {
        VersionEdit::default()
    }

    pub fn set_comparator(&mut self, name: String) {
        self.comparator = Some(name);
    }

    pub fn set_log_number(&mut self, num: u64) {
        self.log_number = Some(num);
    }

    pub fn set_prev_log_number(&mut self, num: u64) {
        self.prev_log_number = Some(num);
    }

    pub fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }

    pub fn set_last_sequence(&mut self, seq: u64) {
        self.last_sequence = Some(seq);
    }

    pub fn add_file(&mut self, level: usize, file: FileMetaData) {
        self.new_files.push((level, file));
    }

    pub fn delete_file(&mut self, level: usize, file_number: u64) {
        self.deleted_files.push((level, file_number));
    }

    /// Encode VersionEdit to bytes for MANIFEST file
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Tag: 1=comparator
        if let Some(ref cmp) = self.comparator {
            buf.push(1);
            let bytes = cmp.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }

        // Tag: 2=log_number
        if let Some(num) = self.log_number {
            buf.push(2);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        // Tag: 3=next_file_number
        if let Some(num) = self.next_file_number {
            buf.push(3);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        // Tag: 4=last_sequence
        if let Some(seq) = self.last_sequence {
            buf.push(4);
            buf.extend_from_slice(&seq.to_le_bytes());
        }

        // Tag: 5=deleted_file
        for (level, file_num) in &self.deleted_files {
            buf.push(5);
            buf.push(*level as u8);
            buf.extend_from_slice(&file_num.to_le_bytes());
        }

        // Tag: 6=new_file
        for (level, file) in &self.new_files {
            buf.push(6);
            buf.push(*level as u8);
            buf.extend_from_slice(&file.number.to_le_bytes());
            buf.extend_from_slice(&file.file_size.to_le_bytes());

            let smallest_data = file.smallest.data();
            buf.extend_from_slice(&(smallest_data.len() as u32).to_le_bytes());
            buf.extend_from_slice(smallest_data);

            let largest_data = file.largest.data();
            buf.extend_from_slice(&(largest_data.len() as u32).to_le_bytes());
            buf.extend_from_slice(largest_data);
        }

        buf
    }

    /// Decode VersionEdit from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut edit = VersionEdit::new();
        let mut pos = 0;

        while pos < data.len() {
            if pos >= data.len() {
                break;
            }

            let tag = data[pos];
            pos += 1;

            match tag {
                1 => {
                    // Comparator
                    if pos + 4 > data.len() {
                        return Err(Status::corruption("Invalid comparator length"));
                    }
                    let len = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + len > data.len() {
                        return Err(Status::corruption("Comparator data truncated"));
                    }
                    let cmp = String::from_utf8(data[pos..pos + len].to_vec())
                        .map_err(|_| Status::corruption("Invalid UTF-8 in comparator"))?;
                    edit.set_comparator(cmp);
                    pos += len;
                },
                2 => {
                    // Log number
                    if pos + 8 > data.len() {
                        return Err(Status::corruption("Invalid log number"));
                    }
                    let num = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    edit.set_log_number(num);
                    pos += 8;
                },
                3 => {
                    // Next file number
                    if pos + 8 > data.len() {
                        return Err(Status::corruption("Invalid next file number"));
                    }
                    let num = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    edit.set_next_file_number(num);
                    pos += 8;
                },
                4 => {
                    // Last sequence
                    if pos + 8 > data.len() {
                        return Err(Status::corruption("Invalid last sequence"));
                    }
                    let seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    edit.set_last_sequence(seq);
                    pos += 8;
                },
                5 => {
                    // Deleted file
                    if pos + 9 > data.len() {
                        return Err(Status::corruption("Invalid deleted file entry"));
                    }
                    let level = data[pos] as usize;
                    pos += 1;
                    let file_num = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    edit.delete_file(level, file_num);
                },
                6 => {
                    // New file
                    if pos + 1 > data.len() {
                        return Err(Status::corruption("Invalid new file entry"));
                    }
                    let level = data[pos] as usize;
                    pos += 1;

                    if pos + 16 > data.len() {
                        return Err(Status::corruption("Invalid file metadata"));
                    }
                    let number = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let file_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;

                    // Smallest key
                    if pos + 4 > data.len() {
                        return Err(Status::corruption("Invalid smallest key length"));
                    }
                    let smallest_len = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + smallest_len > data.len() {
                        return Err(Status::corruption("Smallest key data truncated"));
                    }
                    let smallest = Slice::from(&data[pos..pos + smallest_len]);
                    pos += smallest_len;

                    // Largest key
                    if pos + 4 > data.len() {
                        return Err(Status::corruption("Invalid largest key length"));
                    }
                    let largest_len = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + largest_len > data.len() {
                        return Err(Status::corruption("Largest key data truncated"));
                    }
                    let largest = Slice::from(&data[pos..pos + largest_len]);
                    pos += largest_len;

                    let file = FileMetaData::new(number, file_size, smallest, largest);
                    edit.add_file(level, file);
                },
                _ => {
                    return Err(Status::corruption(format!(
                        "Unknown tag in VersionEdit: {tag}"
                    )));
                },
            }
        }

        Ok(edit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_edit_encode_decode() {
        let mut edit = VersionEdit::new();
        edit.set_comparator("bytewise".to_string());
        edit.set_log_number(10);
        edit.set_next_file_number(100);
        edit.set_last_sequence(1000);
        edit.add_file(
            0,
            FileMetaData::new(1, 4096, Slice::from("key1"), Slice::from("key9")),
        );
        edit.delete_file(1, 5);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.comparator, Some("bytewise".to_string()));
        assert_eq!(decoded.log_number, Some(10));
        assert_eq!(decoded.next_file_number, Some(100));
        assert_eq!(decoded.last_sequence, Some(1000));
        assert_eq!(decoded.new_files.len(), 1);
        assert_eq!(decoded.new_files[0].0, 0);
        assert_eq!(decoded.new_files[0].1.number, 1);
        assert_eq!(decoded.deleted_files.len(), 1);
        assert_eq!(decoded.deleted_files[0], (1, 5));
    }

    #[test]
    fn test_file_metadata() {
        let file = FileMetaData::new(42, 8192, Slice::from("aaa"), Slice::from("zzz"));
        assert_eq!(file.number, 42);
        assert_eq!(file.file_size, 8192);
        assert_eq!(file.smallest, Slice::from("aaa"));
        assert_eq!(file.largest, Slice::from("zzz"));
    }
}
