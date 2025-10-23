use crate::{
    util::Slice,
    version::version_edit::{FileMetaData, NUM_LEVELS},
};

/// A Version represents a snapshot of all SSTables organized by levels
///
/// Level 0: SSTables may have overlapping keys (from MemTable flush)
/// Level 1+: SSTables have non-overlapping keys
pub struct Version {
    /// Files at each level
    pub files: Vec<Vec<FileMetaData>>,
}

impl Version {
    pub fn new() -> Self {
        Version {
            files: vec![Vec::new(); NUM_LEVELS],
        }
    }

    /// Get total number of files across all levels
    pub fn num_files(&self) -> usize {
        self.files.iter().map(|level| level.len()).sum()
    }

    /// Get number of files at a specific level
    pub fn num_level_files(&self, level: usize) -> usize {
        if level < NUM_LEVELS {
            self.files[level].len()
        } else {
            0
        }
    }

    /// Get files at a specific level
    pub fn get_level_files(&self, level: usize) -> &[FileMetaData] {
        if level < NUM_LEVELS {
            &self.files[level]
        } else {
            &[]
        }
    }

    /// Add file to a level
    pub fn add_file(&mut self, level: usize, file: FileMetaData) {
        if level < NUM_LEVELS {
            self.files[level].push(file);
            // Sort files at level 1+ by smallest key
            if level > 0 {
                self.files[level].sort_by(|a, b| a.smallest.data().cmp(b.smallest.data()));
            }
        }
    }

    /// Remove file from a level
    pub fn remove_file(&mut self, level: usize, file_number: u64) {
        if level < NUM_LEVELS {
            self.files[level].retain(|f| f.number != file_number);
        }
    }

    /// Get overlapping files at level 0 for a key range
    /// Level 0 files can overlap, so we need to check all files
    pub fn get_overlapping_level0_files(
        &self,
        smallest: &Slice,
        largest: &Slice,
    ) -> Vec<FileMetaData> {
        let mut result = Vec::new();

        for file in &self.files[0] {
            if Self::key_range_overlaps(smallest, largest, &file.smallest, &file.largest) {
                result.push(file.clone());
            }
        }

        result
    }

    /// Get overlapping files at level 1+ for a key range
    /// Since files don't overlap at these levels, we can use binary search
    pub fn get_overlapping_files(
        &self,
        level: usize,
        smallest: &Slice,
        largest: &Slice,
    ) -> Vec<FileMetaData> {
        if level >= NUM_LEVELS {
            return Vec::new();
        }

        if level == 0 {
            return self.get_overlapping_level0_files(smallest, largest);
        }

        let mut result = Vec::new();

        // Binary search for the first file that might overlap
        let files = &self.files[level];
        let start_idx = files
            .iter()
            .position(|f| f.largest.data() >= smallest.data())
            .unwrap_or(files.len());

        // Add all files that overlap
        for file in files.iter().skip(start_idx) {
            if file.smallest.data() > largest.data() {
                break;
            }
            result.push(file.clone());
        }

        result
    }

    /// Check if two key ranges overlap
    fn key_range_overlaps(
        a_smallest: &Slice,
        a_largest: &Slice,
        b_smallest: &Slice,
        b_largest: &Slice,
    ) -> bool {
        // Ranges overlap if they're not disjoint
        // Disjoint means: a is entirely before b OR b is entirely before a
        !(a_largest.data() < b_smallest.data() || b_largest.data() < a_smallest.data())
    }

    /// Pick level for compaction based on level sizes
    pub fn pick_compaction_level(&self) -> Option<usize> {
        // Simple strategy: pick the first level that exceeds its size threshold
        // Level 0: 4 files
        // Level 1: 10 MB
        // Level 2+: 10x previous level

        const LEVEL0_COMPACTION_TRIGGER: usize = 4;
        const LEVEL1_SIZE_LIMIT: u64 = 10 * 1024 * 1024; // 10MB

        if self.files[0].len() >= LEVEL0_COMPACTION_TRIGGER {
            return Some(0);
        }

        let mut size_limit = LEVEL1_SIZE_LIMIT;
        for level in 1..NUM_LEVELS {
            let level_size: u64 = self.files[level].iter().map(|f| f.file_size).sum();
            if level_size > size_limit {
                return Some(level);
            }
            size_limit *= 10;
        }

        None
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_new() {
        let version = Version::new();
        assert_eq!(version.num_files(), 0);
        assert_eq!(version.files.len(), NUM_LEVELS);
    }

    #[test]
    fn test_version_add_file() {
        let mut version = Version::new();
        let file = FileMetaData::new(1, 1024, Slice::from("a"), Slice::from("z"));

        version.add_file(0, file);
        assert_eq!(version.num_files(), 1);
        assert_eq!(version.num_level_files(0), 1);
    }

    #[test]
    fn test_version_remove_file() {
        let mut version = Version::new();
        let file = FileMetaData::new(1, 1024, Slice::from("a"), Slice::from("z"));

        version.add_file(0, file);
        assert_eq!(version.num_files(), 1);

        version.remove_file(0, 1);
        assert_eq!(version.num_files(), 0);
    }

    #[test]
    fn test_overlapping_level0_files() {
        let mut version = Version::new();

        // Add overlapping files to level 0
        version.add_file(
            0,
            FileMetaData::new(1, 1024, Slice::from("a"), Slice::from("m")),
        );
        version.add_file(
            0,
            FileMetaData::new(2, 1024, Slice::from("k"), Slice::from("z")),
        );

        let overlapping =
            version.get_overlapping_level0_files(&Slice::from("j"), &Slice::from("p"));
        assert_eq!(overlapping.len(), 2);
    }

    #[test]
    fn test_pick_compaction_level() {
        let mut version = Version::new();

        // Add 4 files to level 0 to trigger compaction
        for i in 0..4 {
            version.add_file(
                0,
                FileMetaData::new(i, 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        let level = version.pick_compaction_level();
        assert_eq!(level, Some(0));
    }
}
