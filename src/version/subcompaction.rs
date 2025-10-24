use crate::{util::Slice, version::version_edit::FileMetaData};

/// A key range for subcompaction
#[derive(Debug, Clone)]
pub struct KeyRange {
    pub smallest: Slice,
    pub largest: Slice,
}

impl KeyRange {
    pub fn new(smallest: Slice, largest: Slice) -> Self {
        KeyRange { smallest, largest }
    }

    /// Check if a key falls within this range
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.smallest.data() && key <= self.largest.data()
    }

    /// Check if this range overlaps with another range
    pub fn overlaps(&self, other: &KeyRange) -> bool {
        !(self.largest.data() < other.smallest.data()
            || other.largest.data() < self.smallest.data())
    }
}

/// Configuration for subcompaction
#[derive(Debug, Clone)]
pub struct SubcompactionConfig {
    /// Minimum file size to trigger subcompaction (default: 10 MB)
    pub min_file_size: u64,
    /// Target number of subcompactions (default: 4)
    pub target_subcompactions: usize,
    /// Enable parallel execution (default: true)
    pub enable_parallel: bool,
}

impl SubcompactionConfig {
    pub fn new() -> Self {
        SubcompactionConfig {
            min_file_size: 10 * 1024 * 1024, // 10 MB
            target_subcompactions: 4,
            enable_parallel: true,
        }
    }

    /// Check if subcompaction should be used for given total size
    pub fn should_use_subcompaction(&self, total_size: u64) -> bool {
        self.enable_parallel && total_size >= self.min_file_size
    }
}

impl Default for SubcompactionConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// A subcompaction represents a portion of a full compaction
#[derive(Debug)]
pub struct Subcompaction {
    /// The key range this subcompaction covers
    pub range: KeyRange,
    /// Files from the input level that overlap this range
    pub level_files: Vec<FileMetaData>,
    /// Files from the next level that overlap this range
    pub next_level_files: Vec<FileMetaData>,
}

impl Subcompaction {
    pub fn new(
        range: KeyRange,
        level_files: Vec<FileMetaData>,
        next_level_files: Vec<FileMetaData>,
    ) -> Self {
        Subcompaction {
            range,
            level_files,
            next_level_files,
        }
    }

    /// Calculate total input size for this subcompaction
    pub fn input_size(&self) -> u64 {
        self.level_files.iter().map(|f| f.file_size).sum::<u64>()
            + self
                .next_level_files
                .iter()
                .map(|f| f.file_size)
                .sum::<u64>()
    }
}

/// SubcompactionPlanner splits a compaction into parallel subcompactions
pub struct SubcompactionPlanner {
    config: SubcompactionConfig,
}

impl SubcompactionPlanner {
    pub fn new(config: SubcompactionConfig) -> Self {
        SubcompactionPlanner { config }
    }

    /// Plan subcompactions for a given compaction
    /// Returns None if subcompaction is not beneficial
    pub fn plan(
        &self,
        level_files: &[FileMetaData],
        next_level_files: &[FileMetaData],
    ) -> Option<Vec<Subcompaction>> {
        if level_files.is_empty() {
            return None;
        }

        // Calculate total input size
        let total_size: u64 = level_files.iter().map(|f| f.file_size).sum::<u64>()
            + next_level_files.iter().map(|f| f.file_size).sum::<u64>();

        // Check if subcompaction is beneficial
        if !self.config.should_use_subcompaction(total_size) {
            return None;
        }

        // Split key space into subranges
        let ranges = self.split_key_ranges(level_files, next_level_files);
        if ranges.len() <= 1 {
            return None; // Not worth parallelizing
        }

        // Create subcompactions
        let subcompactions = ranges
            .into_iter()
            .map(|range| {
                let level_overlaps = self.get_overlapping_files(level_files, &range);
                let next_overlaps = self.get_overlapping_files(next_level_files, &range);
                Subcompaction::new(range, level_overlaps, next_overlaps)
            })
            .filter(|sub| !sub.level_files.is_empty() || !sub.next_level_files.is_empty())
            .collect();

        Some(subcompactions)
    }

    /// Split key space into target number of ranges
    fn split_key_ranges(
        &self,
        level_files: &[FileMetaData],
        next_level_files: &[FileMetaData],
    ) -> Vec<KeyRange> {
        // Collect all boundary keys
        let mut boundaries: Vec<Vec<u8>> = Vec::new();

        for file in level_files {
            boundaries.push(file.smallest.data().to_vec());
            boundaries.push(file.largest.data().to_vec());
        }
        for file in next_level_files {
            boundaries.push(file.smallest.data().to_vec());
            boundaries.push(file.largest.data().to_vec());
        }

        // Sort and deduplicate
        boundaries.sort();
        boundaries.dedup();

        if boundaries.len() < 2 {
            return vec![];
        }

        // Create ranges
        let step = boundaries.len().max(2) / self.config.target_subcompactions.max(1);
        let mut ranges = Vec::new();

        let mut i = 0;
        while i < boundaries.len() - 1 {
            let next_i = (i + step).min(boundaries.len() - 1);
            ranges.push(KeyRange::new(
                Slice::from(boundaries[i].as_slice()),
                Slice::from(boundaries[next_i].as_slice()),
            ));
            i = next_i;
            if i == boundaries.len() - 1 {
                break;
            }
        }

        ranges
    }

    /// Get files that overlap with a key range
    fn get_overlapping_files(&self, files: &[FileMetaData], range: &KeyRange) -> Vec<FileMetaData> {
        files
            .iter()
            .filter(|f| {
                !(f.largest.data() < range.smallest.data()
                    || f.smallest.data() > range.largest.data())
            })
            .cloned()
            .collect()
    }
}

impl Default for SubcompactionPlanner {
    fn default() -> Self {
        Self::new(SubcompactionConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_range_contains() {
        let range = KeyRange::new(Slice::from("a"), Slice::from("z"));

        assert!(range.contains(b"a"));
        assert!(range.contains(b"m"));
        assert!(range.contains(b"z"));
        assert!(!range.contains(b"0")); // Before range
        assert!(!range.contains(b"zz")); // After range (but close)
    }

    #[test]
    fn test_key_range_overlaps() {
        let range1 = KeyRange::new(Slice::from("a"), Slice::from("m"));
        let range2 = KeyRange::new(Slice::from("k"), Slice::from("z"));
        let range3 = KeyRange::new(Slice::from("n"), Slice::from("z"));

        assert!(range1.overlaps(&range2)); // k-m overlaps
        assert!(range2.overlaps(&range1)); // Symmetric
        assert!(!range1.overlaps(&range3)); // No overlap
    }

    #[test]
    fn test_subcompaction_config() {
        let config = SubcompactionConfig::new();

        // Small file should not trigger subcompaction
        assert!(!config.should_use_subcompaction(5 * 1024 * 1024)); // 5 MB

        // Large file should trigger subcompaction
        assert!(config.should_use_subcompaction(20 * 1024 * 1024)); // 20 MB
    }

    #[test]
    fn test_subcompaction_input_size() {
        let level_files = vec![
            FileMetaData::new(1, 1000, Slice::from("a"), Slice::from("b")),
            FileMetaData::new(2, 2000, Slice::from("c"), Slice::from("d")),
        ];
        let next_level_files = vec![FileMetaData::new(
            3,
            3000,
            Slice::from("a"),
            Slice::from("e"),
        )];

        let sub = Subcompaction::new(
            KeyRange::new(Slice::from("a"), Slice::from("z")),
            level_files,
            next_level_files,
        );

        assert_eq!(sub.input_size(), 6000);
    }

    #[test]
    fn test_planner_no_subcompaction_small_size() {
        let planner = SubcompactionPlanner::new(SubcompactionConfig::new());

        // Small files should not trigger subcompaction
        let level_files = vec![FileMetaData::new(
            1,
            1024,
            Slice::from("a"),
            Slice::from("z"),
        )];
        let next_level_files = vec![];

        let result = planner.plan(&level_files, &next_level_files);
        assert!(result.is_none());
    }

    #[test]
    fn test_planner_creates_subcompactions() {
        let mut config = SubcompactionConfig::new();
        config.min_file_size = 100; // Very low threshold for testing
        config.target_subcompactions = 2;
        let planner = SubcompactionPlanner::new(config);

        // Multiple files with different key ranges
        let level_files = vec![
            FileMetaData::new(1, 1000, Slice::from("a"), Slice::from("c")),
            FileMetaData::new(2, 1000, Slice::from("m"), Slice::from("p")),
        ];
        let next_level_files = vec![FileMetaData::new(
            3,
            1000,
            Slice::from("d"),
            Slice::from("z"),
        )];

        let result = planner.plan(&level_files, &next_level_files);
        assert!(result.is_some());

        let subcompactions = result.unwrap();
        assert!(!subcompactions.is_empty());

        // Each subcompaction should have some files
        for sub in &subcompactions {
            assert!(sub.input_size() > 0);
        }
    }

    #[test]
    fn test_split_key_ranges() {
        let mut config = SubcompactionConfig::new();
        config.target_subcompactions = 3;
        let planner = SubcompactionPlanner::new(config);

        let level_files = vec![
            FileMetaData::new(1, 1000, Slice::from("a"), Slice::from("b")),
            FileMetaData::new(2, 1000, Slice::from("c"), Slice::from("d")),
            FileMetaData::new(3, 1000, Slice::from("e"), Slice::from("f")),
        ];

        let ranges = planner.split_key_ranges(&level_files, &[]);
        assert!(!ranges.is_empty());

        // Ranges should be ordered
        for i in 1..ranges.len() {
            assert!(ranges[i - 1].largest.data() <= ranges[i].smallest.data());
        }
    }
}
