use crate::version::version::Version;

/// Compaction priority score for a level
#[derive(Debug, Clone)]
pub struct CompactionScore {
    pub level: usize,
    pub score: f64,
}

/// CompactionPicker selects which level to compact based on priority scores
pub struct CompactionPicker {
    /// Base level size (Level 1 target size)
    base_level_size: u64,
    /// Level size multiplier (default: 10x per level)
    level_multiplier: u64,
    /// Level 0 file count trigger
    level0_file_trigger: usize,
}

impl CompactionPicker {
    /// Create a new CompactionPicker with default settings
    pub fn new() -> Self {
        CompactionPicker {
            base_level_size: 10 * 1024 * 1024, // 10 MB
            level_multiplier: 10,
            level0_file_trigger: 4,
        }
    }

    /// Create with custom settings
    pub fn with_config(base_level_size: u64, level_multiplier: u64, level0_trigger: usize) -> Self {
        CompactionPicker {
            base_level_size,
            level_multiplier,
            level0_file_trigger: level0_trigger,
        }
    }

    /// Calculate target size for a level
    pub fn target_size_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            return 0; // Level 0 uses file count, not size
        }

        // Level 1: base_level_size
        // Level 2: base_level_size * multiplier
        // Level 3: base_level_size * multiplier^2
        let multiplier_pow = self.level_multiplier.pow((level - 1) as u32);
        self.base_level_size.saturating_mul(multiplier_pow)
    }

    /// Calculate compaction score for a level
    /// Score > 1.0 means level needs compaction
    /// Higher score = higher priority
    fn calculate_level_score(&self, version: &Version, level: usize) -> f64 {
        if level == 0 {
            // Level 0: score based on file count
            let file_count = version.num_level_files(level);
            file_count as f64 / self.level0_file_trigger as f64
        } else {
            // Other levels: score based on size ratio
            let level_size: u64 = version
                .get_level_files(level)
                .iter()
                .map(|f| f.file_size)
                .sum();

            let target_size = self.target_size_for_level(level);
            if target_size == 0 {
                return 0.0;
            }

            level_size as f64 / target_size as f64
        }
    }

    /// Pick level to compact based on priority scores
    /// Returns level with highest score > 1.0, or None if no compaction needed
    pub fn pick_compaction(&self, version: &Version) -> Option<usize> {
        let mut best_score = 1.0; // Only compact if score > 1.0
        let mut best_level = None;

        // Score all levels (0 through 6, skip last level)
        for level in 0..6 {
            let score = self.calculate_level_score(version, level);

            if score > best_score {
                best_score = score;
                best_level = Some(level);
            }
        }

        best_level
    }

    /// Get compaction scores for all levels (for monitoring/debugging)
    pub fn get_all_scores(&self, version: &Version) -> Vec<CompactionScore> {
        (0..7)
            .map(|level| CompactionScore {
                level,
                score: self.calculate_level_score(version, level),
            })
            .collect()
    }
}

impl Default for CompactionPicker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{util::Slice, version::version_edit::FileMetaData};

    #[test]
    fn test_target_size_calculation() {
        let picker = CompactionPicker::new();

        assert_eq!(picker.target_size_for_level(0), 0); // Level 0 uses file count
        assert_eq!(picker.target_size_for_level(1), 10 * 1024 * 1024); // 10 MB
        assert_eq!(picker.target_size_for_level(2), 100 * 1024 * 1024); // 100 MB
        assert_eq!(picker.target_size_for_level(3), 1000 * 1024 * 1024); // 1 GB
    }

    #[test]
    fn test_level0_score() {
        let picker = CompactionPicker::new();
        let mut version = Version::new();

        // Add 2 files to level 0 (threshold is 4)
        for i in 0..2 {
            version.add_file(
                0,
                FileMetaData::new(i, 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        let scores = picker.get_all_scores(&version);
        assert_eq!(scores[0].level, 0);
        assert_eq!(scores[0].score, 0.5); // 2/4 = 0.5

        // Add 2 more files (now at threshold)
        for i in 2..4 {
            version.add_file(
                0,
                FileMetaData::new(i, 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        let scores = picker.get_all_scores(&version);
        assert_eq!(scores[0].score, 1.0); // 4/4 = 1.0
    }

    #[test]
    fn test_level_size_score() {
        let picker = CompactionPicker::new();
        let mut version = Version::new();

        // Add 5 MB of files to level 1 (target is 10 MB)
        for i in 0..5 {
            version.add_file(
                1,
                FileMetaData::new(i, 1024 * 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        let scores = picker.get_all_scores(&version);
        assert_eq!(scores[1].level, 1);
        assert!((scores[1].score - 0.5).abs() < 0.01); // 5MB/10MB = 0.5

        // Add 10 more MB (now at 15 MB, over threshold)
        for i in 5..15 {
            version.add_file(
                1,
                FileMetaData::new(i, 1024 * 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        let scores = picker.get_all_scores(&version);
        assert!((scores[1].score - 1.5).abs() < 0.01); // 15MB/10MB = 1.5
    }

    #[test]
    fn test_pick_highest_priority() {
        let picker = CompactionPicker::new();
        let mut version = Version::new();

        // Level 0: 3 files (score = 0.75, under threshold)
        for i in 0..3 {
            version.add_file(
                0,
                FileMetaData::new(i, 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        // Level 1: 15 MB (score = 1.5, over threshold)
        for i in 10..25 {
            version.add_file(
                1,
                FileMetaData::new(i, 1024 * 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        // Level 2: 50 MB (score = 0.5, under threshold)
        for i in 30..80 {
            version.add_file(
                2,
                FileMetaData::new(i, 1024 * 1024, Slice::from("a"), Slice::from("z")),
            );
        }

        // Should pick level 1 (highest score > 1.0)
        let level = picker.pick_compaction(&version);
        assert_eq!(level, Some(1));
    }

    #[test]
    fn test_no_compaction_needed() {
        let picker = CompactionPicker::new();
        let version = Version::new();

        // Empty version, no compaction needed
        let level = picker.pick_compaction(&version);
        assert_eq!(level, None);
    }
}
