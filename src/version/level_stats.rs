use std::sync::atomic::{AtomicU64, Ordering};

/// Per-level statistics for monitoring compaction health
#[derive(Debug, Default)]
pub struct LevelStats {
    /// Number of files at this level
    pub num_files: AtomicU64,
    /// Total size of all files at this level (bytes)
    pub total_size: AtomicU64,
    /// Number of reads from this level
    pub reads: AtomicU64,
    /// Bytes read from this level
    pub bytes_read: AtomicU64,
    /// Number of writes to this level
    pub writes: AtomicU64,
    /// Bytes written to this level
    pub bytes_written: AtomicU64,
    /// Number of compactions involving this level
    pub compactions: AtomicU64,
}

impl LevelStats {
    pub fn new() -> Self {
        LevelStats::default()
    }

    /// Update file count and size
    #[inline]
    pub fn update_files(&self, num_files: u64, total_size: u64) {
        self.num_files.store(num_files, Ordering::Relaxed);
        self.total_size.store(total_size, Ordering::Relaxed);
    }

    /// Record a read operation
    #[inline]
    pub fn record_read(&self, bytes: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a write operation
    #[inline]
    pub fn record_write(&self, bytes: u64) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a compaction
    #[inline]
    pub fn record_compaction(&self) {
        self.compactions.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current file count
    #[inline]
    pub fn num_files(&self) -> u64 {
        self.num_files.load(Ordering::Relaxed)
    }

    /// Get current total size
    #[inline]
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Get total reads
    #[inline]
    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    /// Get total bytes read
    #[inline]
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Get total writes
    #[inline]
    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// Get total bytes written
    #[inline]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get compaction count
    #[inline]
    pub fn compactions(&self) -> u64 {
        self.compactions.load(Ordering::Relaxed)
    }

    /// Calculate read amplification (bytes read / bytes written)
    /// Returns 0.0 if no writes yet
    pub fn read_amplification(&self) -> f64 {
        let written = self.bytes_written() as f64;
        if written == 0.0 {
            return 0.0;
        }
        self.bytes_read() as f64 / written
    }

    /// Calculate write amplification for this level
    /// This is an approximation based on compaction activity
    pub fn write_amplification(&self) -> f64 {
        let compactions = self.compactions() as f64;
        if compactions == 0.0 {
            return 1.0; // No compaction = 1x write amplification
        }
        // Rough estimate: each compaction rewrites data
        1.0 + compactions
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.num_files.store(0, Ordering::Relaxed);
        self.total_size.store(0, Ordering::Relaxed);
        self.reads.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.compactions.store(0, Ordering::Relaxed);
    }
}

/// Statistics for all levels
#[derive(Debug)]
pub struct AllLevelStats {
    levels: Vec<LevelStats>,
}

impl AllLevelStats {
    /// Create statistics for all levels
    pub fn new(num_levels: usize) -> Self {
        AllLevelStats {
            levels: (0..num_levels).map(|_| LevelStats::new()).collect(),
        }
    }

    /// Get statistics for a specific level
    #[inline]
    pub fn level(&self, level: usize) -> Option<&LevelStats> {
        self.levels.get(level)
    }

    /// Get total database size across all levels
    pub fn total_size(&self) -> u64 {
        self.levels.iter().map(|l| l.total_size()).sum()
    }

    /// Get total file count across all levels
    pub fn total_files(&self) -> u64 {
        self.levels.iter().map(|l| l.num_files()).sum()
    }

    /// Calculate overall read amplification
    pub fn overall_read_amplification(&self) -> f64 {
        let total_read: u64 = self.levels.iter().map(|l| l.bytes_read()).sum();
        let total_written: u64 = self.levels.iter().map(|l| l.bytes_written()).sum();
        if total_written == 0 {
            return 0.0;
        }
        total_read as f64 / total_written as f64
    }

    /// Calculate overall write amplification
    pub fn overall_write_amplification(&self) -> f64 {
        let total_compactions: u64 = self.levels.iter().map(|l| l.compactions()).sum();
        if total_compactions == 0 {
            return 1.0;
        }
        // Better estimate considering multi-level compactions
        1.0 + (total_compactions as f64 / self.levels.len() as f64)
    }

    /// Get number of levels
    #[inline]
    pub fn num_levels(&self) -> usize {
        self.levels.len()
    }
}

impl Default for AllLevelStats {
    fn default() -> Self {
        Self::new(7) // Default to 7 levels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_stats_basic() {
        let stats = LevelStats::new();

        stats.update_files(5, 50_000);
        assert_eq!(stats.num_files(), 5);
        assert_eq!(stats.total_size(), 50_000);

        stats.record_read(1000);
        stats.record_read(2000);
        assert_eq!(stats.reads(), 2);
        assert_eq!(stats.bytes_read(), 3000);

        stats.record_write(500);
        assert_eq!(stats.writes(), 1);
        assert_eq!(stats.bytes_written(), 500);
    }

    #[test]
    fn test_read_amplification() {
        let stats = LevelStats::new();

        // No writes yet
        assert_eq!(stats.read_amplification(), 0.0);

        // Write 1000 bytes, read 3000 bytes = 3x read amplification
        stats.record_write(1000);
        stats.record_read(3000);
        assert!((stats.read_amplification() - 3.0).abs() < 0.01);
    }

    #[test]
    fn test_write_amplification() {
        let stats = LevelStats::new();

        // No compactions = 1x write amplification
        assert_eq!(stats.write_amplification(), 1.0);

        // 2 compactions = approximately 3x
        stats.record_compaction();
        stats.record_compaction();
        assert_eq!(stats.write_amplification(), 3.0);
    }

    #[test]
    fn test_all_level_stats() {
        let all_stats = AllLevelStats::new(7);

        // Update level 0
        all_stats.level(0).unwrap().update_files(4, 4000);
        all_stats.level(0).unwrap().record_write(4000);

        // Update level 1
        all_stats.level(1).unwrap().update_files(10, 100_000);
        all_stats.level(1).unwrap().record_write(100_000);

        assert_eq!(all_stats.total_files(), 14);
        assert_eq!(all_stats.total_size(), 104_000);
    }

    #[test]
    fn test_overall_amplification() {
        let all_stats = AllLevelStats::new(3);

        // Level 0: write 1000, read 0
        all_stats.level(0).unwrap().record_write(1000);

        // Level 1: write 2000, read 1500
        all_stats.level(1).unwrap().record_write(2000);
        all_stats.level(1).unwrap().record_read(1500);

        // Level 2: write 4000, read 6000
        all_stats.level(2).unwrap().record_write(4000);
        all_stats.level(2).unwrap().record_read(6000);

        // Total: written 7000, read 7500
        let ra = all_stats.overall_read_amplification();
        assert!((ra - 7500.0 / 7000.0).abs() < 0.01);
    }

    #[test]
    fn test_reset() {
        let stats = LevelStats::new();

        stats.update_files(5, 5000);
        stats.record_read(1000);
        stats.record_write(500);
        stats.record_compaction();

        stats.reset();

        assert_eq!(stats.num_files(), 0);
        assert_eq!(stats.total_size(), 0);
        assert_eq!(stats.reads(), 0);
        assert_eq!(stats.bytes_read(), 0);
        assert_eq!(stats.writes(), 0);
        assert_eq!(stats.bytes_written(), 0);
        assert_eq!(stats.compactions(), 0);
    }
}
