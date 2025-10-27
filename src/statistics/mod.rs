use std::sync::atomic::{AtomicU64, Ordering};

/// Database-wide statistics
///
/// Thread-safe statistics tracking for all database operations.
/// Uses atomic counters for lock-free updates.
#[derive(Debug, Default)]
pub struct Statistics {
    // Database operations
    pub num_keys_written: AtomicU64,
    pub num_keys_read: AtomicU64,
    pub num_keys_deleted: AtomicU64,
    pub num_iterations: AtomicU64,

    // Bytes transferred
    pub bytes_written: AtomicU64,
    pub bytes_read: AtomicU64,

    // MemTable operations
    pub memtable_hits: AtomicU64,
    pub memtable_misses: AtomicU64,
    pub immutable_memtable_hits: AtomicU64,
    pub num_memtable_flushes: AtomicU64,
    pub bytes_flushed: AtomicU64,

    // WAL operations
    pub wal_writes: AtomicU64,
    pub wal_syncs: AtomicU64,
    pub wal_bytes_written: AtomicU64,

    // SSTable operations
    pub sstable_reads: AtomicU64,
    pub sstable_hits: AtomicU64,
    pub sstable_misses: AtomicU64,
    pub num_blocks_loaded: AtomicU64,
    pub num_blocks_cached: AtomicU64,

    // Compaction statistics
    pub num_compactions: AtomicU64,
    pub compaction_bytes_read: AtomicU64,
    pub compaction_bytes_written: AtomicU64,
    pub num_files_compacted: AtomicU64,
    pub num_parallel_compactions: AtomicU64,
    pub num_sequential_compactions: AtomicU64,
    pub num_subcompactions: AtomicU64,
    pub compaction_time_micros: AtomicU64,

    // Bloom filter stats
    pub bloom_filter_useful: AtomicU64,
    pub bloom_filter_checked: AtomicU64,

    // Error counts
    pub num_errors: AtomicU64,
}

impl Statistics {
    pub fn new() -> Self {
        Statistics::default()
    }

    // Database operation tracking
    #[inline]
    pub fn record_write(&self, bytes: u64) {
        self.num_keys_written.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_read(&self, bytes: u64) {
        self.num_keys_read.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_delete(&self) {
        self.num_keys_deleted.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_iteration(&self) {
        self.num_iterations.fetch_add(1, Ordering::Relaxed);
    }

    // MemTable tracking
    #[inline]
    pub fn record_memtable_hit(&self) {
        self.memtable_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_memtable_miss(&self) {
        self.memtable_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_immutable_memtable_hit(&self) {
        self.immutable_memtable_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_memtable_flush(&self, bytes: u64) {
        self.num_memtable_flushes.fetch_add(1, Ordering::Relaxed);
        self.bytes_flushed.fetch_add(bytes, Ordering::Relaxed);
    }

    // WAL tracking
    #[inline]
    pub fn record_wal_write(&self, bytes: u64) {
        self.wal_writes.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_wal_sync(&self) {
        self.wal_syncs.fetch_add(1, Ordering::Relaxed);
    }

    // SSTable tracking
    #[inline]
    pub fn record_sstable_read(&self) {
        self.sstable_reads.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_sstable_hit(&self) {
        self.sstable_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_sstable_miss(&self) {
        self.sstable_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_block_loaded(&self) {
        self.num_blocks_loaded.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_block_cached(&self) {
        self.num_blocks_cached.fetch_add(1, Ordering::Relaxed);
    }

    // Compaction tracking
    #[inline]
    pub fn record_compaction(&self, bytes_read: u64, bytes_written: u64, num_files: u64) {
        self.num_compactions.fetch_add(1, Ordering::Relaxed);
        self.compaction_bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.compaction_bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.num_files_compacted
            .fetch_add(num_files, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_parallel_compaction(
        &self,
        bytes_read: u64,
        bytes_written: u64,
        num_files: u64,
        num_subcompactions: u64,
        time_micros: u64,
    ) {
        self.record_compaction(bytes_read, bytes_written, num_files);
        self.num_parallel_compactions
            .fetch_add(1, Ordering::Relaxed);
        self.num_subcompactions
            .fetch_add(num_subcompactions, Ordering::Relaxed);
        self.compaction_time_micros
            .fetch_add(time_micros, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_sequential_compaction(
        &self,
        bytes_read: u64,
        bytes_written: u64,
        num_files: u64,
        time_micros: u64,
    ) {
        self.record_compaction(bytes_read, bytes_written, num_files);
        self.num_sequential_compactions
            .fetch_add(1, Ordering::Relaxed);
        self.compaction_time_micros
            .fetch_add(time_micros, Ordering::Relaxed);
    }

    // Bloom filter tracking
    #[inline]
    pub fn record_bloom_filter_check(&self, useful: bool) {
        self.bloom_filter_checked.fetch_add(1, Ordering::Relaxed);
        if useful {
            self.bloom_filter_useful.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Error tracking
    #[inline]
    pub fn record_error(&self) {
        self.num_errors.fetch_add(1, Ordering::Relaxed);
    }

    // Getters (snapshot values)
    pub fn num_keys_written(&self) -> u64 {
        self.num_keys_written.load(Ordering::Relaxed)
    }

    pub fn num_keys_read(&self) -> u64 {
        self.num_keys_read.load(Ordering::Relaxed)
    }

    pub fn num_keys_deleted(&self) -> u64 {
        self.num_keys_deleted.load(Ordering::Relaxed)
    }

    pub fn num_iterations(&self) -> u64 {
        self.num_iterations.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    pub fn memtable_hit_rate(&self) -> f64 {
        let hits = self.memtable_hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.memtable_misses.load(Ordering::Relaxed) as f64;
        if total > 0.0 { hits / total } else { 0.0 }
    }

    pub fn sstable_hit_rate(&self) -> f64 {
        let hits = self.sstable_hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.sstable_misses.load(Ordering::Relaxed) as f64;
        if total > 0.0 { hits / total } else { 0.0 }
    }

    pub fn bloom_filter_effectiveness(&self) -> f64 {
        let useful = self.bloom_filter_useful.load(Ordering::Relaxed) as f64;
        let checked = self.bloom_filter_checked.load(Ordering::Relaxed) as f64;
        if checked > 0.0 { useful / checked } else { 0.0 }
    }

    pub fn compaction_read_write_ratio(&self) -> f64 {
        let read = self.compaction_bytes_read.load(Ordering::Relaxed) as f64;
        let written = self.compaction_bytes_written.load(Ordering::Relaxed) as f64;
        if written > 0.0 { read / written } else { 0.0 }
    }

    pub fn avg_compaction_time_ms(&self) -> f64 {
        let total_time = self.compaction_time_micros.load(Ordering::Relaxed) as f64;
        let num_compactions = self.num_compactions.load(Ordering::Relaxed) as f64;
        if num_compactions > 0.0 {
            total_time / num_compactions / 1000.0
        } else {
            0.0
        }
    }

    pub fn parallel_compaction_ratio(&self) -> f64 {
        let parallel = self.num_parallel_compactions.load(Ordering::Relaxed) as f64;
        let total = self.num_compactions.load(Ordering::Relaxed) as f64;
        if total > 0.0 { parallel / total } else { 0.0 }
    }

    /// Reset all statistics to zero
    pub fn reset(&self) {
        self.num_keys_written.store(0, Ordering::Relaxed);
        self.num_keys_read.store(0, Ordering::Relaxed);
        self.num_keys_deleted.store(0, Ordering::Relaxed);
        self.num_iterations.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.memtable_hits.store(0, Ordering::Relaxed);
        self.memtable_misses.store(0, Ordering::Relaxed);
        self.immutable_memtable_hits.store(0, Ordering::Relaxed);
        self.num_memtable_flushes.store(0, Ordering::Relaxed);
        self.bytes_flushed.store(0, Ordering::Relaxed);
        self.wal_writes.store(0, Ordering::Relaxed);
        self.wal_syncs.store(0, Ordering::Relaxed);
        self.wal_bytes_written.store(0, Ordering::Relaxed);
        self.sstable_reads.store(0, Ordering::Relaxed);
        self.sstable_hits.store(0, Ordering::Relaxed);
        self.sstable_misses.store(0, Ordering::Relaxed);
        self.num_blocks_loaded.store(0, Ordering::Relaxed);
        self.num_blocks_cached.store(0, Ordering::Relaxed);
        self.num_compactions.store(0, Ordering::Relaxed);
        self.compaction_bytes_read.store(0, Ordering::Relaxed);
        self.compaction_bytes_written.store(0, Ordering::Relaxed);
        self.num_files_compacted.store(0, Ordering::Relaxed);
        self.num_parallel_compactions.store(0, Ordering::Relaxed);
        self.num_sequential_compactions.store(0, Ordering::Relaxed);
        self.num_subcompactions.store(0, Ordering::Relaxed);
        self.compaction_time_micros.store(0, Ordering::Relaxed);
        self.bloom_filter_useful.store(0, Ordering::Relaxed);
        self.bloom_filter_checked.store(0, Ordering::Relaxed);
        self.num_errors.store(0, Ordering::Relaxed);
    }

    /// Get a formatted statistics report
    pub fn report(&self) -> String {
        format!(
            "Database Statistics:\n\
            \n\
            Operations:\n\
            - Keys written:  {}\n\
            - Keys read:     {}\n\
            - Keys deleted:  {}\n\
            - Iterations:    {}\n\
            - Bytes written: {} ({:.2} MB)\n\
            - Bytes read:    {} ({:.2} MB)\n\
            \n\
            MemTable:\n\
            - Hits:          {}\n\
            - Misses:        {}\n\
            - Hit rate:      {:.2}%\n\
            - Imm hits:      {}\n\
            - Flushes:       {}\n\
            - Bytes flushed: {} ({:.2} MB)\n\
            \n\
            WAL:\n\
            - Writes:        {}\n\
            - Syncs:         {}\n\
            - Bytes written: {} ({:.2} MB)\n\
            \n\
            SSTable:\n\
            - Reads:         {}\n\
            - Hits:          {}\n\
            - Misses:        {}\n\
            - Hit rate:      {:.2}%\n\
            - Blocks loaded: {}\n\
            - Blocks cached: {}\n\
            \n\
            Compaction:\n\
            - Runs:          {}\n\
            - Parallel:      {} ({:.1}%)\n\
            - Sequential:    {}\n\
            - Subcompactions: {}\n\
            - Avg time:      {:.2} ms\n\
            - Bytes read:    {} ({:.2} MB)\n\
            - Bytes written: {} ({:.2} MB)\n\
            - Files:         {}\n\
            - R/W ratio:     {:.2}\n\
            \n\
            Bloom Filter:\n\
            - Checked:       {}\n\
            - Useful:        {}\n\
            - Effectiveness: {:.2}%\n\
            \n\
            Errors:          {}",
            self.num_keys_written(),
            self.num_keys_read(),
            self.num_keys_deleted(),
            self.num_iterations(),
            self.bytes_written(),
            self.bytes_written() as f64 / 1024.0 / 1024.0,
            self.bytes_read(),
            self.bytes_read() as f64 / 1024.0 / 1024.0,
            self.memtable_hits.load(Ordering::Relaxed),
            self.memtable_misses.load(Ordering::Relaxed),
            self.memtable_hit_rate() * 100.0,
            self.immutable_memtable_hits.load(Ordering::Relaxed),
            self.num_memtable_flushes.load(Ordering::Relaxed),
            self.bytes_flushed.load(Ordering::Relaxed),
            self.bytes_flushed.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
            self.wal_writes.load(Ordering::Relaxed),
            self.wal_syncs.load(Ordering::Relaxed),
            self.wal_bytes_written.load(Ordering::Relaxed),
            self.wal_bytes_written.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
            self.sstable_reads.load(Ordering::Relaxed),
            self.sstable_hits.load(Ordering::Relaxed),
            self.sstable_misses.load(Ordering::Relaxed),
            self.sstable_hit_rate() * 100.0,
            self.num_blocks_loaded.load(Ordering::Relaxed),
            self.num_blocks_cached.load(Ordering::Relaxed),
            self.num_compactions.load(Ordering::Relaxed),
            self.num_parallel_compactions.load(Ordering::Relaxed),
            self.parallel_compaction_ratio() * 100.0,
            self.num_sequential_compactions.load(Ordering::Relaxed),
            self.num_subcompactions.load(Ordering::Relaxed),
            self.avg_compaction_time_ms(),
            self.compaction_bytes_read.load(Ordering::Relaxed),
            self.compaction_bytes_read.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
            self.compaction_bytes_written.load(Ordering::Relaxed),
            self.compaction_bytes_written.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
            self.num_files_compacted.load(Ordering::Relaxed),
            self.compaction_read_write_ratio(),
            self.bloom_filter_checked.load(Ordering::Relaxed),
            self.bloom_filter_useful.load(Ordering::Relaxed),
            self.bloom_filter_effectiveness() * 100.0,
            self.num_errors.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_basic() {
        let stats = Statistics::new();

        stats.record_write(100);
        stats.record_write(200);
        stats.record_read(50);

        assert_eq!(stats.num_keys_written(), 2);
        assert_eq!(stats.num_keys_read(), 1);
        assert_eq!(stats.bytes_written(), 300);
        assert_eq!(stats.bytes_read(), 50);
    }

    #[test]
    fn test_memtable_hit_rate() {
        let stats = Statistics::new();

        stats.record_memtable_hit();
        stats.record_memtable_hit();
        stats.record_memtable_hit();
        stats.record_memtable_miss();

        assert_eq!(stats.memtable_hit_rate(), 0.75);
    }

    #[test]
    fn test_bloom_filter_effectiveness() {
        let stats = Statistics::new();

        stats.record_bloom_filter_check(true);
        stats.record_bloom_filter_check(true);
        stats.record_bloom_filter_check(false);
        stats.record_bloom_filter_check(true);

        assert_eq!(stats.bloom_filter_effectiveness(), 0.75);
    }

    #[test]
    fn test_statistics_reset() {
        let stats = Statistics::new();

        stats.record_write(100);
        stats.record_read(50);
        stats.record_delete();

        assert!(stats.num_keys_written() > 0);

        stats.reset();

        assert_eq!(stats.num_keys_written(), 0);
        assert_eq!(stats.num_keys_read(), 0);
        assert_eq!(stats.num_keys_deleted(), 0);
        assert_eq!(stats.bytes_written(), 0);
        assert_eq!(stats.bytes_read(), 0);
    }

    #[test]
    fn test_compaction_stats() {
        let stats = Statistics::new();

        stats.record_compaction(1000, 800, 5);
        stats.record_compaction(2000, 1600, 10);

        assert_eq!(stats.num_compactions.load(Ordering::Relaxed), 2);
        assert_eq!(stats.compaction_bytes_read.load(Ordering::Relaxed), 3000);
        assert_eq!(stats.compaction_bytes_written.load(Ordering::Relaxed), 2400);
        assert_eq!(stats.num_files_compacted.load(Ordering::Relaxed), 15);
        assert_eq!(stats.compaction_read_write_ratio(), 1.25);
    }

    #[test]
    fn test_statistics_report() {
        let stats = Statistics::new();

        stats.record_write(1024);
        stats.record_read(512);
        stats.record_memtable_hit();
        stats.record_memtable_miss();

        let report = stats.report();
        assert!(report.contains("Keys written:  1"));
        assert!(report.contains("Keys read:     1"));
        assert!(report.contains("Hit rate:      50.00%"));
    }

    #[test]
    fn test_parallel_compaction_stats() {
        let stats = Statistics::new();

        // Record 2 parallel compactions
        stats.record_parallel_compaction(10000, 8000, 5, 4, 5000);
        stats.record_parallel_compaction(20000, 16000, 10, 4, 7000);

        // Record 1 sequential compaction
        stats.record_sequential_compaction(5000, 4000, 3, 3000);

        assert_eq!(stats.num_compactions.load(Ordering::Relaxed), 3);
        assert_eq!(stats.num_parallel_compactions.load(Ordering::Relaxed), 2);
        assert_eq!(stats.num_sequential_compactions.load(Ordering::Relaxed), 1);
        assert_eq!(stats.num_subcompactions.load(Ordering::Relaxed), 8); // 4+4
        assert_eq!(stats.compaction_time_micros.load(Ordering::Relaxed), 15000); // 5000+7000+3000
        assert_eq!(stats.compaction_bytes_read.load(Ordering::Relaxed), 35000);
        assert_eq!(
            stats.compaction_bytes_written.load(Ordering::Relaxed),
            28000
        );
        assert_eq!(stats.parallel_compaction_ratio(), 2.0 / 3.0);
        assert_eq!(stats.avg_compaction_time_ms(), 5.0); // 15000/3/1000
    }
}
