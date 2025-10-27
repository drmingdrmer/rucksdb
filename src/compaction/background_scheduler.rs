use std::sync::Arc;

use crate::column_family::ColumnFamilySet;

/// Background compaction scheduler utilities
pub struct BackgroundCompactionScheduler;

impl BackgroundCompactionScheduler {
    /// Check if compaction is needed for a column family based on L0 file count
    pub fn should_compact(
        column_families: &Arc<ColumnFamilySet>,
        cf_id: u32,
        l0_trigger: usize,
    ) -> bool {
        // Get the CF by id (0 is default CF)
        let cf = if cf_id == 0 {
            column_families.default_cf()
        } else {
            return false; // For now, only support default CF
        };

        let version_set = cf.version_set();
        let vs = version_set.read();
        let current = vs.current();
        let version = current.read();

        // Count L0 files
        let l0_file_count = version.files[0].len();
        l0_file_count >= l0_trigger
    }

    /// Check if writes should be stalled due to too many L0 files
    pub fn should_stall_writes(
        column_families: &Arc<ColumnFamilySet>,
        cf_id: u32,
        l0_stop_trigger: usize,
    ) -> bool {
        // Get the CF by id (0 is default CF)
        let cf = if cf_id == 0 {
            column_families.default_cf()
        } else {
            return false; // For now, only support default CF
        };

        let version_set = cf.version_set();
        let vs = version_set.read();
        let current = vs.current();
        let version = current.read();

        // Count L0 files
        let l0_file_count = version.files[0].len();
        l0_file_count >= l0_stop_trigger
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_compact_logic() {
        let cf_set = Arc::new(
            ColumnFamilySet::new(
                "test_db",
                crate::column_family::ColumnFamilyOptions::default(),
            )
            .unwrap(),
        );

        // Initially, L0 should be empty, so should_compact should return false
        let should_compact = BackgroundCompactionScheduler::should_compact(&cf_set, 0, 4);
        assert!(!should_compact);

        // should_stall_writes should also return false
        let should_stall = BackgroundCompactionScheduler::should_stall_writes(&cf_set, 0, 12);
        assert!(!should_stall);
    }
}
