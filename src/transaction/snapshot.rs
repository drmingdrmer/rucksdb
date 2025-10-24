use std::sync::Arc;

/// Snapshot provides a consistent point-in-time view of the database
/// Used for read isolation in transactions
#[derive(Clone)]
pub struct Snapshot {
    /// Sequence number at snapshot creation
    sequence: u64,
    /// Keep reference to prevent premature cleanup
    _marker: Arc<SnapshotMarker>,
}

/// Marker to track snapshot lifetime
struct SnapshotMarker;

impl Snapshot {
    /// Create a new snapshot at the given sequence number
    pub fn new(sequence: u64) -> Self {
        Snapshot {
            sequence,
            _marker: Arc::new(SnapshotMarker),
        }
    }

    /// Get the snapshot's sequence number
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl Drop for SnapshotMarker {
    fn drop(&mut self) {
        // Could notify DB to release snapshot here
        // For now, snapshots are cheap (just a sequence number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_basic() {
        let snapshot = Snapshot::new(100);
        assert_eq!(snapshot.sequence(), 100);
    }

    #[test]
    fn test_snapshot_clone() {
        let snapshot1 = Snapshot::new(100);
        let snapshot2 = snapshot1.clone();

        assert_eq!(snapshot1.sequence(), snapshot2.sequence());
    }
}
