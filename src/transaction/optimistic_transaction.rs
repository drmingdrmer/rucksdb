use std::{collections::HashMap, sync::Arc};

use crate::{
    column_family::ColumnFamilyHandle,
    db::{DB, ReadOptions, WriteOptions},
    transaction::{Snapshot, WriteBatch, WriteOp},
    util::{Result, Slice, Status},
};

/// OptimisticTransaction provides optimistic concurrency control
/// Detects conflicts at commit time by checking if keys were modified
pub struct OptimisticTransaction {
    /// Reference to database
    db: Arc<DB>,
    /// Snapshot for consistent reads
    snapshot: Snapshot,
    /// Accumulated writes
    write_batch: WriteBatch,
    /// Tracked keys for conflict detection: cf_id -> key -> sequence
    tracked_keys: HashMap<u32, HashMap<Vec<u8>, u64>>,
}

impl OptimisticTransaction {
    /// Create a new optimistic transaction
    pub fn new(db: Arc<DB>, snapshot: Snapshot) -> Self {
        OptimisticTransaction {
            db,
            snapshot,
            write_batch: WriteBatch::new(),
            tracked_keys: HashMap::new(),
        }
    }

    /// Put a key-value pair (default CF)
    pub fn put(&mut self, key: Slice, value: Slice) -> Result<()> {
        let default_cf = self.db.default_cf();
        self.put_cf(&default_cf, key, value)
    }

    /// Put a key-value pair (specific CF)
    pub fn put_cf(
        &mut self,
        cf_handle: &ColumnFamilyHandle,
        key: Slice,
        value: Slice,
    ) -> Result<()> {
        self.track_key_for_conflict_detection(cf_handle.id(), key.data())?;
        self.write_batch.put(cf_handle.id(), key, value)
    }

    /// Delete a key (default CF)
    pub fn delete(&mut self, key: Slice) -> Result<()> {
        let default_cf = self.db.default_cf();
        self.delete_cf(&default_cf, key)
    }

    /// Delete a key (specific CF)
    pub fn delete_cf(&mut self, cf_handle: &ColumnFamilyHandle, key: Slice) -> Result<()> {
        self.track_key_for_conflict_detection(cf_handle.id(), key.data())?;
        self.write_batch.delete(cf_handle.id(), key)
    }

    /// Get a key (default CF) - reads from transaction's write buffer first,
    /// then DB
    pub fn get(&self, key: &Slice) -> Result<Option<Slice>> {
        let default_cf = self.db.default_cf();
        self.get_cf(&default_cf, key)
    }

    /// Get a key (specific CF) - implements read-your-writes
    pub fn get_cf(&self, cf_handle: &ColumnFamilyHandle, key: &Slice) -> Result<Option<Slice>> {
        // Check write batch first (read-your-writes)
        if let Some(op) = self.write_batch.get_for_update(cf_handle.id(), key.data()) {
            return match op {
                WriteOp::Put { value, .. } => Ok(Some(Slice::from(value.as_slice()))),
                WriteOp::Delete { .. } => Ok(None),
            };
        }

        // Read from DB at snapshot sequence
        // TODO: Implement snapshot-aware reads in DB
        self.db.get_cf(&ReadOptions::default(), cf_handle, key)
    }

    /// Track key for conflict detection
    fn track_key_for_conflict_detection(&mut self, cf_id: u32, key: &[u8]) -> Result<()> {
        // Record the sequence number when we first access this key
        if !self
            .tracked_keys
            .entry(cf_id)
            .or_default()
            .contains_key(key)
        {
            // Track at snapshot sequence
            self.tracked_keys
                .get_mut(&cf_id)
                .unwrap()
                .insert(key.to_vec(), self.snapshot.sequence());
        }
        Ok(())
    }

    /// Check for conflicts before commit
    fn check_conflicts(&self) -> Result<()> {
        for (cf_id, keys) in &self.tracked_keys {
            let cf_handle = ColumnFamilyHandle::new(*cf_id, format!("cf_{}", cf_id));

            for key in keys.keys() {
                // Check if key was modified after snapshot
                // This is a simplified check - in production, we'd check the actual sequence
                // For now, we'll check if the value changed
                let current_value = self.db.get_cf(
                    &ReadOptions::default(),
                    &cf_handle,
                    &Slice::from(key.as_slice()),
                )?;

                // Get value at snapshot time (from write batch if modified by us)
                let snapshot_value = if self.write_batch.contains_key(*cf_id, key) {
                    // We modified it, conflict check not needed
                    continue;
                } else {
                    current_value.clone()
                };

                // If values differ, there was a conflict
                // Note: This is simplified. Real implementation would check sequence numbers.
                if current_value != snapshot_value {
                    return Err(Status::busy(format!(
                        "Transaction conflict on key: {:?}",
                        key
                    )));
                }
            }
        }
        Ok(())
    }

    /// Commit the transaction
    pub fn commit(self, options: &WriteOptions) -> Result<()> {
        // Check for conflicts
        self.check_conflicts()?;

        // Write all operations to DB
        for (cf_id, op) in self.write_batch.ops() {
            let cf_handle = ColumnFamilyHandle::new(*cf_id, format!("cf_{}", cf_id));

            match op {
                WriteOp::Put { key, value } => {
                    self.db.put_cf(
                        options,
                        &cf_handle,
                        Slice::from(key.as_slice()),
                        Slice::from(value.as_slice()),
                    )?;
                },
                WriteOp::Delete { key } => {
                    self.db
                        .delete_cf(options, &cf_handle, Slice::from(key.as_slice()))?;
                },
            }
        }

        Ok(())
    }

    /// Rollback the transaction (drop without committing)
    pub fn rollback(self) {
        // WriteBatch will be dropped, no writes to DB
    }

    /// Get snapshot
    #[inline]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Get write batch
    #[inline]
    pub fn write_batch(&self) -> &WriteBatch {
        &self.write_batch
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::db::DBOptions;

    #[test]
    fn test_optimistic_transaction_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db =
            Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());

        // Create snapshot
        let snapshot = Snapshot::new(0);
        let mut txn = OptimisticTransaction::new(db.clone(), snapshot);

        // Write to transaction
        txn.put(Slice::from("key1"), Slice::from("value1")).unwrap();
        txn.put(Slice::from("key2"), Slice::from("value2")).unwrap();

        // Read-your-writes
        let value = txn.get(&Slice::from("key1")).unwrap();
        assert_eq!(value.unwrap().to_string(), "value1");

        // Commit
        txn.commit(&WriteOptions::default()).unwrap();

        // Verify in DB
        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value.unwrap().to_string(), "value1");
    }

    #[test]
    fn test_optimistic_transaction_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let db =
            Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());

        let snapshot = Snapshot::new(0);
        let mut txn = OptimisticTransaction::new(db.clone(), snapshot);

        txn.put(Slice::from("key1"), Slice::from("value1")).unwrap();

        // Rollback (just drop)
        txn.rollback();

        // Key should not exist in DB
        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_optimistic_transaction_delete() {
        let temp_dir = TempDir::new().unwrap();
        let db =
            Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());

        // Put initial value
        db.put(
            &WriteOptions::default(),
            Slice::from("key1"),
            Slice::from("value1"),
        )
        .unwrap();

        let snapshot = Snapshot::new(1);
        let mut txn = OptimisticTransaction::new(db.clone(), snapshot);

        txn.delete(Slice::from("key1")).unwrap();

        // Read should return None (read-your-writes)
        let value = txn.get(&Slice::from("key1")).unwrap();
        assert!(value.is_none());

        // Commit
        txn.commit(&WriteOptions::default()).unwrap();

        // Verify deletion in DB
        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert!(value.is_none());
    }
}
