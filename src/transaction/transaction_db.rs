use std::{collections::HashMap, sync::Arc, time::Duration};

use parking_lot::RwLock;

use crate::{
    column_family::ColumnFamilyHandle,
    db::{DB, ReadOptions, WriteOptions},
    transaction::{Snapshot, WriteBatch, WriteOp},
    util::{Result, Slice, Status},
};

/// Lock type for pessimistic transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Read,
    Write,
}

/// Lock entry for a key
struct LockEntry {
    lock_type: LockType,
    txn_id: u64,
}

/// Transaction with pessimistic locking
pub struct Transaction {
    /// Transaction ID
    id: u64,
    /// Reference to TransactionDB
    db: Arc<TransactionDB>,
    /// Snapshot for consistent reads
    snapshot: Snapshot,
    /// Accumulated writes
    write_batch: WriteBatch,
    /// Locked keys: cf_id -> key -> lock_type
    locked_keys: HashMap<u32, HashMap<Vec<u8>, LockType>>,
}

impl Transaction {
    /// Create a new transaction
    fn new(id: u64, db: Arc<TransactionDB>, snapshot: Snapshot) -> Self {
        Transaction {
            id,
            db,
            snapshot,
            write_batch: WriteBatch::new(),
            locked_keys: HashMap::new(),
        }
    }

    /// Acquire a read lock on a key
    pub fn get_for_update(&mut self, key: Slice) -> Result<Option<Slice>> {
        let default_cf = self.db.db.default_cf();
        self.get_for_update_cf(&default_cf, key)
    }

    /// Acquire a read lock on a key (specific CF)
    pub fn get_for_update_cf(
        &mut self,
        cf_handle: &ColumnFamilyHandle,
        key: Slice,
    ) -> Result<Option<Slice>> {
        self.acquire_lock(cf_handle.id(), key.data(), LockType::Read)?;

        // Check write batch first
        if let Some(op) = self.write_batch.get_for_update(cf_handle.id(), key.data()) {
            return match op {
                WriteOp::Put { value, .. } => Ok(Some(Slice::from(value.as_slice()))),
                WriteOp::Delete { .. } => Ok(None),
                WriteOp::Merge { value, .. } => Ok(Some(Slice::from(value.as_slice()))),
            };
        }

        self.db.db.get_cf(&ReadOptions::default(), cf_handle, &key)
    }

    /// Put a key-value pair (default CF)
    pub fn put(&mut self, key: Slice, value: Slice) -> Result<()> {
        let default_cf = self.db.db.default_cf();
        self.put_cf(&default_cf, key, value)
    }

    /// Put a key-value pair (specific CF)
    pub fn put_cf(
        &mut self,
        cf_handle: &ColumnFamilyHandle,
        key: Slice,
        value: Slice,
    ) -> Result<()> {
        self.acquire_lock(cf_handle.id(), key.data(), LockType::Write)?;
        self.write_batch.put(cf_handle.id(), key, value)
    }

    /// Delete a key (default CF)
    pub fn delete(&mut self, key: Slice) -> Result<()> {
        let default_cf = self.db.db.default_cf();
        self.delete_cf(&default_cf, key)
    }

    /// Delete a key (specific CF)
    pub fn delete_cf(&mut self, cf_handle: &ColumnFamilyHandle, key: Slice) -> Result<()> {
        self.acquire_lock(cf_handle.id(), key.data(), LockType::Write)?;
        self.write_batch.delete(cf_handle.id(), key)
    }

    /// Acquire a lock on a key
    fn acquire_lock(&mut self, cf_id: u32, key: &[u8], lock_type: LockType) -> Result<()> {
        // Check if we already hold this lock
        if let Some(existing_lock) = self
            .locked_keys
            .get(&cf_id)
            .and_then(|cf_locks| cf_locks.get(key))
        {
            // Upgrade read lock to write lock if needed
            if *existing_lock == LockType::Read && lock_type == LockType::Write {
                self.db
                    .lock_manager
                    .upgrade_lock(cf_id, key, self.id, Duration::from_secs(5))?;
                self.locked_keys
                    .get_mut(&cf_id)
                    .unwrap()
                    .insert(key.to_vec(), LockType::Write);
            }
            return Ok(());
        }

        // Acquire new lock
        self.db.lock_manager.acquire_lock(
            cf_id,
            key,
            lock_type,
            self.id,
            Duration::from_secs(5),
        )?;

        // Track lock
        self.locked_keys
            .entry(cf_id)
            .or_default()
            .insert(key.to_vec(), lock_type);

        Ok(())
    }

    /// Commit the transaction
    pub fn commit(mut self, options: &WriteOptions) -> Result<()> {
        // Write all operations to DB
        for (cf_id, op) in self.write_batch.ops() {
            let cf_handle = ColumnFamilyHandle::new(*cf_id, format!("cf_{}", cf_id));

            match op {
                WriteOp::Put { key, value } => {
                    self.db.db.put_cf(
                        options,
                        &cf_handle,
                        Slice::from(key.as_slice()),
                        Slice::from(value.as_slice()),
                    )?;
                },
                WriteOp::Delete { key } => {
                    self.db
                        .db
                        .delete_cf(options, &cf_handle, Slice::from(key.as_slice()))?;
                },
                WriteOp::Merge { key, value } => {
                    // Treat as put for now
                    self.db.db.put_cf(
                        options,
                        &cf_handle,
                        Slice::from(key.as_slice()),
                        Slice::from(value.as_slice()),
                    )?;
                },
            }
        }

        // Release all locks
        self.release_all_locks();

        Ok(())
    }

    /// Rollback the transaction
    pub fn rollback(mut self) {
        // Release all locks
        self.release_all_locks();
    }

    /// Release all locks held by this transaction
    fn release_all_locks(&mut self) {
        for (cf_id, cf_locks) in &self.locked_keys {
            for key in cf_locks.keys() {
                let _ = self.db.lock_manager.release_lock(*cf_id, key, self.id);
            }
        }
        self.locked_keys.clear();
    }

    /// Get snapshot
    #[inline]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Ensure locks are released even if commit/rollback not called
        self.release_all_locks();
    }
}

/// Type alias for lock storage
type LockStorage = HashMap<u32, HashMap<Vec<u8>, Vec<LockEntry>>>;

/// Lock manager for pessimistic transactions
struct LockManager {
    /// Locks: cf_id -> key -> lock_entry
    locks: RwLock<LockStorage>,
}

impl LockManager {
    fn new() -> Self {
        LockManager {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire a lock with timeout
    fn acquire_lock(
        &self,
        cf_id: u32,
        key: &[u8],
        lock_type: LockType,
        txn_id: u64,
        timeout: Duration,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        loop {
            {
                let mut locks = self.locks.write();
                let cf_locks = locks.entry(cf_id).or_default();
                let key_locks = cf_locks.entry(key.to_vec()).or_default();

                // Check compatibility
                if self.can_acquire_lock(key_locks, lock_type, txn_id) {
                    key_locks.push(LockEntry { lock_type, txn_id });
                    return Ok(());
                }
            }

            // Check timeout
            if start.elapsed() > timeout {
                return Err(Status::busy(format!("Lock timeout for key: {:?}", key)));
            }

            // Wait a bit before retry
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Check if lock can be acquired
    fn can_acquire_lock(&self, key_locks: &[LockEntry], lock_type: LockType, txn_id: u64) -> bool {
        if key_locks.is_empty() {
            return true;
        }

        // Check if we already hold a lock
        if key_locks.iter().any(|entry| entry.txn_id == txn_id) {
            return true;
        }

        // Read locks are compatible with other read locks
        if lock_type == LockType::Read {
            key_locks
                .iter()
                .all(|entry| entry.lock_type == LockType::Read)
        } else {
            // Write locks are not compatible with any locks
            false
        }
    }

    /// Upgrade read lock to write lock
    fn upgrade_lock(&self, cf_id: u32, key: &[u8], txn_id: u64, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();

        loop {
            {
                let mut locks = self.locks.write();
                if let Some(cf_locks) = locks.get_mut(&cf_id)
                    && let Some(key_locks) = cf_locks.get_mut(key)
                {
                    // Check if we're the only holder
                    let our_locks: Vec<_> =
                        key_locks.iter().filter(|e| e.txn_id == txn_id).collect();

                    if our_locks.len() == 1 && key_locks.len() == 1 {
                        // We're the only holder, upgrade
                        key_locks[0].lock_type = LockType::Write;
                        return Ok(());
                    }
                }
            }

            if start.elapsed() > timeout {
                return Err(Status::busy("Lock upgrade timeout"));
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Release a lock
    fn release_lock(&self, cf_id: u32, key: &[u8], txn_id: u64) -> Result<()> {
        let mut locks = self.locks.write();
        if let Some(cf_locks) = locks.get_mut(&cf_id)
            && let Some(key_locks) = cf_locks.get_mut(key)
        {
            key_locks.retain(|entry| entry.txn_id != txn_id);
            if key_locks.is_empty() {
                cf_locks.remove(key);
            }
        }
        Ok(())
    }
}

/// Database with transaction support (pessimistic locking)
pub struct TransactionDB {
    /// Underlying database
    db: Arc<DB>,
    /// Lock manager
    lock_manager: Arc<LockManager>,
    /// Next transaction ID
    next_txn_id: RwLock<u64>,
}

impl TransactionDB {
    /// Open a database with transaction support
    pub fn open(db: Arc<DB>) -> Self {
        TransactionDB {
            db,
            lock_manager: Arc::new(LockManager::new()),
            next_txn_id: RwLock::new(0),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Transaction {
        let txn_id = {
            let mut next_id = self.next_txn_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Create snapshot at current sequence
        // TODO: Get actual sequence from DB
        let snapshot = Snapshot::new(txn_id);

        Transaction::new(txn_id, Arc::new(self.clone()), snapshot)
    }

    /// Get reference to underlying database
    #[inline]
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }
}

impl Clone for TransactionDB {
    fn clone(&self) -> Self {
        TransactionDB {
            db: self.db.clone(),
            lock_manager: self.lock_manager.clone(),
            next_txn_id: RwLock::new(*self.next_txn_id.read()),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::db::DBOptions;

    #[test]
    fn test_transaction_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db =
            Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());
        let txn_db = TransactionDB::open(db.clone());

        let mut txn = txn_db.begin_transaction();
        txn.put(Slice::from("key1"), Slice::from("value1")).unwrap();
        txn.put(Slice::from("key2"), Slice::from("value2")).unwrap();
        txn.commit(&WriteOptions::default()).unwrap();

        // Verify in DB
        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value.unwrap().to_string(), "value1");
    }

    #[test]
    fn test_transaction_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let db =
            Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());
        let txn_db = TransactionDB::open(db.clone());

        let mut txn = txn_db.begin_transaction();
        txn.put(Slice::from("key1"), Slice::from("value1")).unwrap();
        txn.rollback();

        // Key should not exist
        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_transaction_lock_conflict() {
        let temp_dir = TempDir::new().unwrap();
        let db =
            Arc::new(DB::open(temp_dir.path().to_str().unwrap(), DBOptions::default()).unwrap());
        let txn_db = TransactionDB::open(db.clone());

        // Transaction 1 acquires write lock
        let mut txn1 = txn_db.begin_transaction();
        txn1.put(Slice::from("key1"), Slice::from("value1"))
            .unwrap();

        // Transaction 2 tries to acquire write lock (should timeout)
        let mut txn2 = txn_db.begin_transaction();
        let result = txn2.put(Slice::from("key1"), Slice::from("value2"));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &crate::util::Code::Busy);

        // Commit txn1
        txn1.commit(&WriteOptions::default()).unwrap();

        // Now txn2 should succeed
        let mut txn3 = txn_db.begin_transaction();
        txn3.put(Slice::from("key1"), Slice::from("value3"))
            .unwrap();
        txn3.commit(&WriteOptions::default()).unwrap();

        let value = db
            .get(&ReadOptions::default(), &Slice::from("key1"))
            .unwrap();
        assert_eq!(value.unwrap().to_string(), "value3");
    }

    #[test]
    fn test_transaction_read_locks_compatible() {
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

        let txn_db = TransactionDB::open(db.clone());

        // Two transactions acquire read locks (should succeed)
        let mut txn1 = txn_db.begin_transaction();
        let _ = txn1.get_for_update(Slice::from("key1")).unwrap();

        let mut txn2 = txn_db.begin_transaction();
        let result = txn2.get_for_update(Slice::from("key1"));
        assert!(result.is_ok());

        txn1.rollback();
        txn2.rollback();
    }
}
