use std::collections::HashMap;

use crate::util::{Result, Slice};

/// Write operation type
#[derive(Debug, Clone)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// WriteBatch accumulates multiple write operations for atomic execution
/// Provides index for fast key lookup during transaction conflict detection
pub struct WriteBatch {
    /// Operations in insertion order
    ops: Vec<(u32, WriteOp)>, // (cf_id, operation)
    /// Index for fast lookup: cf_id -> key -> latest op index
    index: HashMap<u32, HashMap<Vec<u8>, usize>>,
    /// Approximate memory usage in bytes
    data_size: usize,
}

impl WriteBatch {
    /// Create a new empty WriteBatch
    #[inline]
    pub fn new() -> Self {
        WriteBatch {
            ops: Vec::new(),
            index: HashMap::new(),
            data_size: 0,
        }
    }

    /// Create WriteBatch with reserved capacity
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        WriteBatch {
            ops: Vec::with_capacity(capacity),
            index: HashMap::new(),
            data_size: 0,
        }
    }

    /// Add a Put operation to the batch
    pub fn put(&mut self, cf_id: u32, key: Slice, value: Slice) -> Result<()> {
        let key_vec = key.data().to_vec();
        let value_vec = value.data().to_vec();

        self.data_size += key_vec.len() + value_vec.len();

        let op = WriteOp::Put {
            key: key_vec.clone(),
            value: value_vec,
        };

        self.add_to_index(cf_id, key_vec, self.ops.len());
        self.ops.push((cf_id, op));

        Ok(())
    }

    /// Add a Delete operation to the batch
    pub fn delete(&mut self, cf_id: u32, key: Slice) -> Result<()> {
        let key_vec = key.data().to_vec();

        self.data_size += key_vec.len();

        let op = WriteOp::Delete {
            key: key_vec.clone(),
        };

        self.add_to_index(cf_id, key_vec, self.ops.len());
        self.ops.push((cf_id, op));

        Ok(())
    }

    /// Add operation index for fast lookup
    #[inline]
    fn add_to_index(&mut self, cf_id: u32, key: Vec<u8>, op_index: usize) {
        self.index.entry(cf_id).or_default().insert(key, op_index);
    }

    /// Get the latest operation for a key (for read-your-writes)
    pub fn get_for_update(&self, cf_id: u32, key: &[u8]) -> Option<&WriteOp> {
        self.index
            .get(&cf_id)?
            .get(key)
            .and_then(|&idx| self.ops.get(idx).map(|(_, op)| op))
    }

    /// Check if batch contains a key
    #[inline]
    pub fn contains_key(&self, cf_id: u32, key: &[u8]) -> bool {
        self.index
            .get(&cf_id)
            .is_some_and(|cf_index| cf_index.contains_key(key))
    }

    /// Get all operations
    #[inline]
    pub fn ops(&self) -> &[(u32, WriteOp)] {
        &self.ops
    }

    /// Number of operations in the batch
    #[inline]
    pub fn count(&self) -> usize {
        self.ops.len()
    }

    /// Clear all operations
    pub fn clear(&mut self) {
        self.ops.clear();
        self.index.clear();
        self.data_size = 0;
    }

    /// Approximate memory usage in bytes
    #[inline]
    pub fn data_size(&self) -> usize {
        self.data_size
    }

    /// Check if batch is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch_basic() {
        let mut batch = WriteBatch::new();

        batch
            .put(0, Slice::from("key1"), Slice::from("value1"))
            .unwrap();
        batch
            .put(0, Slice::from("key2"), Slice::from("value2"))
            .unwrap();
        batch.delete(0, Slice::from("key3")).unwrap();

        assert_eq!(batch.count(), 3);
        assert_eq!(batch.data_size(), 4 + 6 + 4 + 6 + 4); // key1+val1 + key2+val2 + key3
    }

    #[test]
    fn test_write_batch_index() {
        let mut batch = WriteBatch::new();

        batch
            .put(0, Slice::from("key1"), Slice::from("value1"))
            .unwrap();
        batch
            .put(0, Slice::from("key1"), Slice::from("value2"))
            .unwrap(); // Overwrite

        // Should return latest value
        match batch.get_for_update(0, b"key1") {
            Some(WriteOp::Put { value, .. }) => {
                assert_eq!(value, b"value2");
            },
            _ => panic!("Expected Put operation"),
        }
    }

    #[test]
    fn test_write_batch_multi_cf() {
        let mut batch = WriteBatch::new();

        batch
            .put(0, Slice::from("key1"), Slice::from("value1"))
            .unwrap();
        batch
            .put(1, Slice::from("key1"), Slice::from("value2"))
            .unwrap();

        // Different CFs should be separate
        assert!(batch.contains_key(0, b"key1"));
        assert!(batch.contains_key(1, b"key1"));

        match batch.get_for_update(0, b"key1") {
            Some(WriteOp::Put { value, .. }) => assert_eq!(value, b"value1"),
            _ => panic!("Expected Put for CF 0"),
        }

        match batch.get_for_update(1, b"key1") {
            Some(WriteOp::Put { value, .. }) => assert_eq!(value, b"value2"),
            _ => panic!("Expected Put for CF 1"),
        }
    }

    #[test]
    fn test_write_batch_clear() {
        let mut batch = WriteBatch::new();

        batch
            .put(0, Slice::from("key1"), Slice::from("value1"))
            .unwrap();
        assert_eq!(batch.count(), 1);

        batch.clear();
        assert_eq!(batch.count(), 0);
        assert_eq!(batch.data_size(), 0);
        assert!(!batch.contains_key(0, b"key1"));
    }

    #[test]
    fn test_write_batch_delete_in_index() {
        let mut batch = WriteBatch::new();

        batch
            .put(0, Slice::from("key1"), Slice::from("value1"))
            .unwrap();
        batch.delete(0, Slice::from("key1")).unwrap();

        // Should return Delete operation
        match batch.get_for_update(0, b"key1") {
            Some(WriteOp::Delete { .. }) => {},
            _ => panic!("Expected Delete operation"),
        }
    }
}
