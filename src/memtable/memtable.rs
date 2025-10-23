use crate::memtable::skiplist::SkipList;
use crate::util::{Result, Slice, Status};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const VALUE_TYPE_DELETION: u8 = 0;
const VALUE_TYPE_VALUE: u8 = 1;

#[derive(Clone)]
pub struct InternalKey {
    user_key: Slice,
    sequence: u64,
    value_type: u8,
}

impl InternalKey {
    pub fn new(user_key: Slice, sequence: u64, value_type: u8) -> Self {
        InternalKey {
            user_key,
            sequence,
            value_type,
        }
    }

    pub fn encode(&self) -> Slice {
        let mut buf = self.user_key.data().to_vec();
        // Add separator to ensure proper grouping of same user_key
        buf.push(0x00);
        // Encode sequence in reverse order for descending sort
        let reversed_seq = u64::MAX - self.sequence;
        buf.extend_from_slice(&reversed_seq.to_be_bytes());
        buf.push(self.value_type);
        Slice::from(buf)
    }

    pub fn decode(data: &Slice) -> Result<Self> {
        if data.size() < 10 {
            return Err(Status::corruption("InternalKey too short"));
        }

        // Find separator (0x00)
        let key_len = data.size() - 10; // user_key + separator + 8 bytes seq + 1 byte type
        let user_key = Slice::from(&data.data()[..key_len]);

        // Skip separator at key_len
        let seq_start = key_len + 1;
        let seq_bytes: [u8; 8] = data.data()[seq_start..seq_start + 8]
            .try_into()
            .map_err(|_| Status::corruption("Invalid sequence number"))?;
        let reversed_seq = u64::from_be_bytes(seq_bytes);
        let sequence = u64::MAX - reversed_seq;
        let value_type = data.data()[seq_start + 8];

        Ok(InternalKey {
            user_key,
            sequence,
            value_type,
        })
    }

    pub fn user_key(&self) -> &Slice {
        &self.user_key
    }

    pub fn is_deletion(&self) -> bool {
        self.value_type == VALUE_TYPE_DELETION
    }
}

pub struct MemTable {
    table: SkipList,
    approximate_memory: Arc<AtomicUsize>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            table: SkipList::new(),
            approximate_memory: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn add(&self, sequence: u64, key: Slice, value: Slice) {
        let internal_key = InternalKey::new(key, sequence, VALUE_TYPE_VALUE);
        let encoded_key = internal_key.encode();

        let mem_usage = encoded_key.size() + value.size();
        self.approximate_memory
            .fetch_add(mem_usage, Ordering::Relaxed);

        self.table.insert(encoded_key, value);
    }

    pub fn delete(&self, sequence: u64, key: Slice) {
        let internal_key = InternalKey::new(key, sequence, VALUE_TYPE_DELETION);
        let encoded_key = internal_key.encode();

        let mem_usage = encoded_key.size();
        self.approximate_memory
            .fetch_add(mem_usage, Ordering::Relaxed);

        self.table.insert(encoded_key, Slice::empty());
    }

    /// Get value for a key. Returns (found, value).
    /// - (true, Some(value)) => key found with value
    /// - (true, None) => key found but deleted
    /// - (false, None) => key not found in memtable
    pub fn get(&self, key: &Slice) -> (bool, Option<Slice>) {
        let iter = self.table.iter();

        // Seek to the first entry with this user_key
        // Use sequence u64::MAX (which becomes 0 after reverse) to get the smallest encoded value
        // This ensures we start from the beginning of all entries for this user_key
        let start_key = InternalKey::new(key.clone(), u64::MAX, VALUE_TYPE_VALUE).encode();

        let entries = iter.range_from(&start_key);

        // Find the first entry that matches the user_key
        // Due to reversed sequence encoding, the first matching entry has the largest sequence
        for (internal_key_data, value) in entries {
            if let Ok(internal_key) = InternalKey::decode(&internal_key_data) {
                if internal_key.user_key() == key {
                    if internal_key.is_deletion() {
                        return (true, None); // Found but deleted
                    }
                    return (true, Some(value)); // Found with value
                }
                // If user_key doesn't match, we've gone past this key
                if internal_key.user_key() > key {
                    break;
                }
            }
        }
        (false, None) // Not found
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.approximate_memory.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Collect all unique user keys with their latest values (for flushing to SSTable)
    pub fn collect_entries(&self) -> Vec<(Slice, Slice)> {
        let mut result = Vec::new();
        let mut last_user_key: Option<Slice> = None;

        // Iterate through all entries in the SkipList (sorted by internal key)
        let iter = self.table.iter();
        let all_entries = iter.range_from(&Slice::empty());

        for (internal_key_data, value) in all_entries {
            if let Ok(internal_key) = InternalKey::decode(&internal_key_data) {
                let user_key = internal_key.user_key().clone();

                // Skip if we've already seen this user_key (we want the first/latest entry due to reverse sequence)
                if let Some(ref last) = last_user_key
                    && last == &user_key
                {
                    continue;
                }

                // Only add non-deletion entries
                if !internal_key.is_deletion() {
                    result.push((user_key.clone(), value));
                }

                last_user_key = Some(user_key);
            }
        }

        result
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let memtable = MemTable::new();

        memtable.add(1, Slice::from("key1"), Slice::from("value1"));

        let (found, value) = memtable.get(&Slice::from("key1"));
        assert!(found);
        assert_eq!(value, Some(Slice::from("value1")));
    }

    #[test]
    fn test_memtable_delete() {
        let memtable = MemTable::new();

        memtable.add(1, Slice::from("key1"), Slice::from("value1"));
        memtable.delete(2, Slice::from("key1"));

        let (found, value) = memtable.get(&Slice::from("key1"));
        assert!(found);
        assert_eq!(value, None);
    }

    #[test]
    fn test_memtable_sequence() {
        let memtable = MemTable::new();

        memtable.add(1, Slice::from("key1"), Slice::from("value1"));
        memtable.add(2, Slice::from("key1"), Slice::from("value2"));

        let (found, value) = memtable.get(&Slice::from("key1"));
        assert!(found);
        assert_eq!(value, Some(Slice::from("value2")));
    }

    #[test]
    fn test_memtable_memory_usage() {
        let memtable = MemTable::new();
        assert_eq!(memtable.approximate_memory_usage(), 0);

        memtable.add(1, Slice::from("key1"), Slice::from("value1"));
        assert!(memtable.approximate_memory_usage() > 0);
    }

    #[test]
    fn test_internal_key_encode_decode() {
        let key = InternalKey::new(Slice::from("test_key"), 123, VALUE_TYPE_VALUE);
        let encoded = key.encode();
        let decoded = InternalKey::decode(&encoded).unwrap();

        assert_eq!(decoded.user_key(), &Slice::from("test_key"));
        assert_eq!(decoded.sequence, 123);
        assert_eq!(decoded.value_type, VALUE_TYPE_VALUE);
    }
}
