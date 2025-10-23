use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use crate::{
    iterator::Iterator,
    memtable::memtable::InternalKey,
    util::{Result, Slice, Status},
};

/// Iterator for MemTable
///
/// Wraps the SkipList iterator and handles:
/// - InternalKey decoding
/// - Deletion marker detection (exposed via is_deletion())
/// - User key extraction
///
/// # Implementation Notes
///
/// The crossbeam_skiplist iterator is used indirectly through range queries.
/// We maintain the current position and use range() to get the next/previous
/// elements.
///
/// Deletion markers are exposed to allow proper merging with other iterators.
/// The MergingIterator will filter them from final results.
pub struct MemTableIterator {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
    current_is_deletion: bool,
    valid: bool,
}

impl MemTableIterator {
    pub fn new(map: Arc<SkipMap<Vec<u8>, Vec<u8>>>) -> Self {
        MemTableIterator {
            map,
            current_key: None,
            current_value: None,
            current_is_deletion: false,
            valid: false,
        }
    }

    /// Advance to next entry (including deletion markers)
    fn advance_forward(&mut self) -> Result<bool> {
        let start_key = if let Some(ref key) = self.current_key {
            // Find next key after current
            let mut next_key = key.clone();
            // Increment to get strictly greater key
            next_key.push(0);
            next_key
        } else {
            // Start from beginning
            vec![]
        };

        // Get next entry from SkipMap
        if let Some(entry) = self.map.range(start_key..).next() {
            let internal_slice = Slice::from(entry.key().clone());
            let internal_key = InternalKey::decode(&internal_slice)?;

            // Store entry (including deletion markers)
            self.current_key = Some(entry.key().clone());
            self.current_value = Some(entry.value().clone());
            self.current_is_deletion = internal_key.is_deletion();
            self.valid = true;
            Ok(true)
        } else {
            // Reached end
            self.valid = false;
            Ok(false)
        }
    }

    /// Move to previous entry (including deletion markers)
    fn advance_backward(&mut self) -> Result<bool> {
        if let Some(ref key) = self.current_key {
            // Need to scan from beginning to find previous
            // This is inefficient but crossbeam_skiplist doesn't support reverse iteration
            let mut last_valid: Option<(Vec<u8>, Vec<u8>, bool)> = None;

            for entry in self.map.iter() {
                if entry.key() >= key {
                    break;
                }

                let internal_slice = Slice::from(entry.key().clone());
                let internal_key = InternalKey::decode(&internal_slice)?;

                // Store all entries including deletion markers
                last_valid = Some((
                    entry.key().clone(),
                    entry.value().clone(),
                    internal_key.is_deletion(),
                ));
            }

            if let Some((key, value, is_deletion)) = last_valid {
                self.current_key = Some(key);
                self.current_value = Some(value);
                self.current_is_deletion = is_deletion;
                self.valid = true;
                Ok(true)
            } else {
                self.valid = false;
                Ok(false)
            }
        } else {
            // Not positioned, can't go backward
            self.valid = false;
            Ok(false)
        }
    }

    /// Extract user key from current internal key
    fn extract_user_key(&self) -> Result<Slice> {
        if let Some(ref key) = self.current_key {
            let internal_slice = Slice::from(key.clone());
            let internal_key = InternalKey::decode(&internal_slice)?;
            Ok(internal_key.user_key().clone())
        } else {
            Err(Status::invalid_argument("Iterator not positioned"))
        }
    }
}

impl Iterator for MemTableIterator {
    fn seek_to_first(&mut self) -> Result<bool> {
        self.current_key = None;
        self.current_value = None;
        self.valid = false;
        self.advance_forward()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        // Find last entry (including deletion markers)
        let mut last_entry: Option<(Vec<u8>, Vec<u8>, bool)> = None;

        for entry in self.map.iter() {
            let internal_slice = Slice::from(entry.key().clone());
            let internal_key = InternalKey::decode(&internal_slice)?;

            // Store all entries including deletion markers
            last_entry = Some((
                entry.key().clone(),
                entry.value().clone(),
                internal_key.is_deletion(),
            ));
        }

        if let Some((key, value, is_deletion)) = last_entry {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.current_is_deletion = is_deletion;
            self.valid = true;
            Ok(true)
        } else {
            self.valid = false;
            Ok(false)
        }
    }

    fn seek(&mut self, target: &Slice) -> Result<bool> {
        // Encode target as internal key for searching
        // We use a high sequence number to match any version
        let target_internal = InternalKey::new(target.clone(), u64::MAX, 1);
        let target_encoded = target_internal.encode();

        self.current_key = None;
        self.current_value = None;
        self.current_is_deletion = false;
        self.valid = false;

        // Find first key >= target (including deletion markers)
        for entry in self.map.range(target_encoded.data().to_vec()..) {
            let internal_slice = Slice::from(entry.key().clone());
            let internal_key = InternalKey::decode(&internal_slice)?;

            // Check if user key matches or is greater
            if internal_key.user_key() >= target {
                self.current_key = Some(entry.key().clone());
                self.current_value = Some(entry.value().clone());
                self.current_is_deletion = internal_key.is_deletion();
                self.valid = true;
                return Ok(true);
            }
        }

        self.valid = false;
        Ok(false)
    }

    fn seek_for_prev(&mut self, target: &Slice) -> Result<bool> {
        // Find last key <= target (including deletion markers)
        let mut last_entry: Option<(Vec<u8>, Vec<u8>, bool)> = None;

        for entry in self.map.iter() {
            let internal_slice = Slice::from(entry.key().clone());
            let internal_key = InternalKey::decode(&internal_slice)?;

            if internal_key.user_key() > target {
                break;
            }

            // Store all entries including deletion markers
            last_entry = Some((
                entry.key().clone(),
                entry.value().clone(),
                internal_key.is_deletion(),
            ));
        }

        if let Some((key, value, is_deletion)) = last_entry {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.current_is_deletion = is_deletion;
            self.valid = true;
            Ok(true)
        } else {
            self.valid = false;
            Ok(false)
        }
    }

    fn next(&mut self) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }
        self.advance_forward()
    }

    fn prev(&mut self) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }
        self.advance_backward()
    }

    fn key(&self) -> Slice {
        self.extract_user_key().unwrap_or_else(|_| Slice::empty())
    }

    fn value(&self) -> Slice {
        self.current_value
            .as_ref()
            .map(|v| Slice::from(v.clone()))
            .unwrap_or_else(Slice::empty)
    }

    fn valid(&self) -> bool {
        self.valid
    }

    fn is_deletion(&self) -> bool {
        self.current_is_deletion
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::MemTable;

    #[test]
    fn test_memtable_iterator_basic() {
        let mem = MemTable::new();
        mem.add(1, Slice::from("key1"), Slice::from("value1"));
        mem.add(2, Slice::from("key2"), Slice::from("value2"));
        mem.add(3, Slice::from("key3"), Slice::from("value3"));

        let mut iter = mem.iter();
        assert!(iter.seek_to_first().unwrap());
        assert!(iter.valid());
        assert_eq!(iter.key(), Slice::from("key1"));
        assert_eq!(iter.value(), Slice::from("value1"));

        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), Slice::from("key2"));

        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), Slice::from("key3"));

        assert!(!iter.next().unwrap());
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_iterator_seek() {
        let mem = MemTable::new();
        mem.add(1, Slice::from("key1"), Slice::from("value1"));
        mem.add(2, Slice::from("key3"), Slice::from("value3"));
        mem.add(3, Slice::from("key5"), Slice::from("value5"));

        let mut iter = mem.iter();

        // Seek to exact key
        assert!(iter.seek(&Slice::from("key3")).unwrap());
        assert_eq!(iter.key(), Slice::from("key3"));

        // Seek to key between entries
        assert!(iter.seek(&Slice::from("key2")).unwrap());
        assert_eq!(iter.key(), Slice::from("key3"));

        // Seek past all keys
        assert!(!iter.seek(&Slice::from("key9")).unwrap());
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_iterator_with_deletions() {
        let mem = MemTable::new();
        mem.add(1, Slice::from("key1"), Slice::from("value1"));
        mem.delete(2, Slice::from("key2"));
        mem.add(3, Slice::from("key3"), Slice::from("value3"));

        let mut iter = mem.iter();
        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), Slice::from("key1"));
        assert!(!iter.is_deletion());

        // MemTableIterator now exposes deletion markers
        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), Slice::from("key2"));
        assert!(iter.is_deletion(), "key2 should be a deletion marker");

        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), Slice::from("key3"));
        assert!(!iter.is_deletion());

        assert!(!iter.next().unwrap());
    }
}
