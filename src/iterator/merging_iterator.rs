use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    iterator::Iterator,
    util::{Result, Slice},
};

/// Merging iterator for combining multiple sorted iterators
///
/// Uses a min-heap to efficiently merge multiple sorted child iterators,
/// always returning the smallest key across all sources. This is the core
/// of LSM-tree query processing, combining data from:
/// - MemTable (most recent writes)
/// - Immutable MemTable (being flushed)
/// - Multiple SSTable files (older data)
///
/// # Architecture
///
/// ```text
/// MergingIterator
///     ├─→ Min-Heap of (key, iterator_index)
///     ├─→ Vec<Box<dyn Iterator>> (child iterators)
///     └─→ Current key/value (from smallest iterator)
/// ```
///
/// # Priority Rules
///
/// When multiple iterators have the same key:
/// - Earlier iterators (lower index) have higher priority
/// - This ensures newer data (MemTable) shadows older data (SSTables)
/// - Index 0 = highest priority (active MemTable)
///
/// # Implementation Notes
///
/// - Uses BinaryHeap with reversed ordering for min-heap behavior
/// - Each heap entry stores (key, iterator_index) for tie-breaking
/// - Clones current key/value to avoid lifetime issues
/// - Forward iteration (next) is O(log k) where k = number of iterators
/// - Backward iteration (prev) is expensive - requires full re-scan
pub struct MergingIterator {
    iterators: Vec<Box<dyn Iterator>>,
    heap: BinaryHeap<HeapEntry>,
    current_key: Option<Slice>,
    current_value: Option<Slice>,
    valid: bool,
}

/// Entry in the min-heap, ordered by key (reversed) then by index (reversed)
struct HeapEntry {
    key: Slice,
    index: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (BinaryHeap is max-heap by default)
        // When keys are equal, prioritize lower index (earlier iterator)
        other
            .key
            .data()
            .cmp(self.key.data())
            .then_with(|| other.index.cmp(&self.index))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key.data() == other.key.data() && self.index == other.index
    }
}

impl MergingIterator {
    /// Create a new merging iterator from multiple child iterators
    ///
    /// # Priority Order
    /// - iterators[0] has highest priority (e.g., active MemTable)
    /// - iterators[n] has lowest priority (e.g., oldest SSTable)
    pub fn new(iterators: Vec<Box<dyn Iterator>>) -> Self {
        MergingIterator {
            iterators,
            heap: BinaryHeap::new(),
            current_key: None,
            current_value: None,
            valid: false,
        }
    }

    /// Rebuild heap with current positions of all valid iterators
    fn rebuild_heap(&mut self) {
        self.heap.clear();
        for (idx, iter) in self.iterators.iter().enumerate() {
            if iter.valid() {
                self.heap.push(HeapEntry {
                    key: iter.key(),
                    index: idx,
                });
            }
        }
    }

    /// Update current key/value from the top of the heap
    /// Skips deletion markers to only expose real values
    fn update_current(&mut self) -> Result<bool> {
        loop {
            if let Some(entry) = self.heap.peek() {
                let is_deletion = self.iterators[entry.index].is_deletion();

                // Skip deletion markers
                if is_deletion {
                    // Remove this deletion marker and ALL entries with same user key
                    let deleted_key = entry.key.clone();

                    // Find all iterators with this key
                    let entries_to_advance: Vec<usize> = self
                        .heap
                        .iter()
                        .filter(|e| e.key.data() == deleted_key.data())
                        .map(|e| e.index)
                        .collect();

                    for idx in entries_to_advance {
                        // Remove from heap
                        self.heap.retain(|e| e.index != idx);

                        // Keep advancing this iterator until user key changes
                        loop {
                            if !self.iterators[idx].next()? {
                                break; // Iterator exhausted
                            }

                            let current_key = self.iterators[idx].key();
                            if current_key.data() != deleted_key.data() {
                                // Different user key - add back to heap
                                self.heap.push(HeapEntry {
                                    key: current_key,
                                    index: idx,
                                });
                                break;
                            }
                            // Same user key - keep advancing to skip old
                            // versions
                        }
                    }
                    continue; // Check next entry
                }

                // Not a deletion - return this entry
                let idx = entry.index;
                self.current_key = Some(self.iterators[idx].key());
                self.current_value = Some(self.iterators[idx].value());
                self.valid = true;
                return Ok(true);
            } else {
                self.valid = false;
                return Ok(false);
            }
        }
    }
}

impl Iterator for MergingIterator {
    fn seek_to_first(&mut self) -> Result<bool> {
        // Position all child iterators at their first entry
        for iter in &mut self.iterators {
            iter.seek_to_first()?;
        }

        // Rebuild heap with all valid iterators
        self.rebuild_heap();

        // Set current to the minimum key
        self.update_current()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        // Position all child iterators at their last entry
        for iter in &mut self.iterators {
            iter.seek_to_last()?;
        }

        // Rebuild heap (will contain last keys, but we want the maximum)
        self.rebuild_heap();

        // For seek_to_last, we need the MAXIMUM key, not minimum
        // So we need to find the max across all valid iterators
        let mut max_key: Option<Slice> = None;
        let mut max_idx: Option<usize> = None;

        for (idx, iter) in self.iterators.iter().enumerate() {
            if iter.valid() {
                let key = iter.key();
                match &max_key {
                    None => {
                        max_key = Some(key);
                        max_idx = Some(idx);
                    },
                    Some(current_max) => {
                        if key.data() > current_max.data() {
                            max_key = Some(key);
                            max_idx = Some(idx);
                        } else if key.data() == current_max.data() && idx < max_idx.unwrap() {
                            // Equal keys: prefer lower index (higher priority)
                            max_idx = Some(idx);
                        }
                    },
                }
            }
        }

        if let Some(idx) = max_idx {
            self.current_key = Some(self.iterators[idx].key());
            self.current_value = Some(self.iterators[idx].value());
            self.valid = true;
            Ok(true)
        } else {
            self.valid = false;
            Ok(false)
        }
    }

    fn seek(&mut self, target: &Slice) -> Result<bool> {
        // Position all child iterators at target
        for iter in &mut self.iterators {
            iter.seek(target)?;
        }

        // Rebuild heap with all valid iterators
        self.rebuild_heap();

        // Set current to the minimum key >= target
        self.update_current()
    }

    fn seek_for_prev(&mut self, target: &Slice) -> Result<bool> {
        // Position all child iterators at or before target
        for iter in &mut self.iterators {
            iter.seek_for_prev(target)?;
        }

        // Find the maximum key <= target across all valid iterators
        let mut max_key: Option<Slice> = None;
        let mut max_idx: Option<usize> = None;

        for (idx, iter) in self.iterators.iter().enumerate() {
            if iter.valid() {
                let key = iter.key();
                match &max_key {
                    None => {
                        max_key = Some(key);
                        max_idx = Some(idx);
                    },
                    Some(current_max) => {
                        if key.data() > current_max.data() {
                            max_key = Some(key);
                            max_idx = Some(idx);
                        } else if key.data() == current_max.data() && idx < max_idx.unwrap() {
                            // Equal keys: prefer lower index (higher priority)
                            max_idx = Some(idx);
                        }
                    },
                }
            }
        }

        if let Some(idx) = max_idx {
            self.current_key = Some(self.iterators[idx].key());
            self.current_value = Some(self.iterators[idx].value());
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

        // Get current minimum and advance its iterator
        if let Some(entry) = self.heap.pop() {
            let current_key = entry.key.clone();

            // Advance the iterator that had the minimum key
            // Keep advancing until user key changes or iterator exhausted
            let idx = entry.index;
            loop {
                if !self.iterators[idx].next()? {
                    break; // Iterator exhausted
                }

                let new_key = self.iterators[idx].key();
                if new_key.data() != current_key.data() {
                    // Different user key - add back to heap
                    self.heap.push(HeapEntry {
                        key: new_key,
                        index: idx,
                    });
                    break;
                }
                // Same user key - keep advancing to skip old versions
            }

            // Skip all other iterators with the same key (lower priority duplicates)
            let entries_to_advance: Vec<usize> = self
                .heap
                .iter()
                .filter(|e| e.key.data() == current_key.data())
                .map(|e| e.index)
                .collect();

            for idx in entries_to_advance {
                // Remove from heap
                self.heap.retain(|e| e.index != idx);

                // Keep advancing this iterator until user key changes
                loop {
                    if !self.iterators[idx].next()? {
                        break; // Iterator exhausted
                    }

                    let new_key = self.iterators[idx].key();
                    if new_key.data() != current_key.data() {
                        // Different user key - add back to heap
                        self.heap.push(HeapEntry {
                            key: new_key,
                            index: idx,
                        });
                        break;
                    }
                    // Same user key - keep advancing to skip old versions
                }
            }

            // Update current from new heap top
            return self.update_current();
        }

        self.valid = false;
        Ok(false)
    }

    fn prev(&mut self) -> Result<bool> {
        // Backward iteration is expensive - need to restart from beginning
        // and scan to find the previous entry
        unimplemented!("MergingIterator::prev() not implemented - use seek_for_prev() instead")
    }

    fn key(&self) -> Slice {
        self.current_key.clone().unwrap_or_else(Slice::empty)
    }

    fn value(&self) -> Slice {
        self.current_value.clone().unwrap_or_else(Slice::empty)
    }

    fn valid(&self) -> bool {
        self.valid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::MemTable;

    #[test]
    fn test_merging_iterator_two_memtables() {
        // Create two memtables with non-overlapping keys
        let mt1 = MemTable::new();
        mt1.add(1, Slice::from("key1"), Slice::from("value1"));
        mt1.add(2, Slice::from("key3"), Slice::from("value3"));

        let mt2 = MemTable::new();
        mt2.add(1, Slice::from("key2"), Slice::from("value2"));
        mt2.add(2, Slice::from("key4"), Slice::from("value4"));

        let iter1: Box<dyn Iterator> = Box::new(mt1.iter());
        let iter2: Box<dyn Iterator> = Box::new(mt2.iter());

        let mut merge_iter = MergingIterator::new(vec![iter1, iter2]);
        assert!(merge_iter.seek_to_first().unwrap());

        // Should get keys in sorted order from both sources
        assert_eq!(merge_iter.key(), Slice::from("key1"));
        assert!(merge_iter.next().unwrap());
        assert_eq!(merge_iter.key(), Slice::from("key2"));
        assert!(merge_iter.next().unwrap());
        assert_eq!(merge_iter.key(), Slice::from("key3"));
        assert!(merge_iter.next().unwrap());
        assert_eq!(merge_iter.key(), Slice::from("key4"));
        assert!(!merge_iter.next().unwrap());
    }

    #[test]
    fn test_merging_iterator_priority() {
        // Create two memtables with overlapping keys
        // mt1 should shadow mt2 (higher priority)
        let mt1 = MemTable::new();
        mt1.add(2, Slice::from("key1"), Slice::from("value1_new"));

        let mt2 = MemTable::new();
        mt2.add(1, Slice::from("key1"), Slice::from("value1_old"));

        let iter1: Box<dyn Iterator> = Box::new(mt1.iter());
        let iter2: Box<dyn Iterator> = Box::new(mt2.iter());

        let mut merge_iter = MergingIterator::new(vec![iter1, iter2]);
        assert!(merge_iter.seek_to_first().unwrap());

        // Should get value from mt1 (higher priority)
        assert_eq!(merge_iter.key(), Slice::from("key1"));
        assert_eq!(merge_iter.value(), Slice::from("value1_new"));
        assert!(!merge_iter.next().unwrap());
    }

    #[test]
    fn test_merging_iterator_seek() {
        let mt1 = MemTable::new();
        mt1.add(1, Slice::from("key1"), Slice::from("value1"));
        mt1.add(2, Slice::from("key5"), Slice::from("value5"));

        let mt2 = MemTable::new();
        mt2.add(1, Slice::from("key3"), Slice::from("value3"));
        mt2.add(2, Slice::from("key7"), Slice::from("value7"));

        let iter1: Box<dyn Iterator> = Box::new(mt1.iter());
        let iter2: Box<dyn Iterator> = Box::new(mt2.iter());

        let mut merge_iter = MergingIterator::new(vec![iter1, iter2]);

        // Seek to key3
        assert!(merge_iter.seek(&Slice::from("key3")).unwrap());
        assert_eq!(merge_iter.key(), Slice::from("key3"));

        // Seek to non-existing key (should find next)
        assert!(merge_iter.seek(&Slice::from("key4")).unwrap());
        assert_eq!(merge_iter.key(), Slice::from("key5"));
    }
}
