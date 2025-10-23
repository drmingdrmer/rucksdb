/// Iterator module for RucksDB
///
/// Provides a unified iterator interface for traversing key-value pairs across:
/// - MemTable (in-memory data)
/// - Immutable MemTable (pending flush)
/// - SSTables (on-disk data)
///
/// # Architecture
///
/// The iterator system uses a **merge iterator pattern** to combine multiple
/// data sources while maintaining sorted order and handling deletions.
///
/// ```text
/// DB::iter()
///     ↓
/// MergingIterator
///     ├─→ MemTableIterator (newest)
///     ├─→ ImmutableMemTableIterator
///     └─→ [TableIterator, TableIterator, ...] (oldest)
/// ```
///
/// ## Key Design Principles
///
/// 1. **Newest Wins**: When multiple sources have the same key, use the newest
/// 2. **Deletion Markers**: Skip entries marked as deleted
/// 3. **Lazy Evaluation**: Only read data when iterator advances
/// 4. **Bidirectional**: Support both forward and backward iteration
///
/// ## Performance Considerations
///
/// - Min-heap for efficient multi-way merge (O(log N) per next())
/// - Block-level caching in TableIterator
/// - Seek operations use binary search where possible
use crate::util::{Result, Slice};

/// Iterator trait for traversing key-value pairs in sorted order
///
/// # Lifecycle
///
/// An iterator starts in an invalid state. Call one of the seek methods
/// to position it:
///
/// ```ignore
/// let mut iter = db.iter();
/// iter.seek_to_first()?;  // Position at first key
/// while iter.valid() {
///     println!("{:?}: {:?}", iter.key(), iter.value());
///     iter.next()?;
/// }
/// ```
///
/// # Error Handling
///
/// Iterator operations return `Result` to handle I/O errors during SSTable
/// reads. The iterator becomes invalid on error.
pub trait Iterator {
    /// Position at the first key in the source
    ///
    /// Returns Ok(true) if positioned, Ok(false) if source is empty
    fn seek_to_first(&mut self) -> Result<bool>;

    /// Position at the last key in the source
    ///
    /// Returns Ok(true) if positioned, Ok(false) if source is empty
    fn seek_to_last(&mut self) -> Result<bool>;

    /// Position at the first key >= target
    ///
    /// If no such key exists, iterator becomes invalid.
    /// Returns Ok(true) if positioned, Ok(false) if not found
    fn seek(&mut self, target: &Slice) -> Result<bool>;

    /// Position at the last key <= target
    ///
    /// If no such key exists, iterator becomes invalid.
    /// Returns Ok(true) if positioned, Ok(false) if not found
    fn seek_for_prev(&mut self, target: &Slice) -> Result<bool>;

    /// Move to the next entry
    ///
    /// Prerequisite: valid() == true
    /// Returns Ok(true) if moved, Ok(false) if reached end
    fn next(&mut self) -> Result<bool>;

    /// Move to the previous entry
    ///
    /// Prerequisite: valid() == true
    /// Returns Ok(true) if moved, Ok(false) if reached beginning
    fn prev(&mut self) -> Result<bool>;

    /// Get current key
    ///
    /// Prerequisite: valid() == true
    fn key(&self) -> Slice;

    /// Get current value
    ///
    /// Prerequisite: valid() == true
    fn value(&self) -> Slice;

    /// Check if iterator is positioned at a valid entry
    ///
    /// Returns false if:
    /// - Iterator hasn't been positioned yet
    /// - Iterator has reached the end
    /// - An error occurred
    fn valid(&self) -> bool;

    /// Check if current entry is a deletion marker
    ///
    /// Prerequisite: valid() == true
    /// Returns true if current entry represents a deleted key
    /// Default implementation returns false (no deletions)
    fn is_deletion(&self) -> bool {
        false
    }
}

mod memtable_iterator;
mod merging_iterator;
mod table_iterator;

pub use memtable_iterator::MemTableIterator;
pub use merging_iterator::MergingIterator;
pub use table_iterator::TableIterator;
