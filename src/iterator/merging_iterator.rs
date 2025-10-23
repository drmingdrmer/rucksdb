use crate::{
    iterator::Iterator,
    util::{Result, Slice},
};

/// Merging iterator for combining multiple iterators
///
/// Uses a min-heap to efficiently merge multiple sorted iterators.
/// This will be implemented in the next step.
pub struct MergingIterator {
    // TODO: Implementation
}

// Placeholder - will be implemented in Part 3

impl Iterator for MergingIterator {
    fn seek_to_first(&mut self) -> Result<bool> {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn seek(&mut self, _target: &Slice) -> Result<bool> {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn seek_for_prev(&mut self, _target: &Slice) -> Result<bool> {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn next(&mut self) -> Result<bool> {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn prev(&mut self) -> Result<bool> {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn key(&self) -> Slice {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn value(&self) -> Slice {
        unimplemented!("MergingIterator not yet implemented")
    }

    fn valid(&self) -> bool {
        false
    }
}
