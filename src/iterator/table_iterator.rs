use crate::{
    iterator::Iterator,
    util::{Result, Slice},
};

/// Iterator for SSTable
///
/// Wraps BlockIterator and provides index block navigation.
/// This will be implemented in the next step.
pub struct TableIterator {
    // TODO: Implementation
}

// Placeholder - will be implemented in Part 2

impl Iterator for TableIterator {
    fn seek_to_first(&mut self) -> Result<bool> {
        unimplemented!("TableIterator not yet implemented")
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        unimplemented!("TableIterator not yet implemented")
    }

    fn seek(&mut self, _target: &Slice) -> Result<bool> {
        unimplemented!("TableIterator not yet implemented")
    }

    fn seek_for_prev(&mut self, _target: &Slice) -> Result<bool> {
        unimplemented!("TableIterator not yet implemented")
    }

    fn next(&mut self) -> Result<bool> {
        unimplemented!("TableIterator not yet implemented")
    }

    fn prev(&mut self) -> Result<bool> {
        unimplemented!("TableIterator not yet implemented")
    }

    fn key(&self) -> Slice {
        unimplemented!("TableIterator not yet implemented")
    }

    fn value(&self) -> Slice {
        unimplemented!("TableIterator not yet implemented")
    }

    fn valid(&self) -> bool {
        false
    }
}
