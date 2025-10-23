use crate::util::Slice;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

pub struct SkipList {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
}

impl SkipList {
    pub fn new() -> Self {
        SkipList {
            map: Arc::new(SkipMap::new()),
        }
    }

    pub fn insert(&self, key: Slice, value: Slice) {
        self.map.insert(key.data().to_vec(), value.data().to_vec());
    }

    pub fn get(&self, key: &Slice) -> Option<Slice> {
        self.map
            .get(key.data())
            .map(|entry| Slice::from(entry.value().clone()))
    }

    pub fn contains(&self, key: &Slice) -> bool {
        self.map.contains_key(key.data())
    }

    pub fn iter(&self) -> SkipListIterator {
        SkipListIterator {
            inner: self.map.clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SkipListIterator {
    inner: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
}

impl SkipListIterator {
    pub fn seek_to_first(&self) -> Option<(Slice, Slice)> {
        self.inner.front().map(|entry| {
            (
                Slice::from(entry.key().clone()),
                Slice::from(entry.value().clone()),
            )
        })
    }

    pub fn seek(&self, target: &Slice) -> Option<(Slice, Slice)> {
        self.inner
            .range(target.data().to_vec()..)
            .next()
            .map(|entry| {
                (
                    Slice::from(entry.key().clone()),
                    Slice::from(entry.value().clone()),
                )
            })
    }

    pub fn range_from(&self, start: &Slice) -> Vec<(Slice, Slice)> {
        self.inner
            .range(start.data().to_vec()..)
            .map(|entry| {
                (
                    Slice::from(entry.key().clone()),
                    Slice::from(entry.value().clone()),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skiplist_basic() {
        let list = SkipList::new();
        assert!(list.is_empty());

        list.insert(Slice::from("key1"), Slice::from("value1"));
        assert_eq!(list.len(), 1);
        assert!(list.contains(&Slice::from("key1")));

        let value = list.get(&Slice::from("key1"));
        assert_eq!(value, Some(Slice::from("value1")));
    }

    #[test]
    fn test_skiplist_multiple_inserts() {
        let list = SkipList::new();

        list.insert(Slice::from("key3"), Slice::from("value3"));
        list.insert(Slice::from("key1"), Slice::from("value1"));
        list.insert(Slice::from("key2"), Slice::from("value2"));

        assert_eq!(list.len(), 3);
        assert_eq!(list.get(&Slice::from("key1")), Some(Slice::from("value1")));
        assert_eq!(list.get(&Slice::from("key2")), Some(Slice::from("value2")));
        assert_eq!(list.get(&Slice::from("key3")), Some(Slice::from("value3")));
    }

    #[test]
    fn test_skiplist_iterator() {
        let list = SkipList::new();

        list.insert(Slice::from("key2"), Slice::from("value2"));
        list.insert(Slice::from("key1"), Slice::from("value1"));
        list.insert(Slice::from("key3"), Slice::from("value3"));

        let iter = list.iter();
        let first = iter.seek_to_first();
        assert!(first.is_some());

        let (key, value) = first.unwrap();
        assert_eq!(key, Slice::from("key1"));
        assert_eq!(value, Slice::from("value1"));
    }

    #[test]
    fn test_skiplist_seek() {
        let list = SkipList::new();

        list.insert(Slice::from("key1"), Slice::from("value1"));
        list.insert(Slice::from("key3"), Slice::from("value3"));
        list.insert(Slice::from("key5"), Slice::from("value5"));

        let iter = list.iter();
        let result = iter.seek(&Slice::from("key2"));
        assert!(result.is_some());

        let (key, _) = result.unwrap();
        assert_eq!(key, Slice::from("key3"));
    }
}
