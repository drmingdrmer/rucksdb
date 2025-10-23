use parking_lot::Mutex;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// LRU Cache with fixed capacity
/// Uses the lru crate which implements O(1) get/insert/eviction
pub struct LRUCache<K: Clone + Eq + Hash, V: Clone> {
    cache: Arc<Mutex<LRUCacheInner<K, V>>>,
    capacity: usize,
}

struct LRUCacheInner<K: Clone + Eq + Hash, V: Clone> {
    lru: Option<lru::LruCache<K, V>>, // None means cache is disabled
    hits: u64,
    misses: u64,
}

impl<K: Clone + Eq + Hash, V: Clone> LRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        let lru = if capacity > 0 {
            Some(lru::LruCache::new(NonZeroUsize::new(capacity).unwrap()))
        } else {
            None
        };

        LRUCache {
            capacity,
            cache: Arc::new(Mutex::new(LRUCacheInner {
                lru,
                hits: 0,
                misses: 0,
            })),
        }
    }

    /// Get a value from the cache
    pub fn get(&self, key: &K) -> Option<V> {
        let mut inner = self.cache.lock();

        let result = inner.lru.as_mut().and_then(|lru| lru.get(key).cloned());
        if result.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        result
    }

    /// Insert a value into the cache
    pub fn insert(&self, key: K, value: V) {
        let mut inner = self.cache.lock();
        if let Some(lru) = &mut inner.lru {
            lru.put(key, value);
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        let mut inner = self.cache.lock();
        if let Some(lru) = &mut inner.lru {
            lru.clear();
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let inner = self.cache.lock();
        let (entries, capacity) = if let Some(lru) = &inner.lru {
            (lru.len(), lru.cap().get())
        } else {
            (0, 0)
        };

        CacheStats {
            hits: inner.hits,
            misses: inner.misses,
            entries,
            capacity,
        }
    }

    /// Get current size
    pub fn len(&self) -> usize {
        let inner = self.cache.lock();
        inner.lru.as_ref().map_or(0, |lru| lru.len())
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        let inner = self.cache.lock();
        inner.lru.as_ref().is_none_or(|lru| lru.is_empty())
    }
}

impl<K: Clone + Eq + Hash, V: Clone> Clone for LRUCache<K, V> {
    fn clone(&self) -> Self {
        LRUCache {
            capacity: self.capacity,
            cache: Arc::clone(&self.cache),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub capacity: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let cache = LRUCache::new(2);

        cache.insert("key1", "value1");
        cache.insert("key2", "value2");

        assert_eq!(cache.get(&"key1"), Some("value1"));
        assert_eq!(cache.get(&"key2"), Some("value2"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let cache = LRUCache::new(2);

        cache.insert("key1", "value1");
        cache.insert("key2", "value2");
        cache.insert("key3", "value3"); // Should evict key1

        assert_eq!(cache.get(&"key1"), None);
        assert_eq!(cache.get(&"key2"), Some("value2"));
        assert_eq!(cache.get(&"key3"), Some("value3"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_cache_update_order() {
        let cache = LRUCache::new(2);

        cache.insert("key1", "value1");
        cache.insert("key2", "value2");

        // Access key1 to make it more recent
        assert_eq!(cache.get(&"key1"), Some("value1"));

        // Insert key3, should evict key2 (LRU)
        cache.insert("key3", "value3");

        assert_eq!(cache.get(&"key1"), Some("value1"));
        assert_eq!(cache.get(&"key2"), None);
        assert_eq!(cache.get(&"key3"), Some("value3"));
    }

    #[test]
    fn test_lru_cache_overwrite() {
        let cache = LRUCache::new(2);

        cache.insert("key1", "value1");
        cache.insert("key1", "value2");

        assert_eq!(cache.get(&"key1"), Some("value2"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_lru_cache_stats() {
        let cache = LRUCache::new(2);

        cache.insert("key1", "value1");

        cache.get(&"key1"); // hit
        cache.get(&"key2"); // miss
        cache.get(&"key1"); // hit

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
        assert!((stats.hit_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_lru_cache_clear() {
        let cache = LRUCache::new(2);

        cache.insert("key1", "value1");
        cache.insert("key2", "value2");

        assert_eq!(cache.len(), 2);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get(&"key1"), None);
    }

    #[test]
    fn test_lru_cache_disabled() {
        let cache = LRUCache::new(0);

        // Insert should be no-op
        cache.insert("key1", "value1");
        assert_eq!(cache.get(&"key1"), None);
        assert_eq!(cache.len(), 0);

        let stats = cache.stats();
        assert_eq!(stats.capacity, 0);
    }
}
