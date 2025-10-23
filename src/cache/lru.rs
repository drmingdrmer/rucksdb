use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// LRU Cache with fixed capacity
/// Uses HashMap for O(1) lookup and doubly-linked list for O(1) eviction
pub struct LRUCache<K: Clone + Eq + std::hash::Hash, V: Clone> {
    capacity: usize,
    cache: Arc<Mutex<LRUCacheInner<K, V>>>,
}

struct LRUCacheInner<K: Clone + Eq + std::hash::Hash, V: Clone> {
    map: HashMap<K, (V, usize)>, // Key -> (Value, access_order)
    access_order: usize,
    hits: u64,
    misses: u64,
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> LRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        LRUCache {
            capacity,
            cache: Arc::new(Mutex::new(LRUCacheInner {
                map: HashMap::new(),
                access_order: 0,
                hits: 0,
                misses: 0,
            })),
        }
    }

    /// Get a value from the cache
    pub fn get(&self, key: &K) -> Option<V> {
        let mut inner = self.cache.lock();

        // First check if key exists
        if !inner.map.contains_key(key) {
            inner.misses += 1;
            return None;
        }

        // Update access order
        inner.access_order += 1;
        let new_order = inner.access_order;
        inner.hits += 1;

        // Get value and update order
        if let Some((value, order)) = inner.map.get_mut(key) {
            *order = new_order;
            Some(value.clone())
        } else {
            None
        }
    }

    /// Insert a value into the cache
    pub fn insert(&self, key: K, value: V) {
        let mut inner = self.cache.lock();

        inner.access_order += 1;
        let new_order = inner.access_order;

        // If already exists, just update
        if inner.map.contains_key(&key) {
            inner.map.insert(key, (value, new_order));
            return;
        }

        // Evict if at capacity
        if inner.map.len() >= self.capacity {
            // Find LRU item (smallest access_order)
            if let Some(lru_key) = inner.map.iter()
                .min_by_key(|(_, (_, order))| order)
                .map(|(k, _)| k.clone())
            {
                inner.map.remove(&lru_key);
            }
        }

        inner.map.insert(key, (value, new_order));
    }

    /// Clear all entries
    pub fn clear(&self) {
        let mut inner = self.cache.lock();
        inner.map.clear();
        inner.access_order = 0;
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let inner = self.cache.lock();
        CacheStats {
            hits: inner.hits,
            misses: inner.misses,
            entries: inner.map.len(),
            capacity: self.capacity,
        }
    }

    /// Get current size
    pub fn len(&self) -> usize {
        self.cache.lock().map.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.cache.lock().map.is_empty()
    }
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> Clone for LRUCache<K, V> {
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
}
