use std::{collections::HashMap, hash::Hash, sync::Arc};

use parking_lot::Mutex;

/// A custom LRU (Least Recently Used) cache implementation.
///
/// This implementation uses a HashMap for O(1) lookups and a doubly-linked list
/// to maintain the access order. The most recently used items are at the front
/// of the list, and the least recently used items are at the back.
///
/// # Architecture
/// - HashMap: Maps keys to node indices for O(1) access
/// - Vec<Node>: Stores all nodes (doubly-linked list nodes)
/// - head/tail: Indices pointing to the front and back of the list
/// - free_list: Reuses node slots when items are evicted
///
/// This design demonstrates how to build efficient data structures by
/// combining multiple primitives (HashMap + custom linked list).
pub struct LRUCache<K: Clone + Eq + Hash, V: Clone> {
    cache: Arc<Mutex<LRUCacheInner<K, V>>>,
    capacity: usize,
}

struct LRUCacheInner<K: Clone + Eq + Hash, V: Clone> {
    /// Maps keys to node indices in the nodes vector
    map: HashMap<K, usize>,
    /// Stores all nodes (doubly-linked list)
    nodes: Vec<Node<K, V>>,
    /// Index of the most recently used node (front of list)
    head: Option<usize>,
    /// Index of the least recently used node (back of list)
    tail: Option<usize>,
    /// Reusable node indices from evicted/removed entries
    free_list: Vec<usize>,
    /// Maximum number of entries
    capacity: usize,
    /// Statistics
    hits: u64,
    misses: u64,
}

/// A node in the doubly-linked list
struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

impl<K: Clone + Eq + Hash, V: Clone> LRUCache<K, V> {
    /// Creates a new LRU cache with the specified capacity.
    ///
    /// If capacity is 0, the cache is disabled and all operations are no-ops.
    pub fn new(capacity: usize) -> Self {
        LRUCache {
            capacity,
            cache: Arc::new(Mutex::new(LRUCacheInner {
                map: HashMap::new(),
                nodes: Vec::new(),
                head: None,
                tail: None,
                free_list: Vec::new(),
                capacity,
                hits: 0,
                misses: 0,
            })),
        }
    }

    /// Gets a value from the cache, updating its position to most recently
    /// used.
    ///
    /// Returns None if the key is not found or if the cache is disabled.
    pub fn get(&self, key: &K) -> Option<V> {
        let mut inner = self.cache.lock();

        if inner.capacity == 0 {
            inner.misses += 1;
            return None;
        }

        if let Some(&node_idx) = inner.map.get(key) {
            inner.hits += 1;
            let value = inner.nodes[node_idx].value.clone();
            // Move to front (most recently used)
            inner.move_to_front(node_idx);
            Some(value)
        } else {
            inner.misses += 1;
            None
        }
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, its value is updated and it becomes the most
    /// recently used item. If the cache is at capacity, the least recently used
    /// item is evicted.
    pub fn insert(&self, key: K, value: V) {
        let mut inner = self.cache.lock();

        if inner.capacity == 0 {
            return;
        }

        // If key exists, update value and move to front
        if let Some(&node_idx) = inner.map.get(&key) {
            inner.nodes[node_idx].value = value;
            inner.move_to_front(node_idx);
            return;
        }

        // Need to insert new node
        // First, evict LRU if at capacity
        if inner.map.len() >= inner.capacity {
            inner.evict_lru();
        }

        // Create new node
        let node_idx = inner.allocate_node(key.clone(), value);

        // Add to map
        inner.map.insert(key, node_idx);

        // Add to front of list
        inner.push_front(node_idx);
    }

    /// Clears all entries from the cache.
    pub fn clear(&self) {
        let mut inner = self.cache.lock();
        inner.map.clear();
        inner.nodes.clear();
        inner.free_list.clear();
        inner.head = None;
        inner.tail = None;
    }

    /// Returns cache statistics including hit rate and size.
    pub fn stats(&self) -> CacheStats {
        let inner = self.cache.lock();
        CacheStats {
            hits: inner.hits,
            misses: inner.misses,
            entries: inner.map.len(),
            capacity: inner.capacity,
        }
    }

    /// Returns the current number of entries in the cache.
    pub fn len(&self) -> usize {
        let inner = self.cache.lock();
        inner.map.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        let inner = self.cache.lock();
        inner.map.is_empty()
    }
}

impl<K: Clone + Eq + Hash, V: Clone> LRUCacheInner<K, V> {
    /// Allocates a new node, reusing from free list if available.
    ///
    /// # Memory Management Strategy
    /// When an entry is evicted from the cache, instead of deallocating the
    /// node, we add its index to the free_list. Future allocations first
    /// check the free_list before growing the nodes vector.
    ///
    /// This reduces allocation overhead and keeps node indices stable, which
    /// is important for the HashMap lookups.
    ///
    /// # Returns
    /// The index of the allocated node in the nodes vector.
    fn allocate_node(&mut self, key: K, value: V) -> usize {
        if let Some(idx) = self.free_list.pop() {
            // Reuse a previously freed node
            self.nodes[idx] = Node {
                key,
                value,
                prev: None,
                next: None,
            };
            idx
        } else {
            // No free nodes available, grow the vector
            let idx = self.nodes.len();
            self.nodes.push(Node {
                key,
                value,
                prev: None,
                next: None,
            });
            idx
        }
    }

    /// Moves a node to the front of the list (most recently used position)
    fn move_to_front(&mut self, node_idx: usize) {
        if self.head == Some(node_idx) {
            // Already at front
            return;
        }

        // Remove from current position
        self.unlink(node_idx);

        // Add to front
        self.push_front(node_idx);
    }

    /// Removes a node from its current position in the doubly-linked list.
    ///
    /// # Doubly-Linked List Operation
    /// This function updates the prev/next pointers of adjacent nodes to
    /// maintain list consistency. It handles three cases:
    ///
    /// 1. **Node is head**: Update head pointer to next node
    /// 2. **Node is tail**: Update tail pointer to previous node
    /// 3. **Node is middle**: Update adjacent nodes' pointers
    ///
    /// After unlinking, the node is disconnected from the list but still
    /// exists in the nodes vector.
    fn unlink(&mut self, node_idx: usize) {
        let node = &self.nodes[node_idx];
        let prev = node.prev;
        let next = node.next;

        // Update previous node's next pointer (or head if this is first node)
        if let Some(prev_idx) = prev {
            self.nodes[prev_idx].next = next;
        } else {
            // This was the head
            self.head = next;
        }

        // Update next node's prev pointer (or tail if this is last node)
        if let Some(next_idx) = next {
            self.nodes[next_idx].prev = prev;
        } else {
            // This was the tail
            self.tail = prev;
        }
    }

    /// Adds a node to the front of the list (most recently used position).
    ///
    /// # List Structure
    /// The doubly-linked list maintains LRU order:
    /// - **Head**: Most recently used (newest)
    /// - **Tail**: Least recently used (oldest, will be evicted first)
    ///
    /// This operation:
    /// 1. Sets node's prev to None (it becomes the new head)
    /// 2. Sets node's next to current head
    /// 3. Updates old head's prev to point to this node
    /// 4. Updates head pointer to this node
    /// 5. If list was empty, also sets tail pointer
    fn push_front(&mut self, node_idx: usize) {
        self.nodes[node_idx].prev = None;
        self.nodes[node_idx].next = self.head;

        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(node_idx);
        }

        self.head = Some(node_idx);

        if self.tail.is_none() {
            self.tail = Some(node_idx);
        }
    }

    /// Evicts the least recently used entry (tail of the list)
    fn evict_lru(&mut self) {
        if let Some(tail_idx) = self.tail {
            let key = self.nodes[tail_idx].key.clone();
            self.map.remove(&key);
            self.unlink(tail_idx);
            self.free_list.push(tail_idx);
        }
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
    /// Returns the cache hit rate as a value between 0.0 and 1.0
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

    #[test]
    fn test_lru_cache_node_reuse() {
        let cache = LRUCache::new(2);

        // Fill cache
        cache.insert(1, "a");
        cache.insert(2, "b");

        // Evict and add new items multiple times
        cache.insert(3, "c"); // evicts 1
        cache.insert(4, "d"); // evicts 2
        cache.insert(5, "e"); // evicts 3

        // Verify only latest 2 items exist
        assert_eq!(cache.get(&3), None);
        assert_eq!(cache.get(&4), Some("d"));
        assert_eq!(cache.get(&5), Some("e"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_cache_large_capacity() {
        let cache = LRUCache::new(100);

        // Insert 100 items
        for i in 0..100 {
            cache.insert(i, i * 10);
        }

        // All should be accessible
        for i in 0..100 {
            assert_eq!(cache.get(&i), Some(i * 10));
        }

        // Insert one more, should evict first
        cache.insert(100, 1000);
        assert_eq!(cache.get(&0), None);
        assert_eq!(cache.get(&100), Some(1000));
    }
}
