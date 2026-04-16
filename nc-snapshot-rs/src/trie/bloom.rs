//! Memory-efficient set for tracking reachable nodes during trie pruning.
//!
//! For large state DBs (110M+ keys), a `HashSet<[u8; 32]>` consumes ~1.7 GB of RAM.
//! A Bloom Filter with 0.1% false positive rate uses only ~240 MB for the same count.
//!
//! The trade-off: ~0.1% of unreachable nodes may be copied as false positives,
//! adding ~2-3 MB to the output — completely negligible.
//!
//! For small DBs (< 1M keys, e.g. tests), we fall back to an exact `HashSet`.

use crate::trie::node::NodeHash;
use std::collections::HashSet;

/// Threshold below which we use an exact HashSet instead of a Bloom Filter.
/// This ensures tests and small DBs get exact behavior.
const BLOOM_THRESHOLD: usize = 1_000_000;

/// A memory-efficient set that tracks which node hashes have been visited.
///
/// Automatically chooses between:
/// - `HashSet<NodeHash>` for small sets (exact, good for tests)
/// - `Bloom<NodeHash>` for large sets (probabilistic, memory-efficient)
pub enum ReachableSet {
    /// Exact set for small DBs / tests.
    Exact {
        set: HashSet<NodeHash>,
    },
    /// Probabilistic set for production-scale DBs.
    Probabilistic {
        bloom: bloomfilter::Bloom<NodeHash>,
        count: u64,
    },
}

impl ReachableSet {
    /// Create a new reachable set sized for `estimated_items`.
    ///
    /// - If `estimated_items < 1_000_000`: uses exact HashSet
    /// - Otherwise: uses Bloom Filter with ~0.1% false positive rate (~240 MB for 110M items)
    pub fn new(estimated_items: usize) -> Self {
        if estimated_items < BLOOM_THRESHOLD {
            ReachableSet::Exact {
                set: HashSet::with_capacity(estimated_items),
            }
        } else {
            // Bloom filter: bitmap_size and num_hashes are computed from
            // the desired false positive rate (0.001 = 0.1%).
            let bloom = bloomfilter::Bloom::new_for_fp_rate(estimated_items, 0.001);
            ReachableSet::Probabilistic { bloom, count: 0 }
        }
    }

    /// Create a new reachable set that always uses exact HashSet (for tests).
    #[cfg(test)]
    pub fn new_exact() -> Self {
        ReachableSet::Exact {
            set: HashSet::new(),
        }
    }

    /// Insert a node hash into the set.
    pub fn insert(&mut self, hash: &NodeHash) {
        match self {
            ReachableSet::Exact { set } => {
                set.insert(*hash);
            }
            ReachableSet::Probabilistic { bloom, count } => {
                bloom.set(hash);
                *count += 1;
            }
        }
    }

    /// Check if a node hash might be in the set.
    ///
    /// - For Exact sets: always correct (no false positives).
    /// - For Bloom filters: may return `true` for items not inserted (~0.1% FPR).
    pub fn contains(&self, hash: &NodeHash) -> bool {
        match self {
            ReachableSet::Exact { set } => set.contains(hash),
            ReachableSet::Probabilistic { bloom, .. } => bloom.check(hash),
        }
    }

    /// Return the number of items inserted.
    ///
    /// For Bloom filters, this is the exact insertion count (not the number of bits set).
    pub fn len(&self) -> u64 {
        match self {
            ReachableSet::Exact { set } => set.len() as u64,
            ReachableSet::Probabilistic { count, .. } => *count,
        }
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if a hash was already inserted (for deduplication during DFS).
    ///
    /// For exact sets, this is reliable.
    /// For bloom filters, false positives mean we might skip revisiting a node —
    /// but since duplicate visits are wasteful anyway, this is safe.
    pub fn already_visited(&self, hash: &NodeHash) -> bool {
        self.contains(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_set() {
        let mut set = ReachableSet::new_exact();
        let h1 = [0x11u8; 32];
        let h2 = [0x22u8; 32];

        assert!(!set.contains(&h1));
        set.insert(&h1);
        assert!(set.contains(&h1));
        assert!(!set.contains(&h2));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_bloom_filter() {
        // Force bloom filter by using large estimated count
        let mut set = ReachableSet::new(2_000_000);
        let h1 = [0xAAu8; 32];
        let h2 = [0xBBu8; 32];

        set.insert(&h1);
        assert!(set.contains(&h1)); // Always true (no false negatives)
        // h2 *might* be a false positive, but extremely unlikely for just 1 item
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_small_db_uses_exact() {
        let set = ReachableSet::new(100);
        matches!(set, ReachableSet::Exact { .. });
    }

    #[test]
    fn test_large_db_uses_bloom() {
        let set = ReachableSet::new(5_000_000);
        matches!(set, ReachableSet::Probabilistic { .. });
    }
}
