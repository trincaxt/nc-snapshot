//! Integration tests for the nc-snapshot pruner.
//!
//! These tests create realistic trie structures in temporary RocksDB instances
//! and verify that pruning correctly preserves reachable nodes and removes orphans.

use nc_snapshot_rs::trie::bencodex::{encode, BencodexValue};
use nc_snapshot_rs::trie::node::{sha256, NodeHash};
use nc_snapshot_rs::trie::pruner::prune_states;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};
use tempfile::TempDir;

/// Helper: store a Bencodex value in the DB under its SHA256 hash.
fn store_node(
    db: &DBWithThreadMode<MultiThreaded>,
    benc: &BencodexValue,
) -> NodeHash {
    let encoded = encode(benc);
    let hash = sha256(&encoded);
    db.put(&hash, &encoded).unwrap();
    hash
}

/// Create a realistic trie structure resembling Libplanet's states/.
///
/// Structure:
///   root (FullNode)
///     ├── [0] -> short_a (ShortNode) -> full_inner (FullNode)
///     │                                     ├── [3] -> leaf_1 (ValueNode)
///     │                                     └── [7] -> leaf_2 (ValueNode)
///     ├── [5] -> leaf_3 (ValueNode)
///     └── [15] -> short_b (ShortNode) -> leaf_4 (ValueNode)
///
///   orphan_1 (ValueNode) -- not reachable
///   orphan_2 (ShortNode -> orphan_3) -- not reachable
///   orphan_3 (ValueNode) -- not reachable
fn create_realistic_trie() -> (TempDir, NodeHash, usize, usize) {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("states");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DBWithThreadMode::<MultiThreaded>::open(&opts, &db_path).unwrap();

    // Leaf nodes (ValueNode)
    let leaf_1 = store_node(&db, &BencodexValue::Bytes(b"state_value_1".to_vec()));
    let leaf_2 = store_node(&db, &BencodexValue::Bytes(b"state_value_2".to_vec()));
    let leaf_3 = store_node(&db, &BencodexValue::Bytes(b"state_value_3".to_vec()));
    let leaf_4 = store_node(&db, &BencodexValue::Bytes(b"state_value_4_longer_data".to_vec()));

    // Inner full node
    let mut inner_items = vec![BencodexValue::Null; 17];
    inner_items[3] = BencodexValue::Bytes(leaf_1.to_vec());
    inner_items[7] = BencodexValue::Bytes(leaf_2.to_vec());
    let full_inner = store_node(&db, &BencodexValue::List(inner_items));

    // Short node A -> full_inner
    let short_a = store_node(
        &db,
        &BencodexValue::List(vec![
            BencodexValue::Bytes(vec![0x01, 0x23]),          // path nibbles
            BencodexValue::Bytes(full_inner.to_vec()),       // hash ref
        ]),
    );

    // Short node B -> leaf_4
    let short_b = store_node(
        &db,
        &BencodexValue::List(vec![
            BencodexValue::Bytes(vec![0xAB, 0xCD]),          // path nibbles
            BencodexValue::Bytes(leaf_4.to_vec()),           // hash ref
        ]),
    );

    // Root full node
    let mut root_items = vec![BencodexValue::Null; 17];
    root_items[0] = BencodexValue::Bytes(short_a.to_vec());
    root_items[5] = BencodexValue::Bytes(leaf_3.to_vec());
    root_items[15] = BencodexValue::Bytes(short_b.to_vec());
    let root = store_node(&db, &BencodexValue::List(root_items));

    // Orphan nodes (not reachable from root)
    let _orphan_1 = store_node(&db, &BencodexValue::Bytes(b"orphan_data_1".to_vec()));
    let orphan_3 = store_node(&db, &BencodexValue::Bytes(b"orphan_data_3".to_vec()));
    let _orphan_2 = store_node(
        &db,
        &BencodexValue::List(vec![
            BencodexValue::Bytes(vec![0xFF]),
            BencodexValue::Bytes(orphan_3.to_vec()),
        ]),
    );

    drop(db);

    // 8 reachable: root, short_a, full_inner, leaf_1, leaf_2, leaf_3, short_b, leaf_4
    // 3 orphans: orphan_1, orphan_2, orphan_3
    (tmp, root, 8, 3)
}

#[test]
fn test_full_prune_cycle() {
    let (tmp, root, expected_reachable, expected_orphans) = create_realistic_trie();
    let states_path = tmp.path().join("states");
    let target_path = tmp.path().join("states_pruned");

    let stats = prune_states(&states_path, &target_path, &[root], false).unwrap();

    assert_eq!(
        stats.reachable, expected_reachable as u64,
        "Unexpected number of reachable nodes"
    );
    assert_eq!(
        stats.deleted, expected_orphans as u64,
        "Unexpected number of deleted nodes"
    );
    assert_eq!(
        stats.total_scanned,
        (expected_reachable + expected_orphans) as u64,
    );

    // Verify TARGET DB has only reachable nodes
    let db = DBWithThreadMode::<MultiThreaded>::open_default(&target_path).unwrap();
    let remaining: Vec<_> = db
        .iterator(rocksdb::IteratorMode::Start)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        remaining.len(),
        expected_reachable,
        "Target DB should only contain reachable nodes"
    );
    drop(db);

    // Verify SOURCE DB is UNTOUCHED
    let source_db = DBWithThreadMode::<MultiThreaded>::open_default(&states_path).unwrap();
    let source_remaining: Vec<_> = source_db
        .iterator(rocksdb::IteratorMode::Start)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        source_remaining.len(),
        expected_reachable + expected_orphans,
        "Source DB must be untouched!"
    );
}

#[test]
fn test_dry_run_preserves_all() {
    let (tmp, root, _expected_reachable, expected_orphans) = create_realistic_trie();
    let states_path = tmp.path().join("states");
    let target_path = tmp.path().join("states_pruned");

    let total_before = {
        let db = DBWithThreadMode::<MultiThreaded>::open_default(&states_path).unwrap();
        db.iterator(rocksdb::IteratorMode::Start).count()
    };

    let stats = prune_states(&states_path, &target_path, &[root], true).unwrap();
    assert_eq!(stats.deleted, expected_orphans as u64);

    // Source should be untouched
    let total_after = {
        let db = DBWithThreadMode::<MultiThreaded>::open_default(&states_path).unwrap();
        db.iterator(rocksdb::IteratorMode::Start).count()
    };
    assert_eq!(total_before, total_after, "Dry run should not modify source");

    // Target should not exist
    assert!(!target_path.exists(), "Dry run should not create target");
}

#[test]
fn test_multiple_roots() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("states");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DBWithThreadMode::<MultiThreaded>::open(&opts, &db_path).unwrap();

    // Two separate tries sharing some nodes
    let shared_leaf = store_node(&db, &BencodexValue::Bytes(b"shared".to_vec()));
    let leaf_a_only = store_node(&db, &BencodexValue::Bytes(b"only_in_a".to_vec()));
    let leaf_b_only = store_node(&db, &BencodexValue::Bytes(b"only_in_b".to_vec()));

    // Root A: [shared, leaf_a_only, nulls...]
    let mut items_a = vec![BencodexValue::Null; 17];
    items_a[0] = BencodexValue::Bytes(shared_leaf.to_vec());
    items_a[1] = BencodexValue::Bytes(leaf_a_only.to_vec());
    let root_a = store_node(&db, &BencodexValue::List(items_a));

    // Root B: [shared, null, leaf_b_only, nulls...]
    let mut items_b = vec![BencodexValue::Null; 17];
    items_b[0] = BencodexValue::Bytes(shared_leaf.to_vec());
    items_b[2] = BencodexValue::Bytes(leaf_b_only.to_vec());
    let root_b = store_node(&db, &BencodexValue::List(items_b));

    // Orphan
    let _orphan = store_node(&db, &BencodexValue::Bytes(b"orphan".to_vec()));

    drop(db);

    let stats = prune_states(&db_path, &tmp.path().join("target"), &[root_a, root_b], false).unwrap();

    // Reachable: root_a, root_b, shared_leaf, leaf_a_only, leaf_b_only = 5
    // Orphan: 1
    assert_eq!(stats.reachable, 5);
    assert_eq!(stats.deleted, 1);
}

#[test]
fn test_idempotent_prune() {
    let (tmp, root, expected_reachable, _) = create_realistic_trie();
    let states_path = tmp.path().join("states");
    let target1 = tmp.path().join("target1");
    let target2 = tmp.path().join("target2");

    // First prune: source → target1
    let stats1 = prune_states(&states_path, &target1, &[root], false).unwrap();
    assert!(stats1.deleted > 0);

    // Second prune from target1 → target2: should find nothing to delete
    let stats2 = prune_states(&target1, &target2, &[root], false).unwrap();
    assert_eq!(stats2.deleted, 0, "Second prune should find no orphans");
    assert_eq!(stats2.reachable, expected_reachable as u64);
}

#[test]
fn test_prune_hash_hex_parsing() {
    use nc_snapshot_rs::trie::chain_reader::parse_state_root_hashes;

    let hashes = vec![
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string(),
    ];
    let parsed = parse_state_root_hashes(&hashes).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0][0], 0xab);
    assert_eq!(parsed[0][31], 0x89);
}
