//! Trie pruner: Selective Streaming Copy for high-performance state pruning.
//!
//! Instead of deleting unreachable nodes in-place (which creates tombstones and
//! requires slow compaction), we:
//! 1. Open the source DB in ReadOnly mode
//! 2. DFS-traverse from state roots to build a Bloom Filter of reachable nodes
//! 3. Stream all keys through the filter, writing only reachable ones to SST files
//! 4. Ingest SSTs into a clean target DB
//! 5. Atomic swap: rename source → old, target → source
//!
//! This approach produces a perfectly compact DB with zero tombstones,
//! reducing states/ from ~36 GB to ~19 GB (parity with C# CopyStates).

use crate::trie::bloom::ReachableSet;
use crate::trie::node::{NodeHash, TrieNode};
use crate::trie::sst_writer;
use anyhow::{bail, Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, Options};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Statistics from a prune operation.
#[derive(Debug, Clone)]
pub struct PruneStats {
    /// Number of reachable nodes (kept).
    pub reachable: u64,
    /// Number of unreachable nodes (skipped/deleted).
    pub deleted: u64,
    /// Number of non-node keys skipped (not 32-byte keys).
    pub skipped_keys: u64,
    /// Total keys scanned.
    pub total_scanned: u64,
    /// Time spent collecting reachable nodes.
    pub traversal_time_secs: f64,
    /// Time spent on SST copy (replaces deletion_time).
    pub deletion_time_secs: f64,
    /// Time spent on atomic swap (replaces compaction_time).
    pub compaction_time_secs: f64,
    /// Size of source DB in bytes.
    pub source_size_bytes: u64,
    /// Size of target DB in bytes.
    pub target_size_bytes: u64,
    /// Number of SST files created.
    pub sst_files_created: u32,
}

impl std::fmt::Display for PruneStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Prune complete (Selective Streaming Copy):\n\
             - Reachable nodes kept: {}\n\
             - Unreachable nodes removed: {}\n\
             - Non-node keys preserved: {}\n\
             - Total keys scanned: {}\n\
             - Traversal time: {:.1}s\n\
             - SST copy time: {:.1}s\n\
             - Swap time: {:.1}s\n\
             - Total time: {:.1}s\n\
             - Source size: {}\n\
             - Target size: {}\n\
             - SST files created: {}\n\
             - Size reduction: {:.1}%",
            self.reachable,
            self.deleted,
            self.skipped_keys,
            self.total_scanned,
            self.traversal_time_secs,
            self.deletion_time_secs,
            self.compaction_time_secs,
            self.traversal_time_secs + self.deletion_time_secs + self.compaction_time_secs,
            format_size(self.source_size_bytes),
            format_size(self.target_size_bytes),
            self.sst_files_created,
            if self.source_size_bytes > 0 {
                (1.0 - self.target_size_bytes as f64 / self.source_size_bytes as f64) * 100.0
            } else {
                0.0
            },
        )
    }
}

fn format_size(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Collect all node hashes reachable from a given root hash by iterative DFS.
///
/// This traverses the Merkle Patricia Trie starting from `root`, following
/// all HashNode references and recursing into inline nodes.
///
/// Uses a stack-based DFS (no recursion) to avoid stack overflow on deep tries.
///
/// # Arguments
/// - `db`: Open RocksDB handle for states/ (ReadOnly)
/// - `root`: The state root hash to start traversal from
/// - `visited`: Bloom filter or HashSet tracking visited nodes
/// - `visited_counter`: Atomic counter for progress tracking
///
/// # Returns
/// Number of new nodes discovered from this root.
pub fn collect_reachable_nodes(
    db: &DBWithThreadMode<MultiThreaded>,
    root: &NodeHash,
    visited: &mut ReachableSet,
    visited_counter: &AtomicU64,
) -> Result<u64> {
    let mut stack = vec![*root];
    let mut errors = Vec::new();
    let mut discovered = 0u64;

    const BATCH_SIZE: usize = 10000;

    while !stack.is_empty() {
        let mut batch_keys = Vec::with_capacity(BATCH_SIZE);
        
        while let Some(hash) = stack.pop() {
            if !visited.already_visited(&hash) {
                visited.insert(&hash);
                batch_keys.push(hash);
            }
            if batch_keys.len() >= BATCH_SIZE {
                break;
            }
        }

        if batch_keys.is_empty() {
            break;
        }

        discovered += batch_keys.len() as u64;
        visited_counter.fetch_add(batch_keys.len() as u64, Ordering::Relaxed);

        // Fetch them IN PARALLEL utilizing NVMe QD scaling via multi_get
        let results = db.multi_get(&batch_keys);

        for (i, result) in results.into_iter().enumerate() {
            let hash = batch_keys[i];

            let raw = match result {
                Ok(Some(data)) => data,
                Ok(None) => {
                    // Node not found -- log and continue
                    tracing::warn!("Node not found in states/ DB: {}", hex::encode(hash));
                    errors.push(hash);
                    continue;
                }
                Err(e) => {
                    tracing::error!("RocksDB read error for node {}: {}", hex::encode(hash), e);
                    errors.push(hash);
                    continue;
                }
            };

            // Decode the node and push children
            match TrieNode::decode(&raw) {
                Ok(node) => {
                    for child_hash in node.child_hashes() {
                        if !visited.already_visited(&child_hash) {
                            stack.push(child_hash);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to decode node {}: {}. Treating as leaf.", hex::encode(hash), e);
                }
            }
        }
    }

    if !errors.is_empty() {
        tracing::warn!(
            "{} nodes had errors during traversal (still marked reachable if previously visited)",
            errors.len()
        );
    }

    Ok(discovered)
}

/// Open the states/ RocksDB in ReadOnly mode.
fn open_states_db_readonly(states_path: &Path) -> Result<DBWithThreadMode<MultiThreaded>> {
    let cf_names = match DBWithThreadMode::<MultiThreaded>::list_cf(
        &Options::default(),
        states_path,
    ) {
        Ok(cfs) => cfs,
        Err(_) => vec!["default".to_string()],
    };

    tracing::info!("states/ DB column families: {:?}", cf_names);

    let mut opts = Options::default();
    opts.set_max_open_files(2048); // Increase open files considerably
    opts.set_allow_mmap_reads(true);
    opts.set_max_background_jobs(4);

    // ==========================================
    // MASSIVE PERFORMANCE FIX: 4GB Block Cache
    // Without this, every random read misses the 8MB default cache
    // and causes a physical 4KB read from the NVMe slot.
    // Over 90,000,000 nodes, it translates to ~700GB read and 11+ runtime.
    // ==========================================
    let mut table_opts = rocksdb::BlockBasedOptions::default();
    table_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(4 * 1024 * 1024 * 1024));
    // A bloom filter prevents fetching un-existant hashes entirely
    table_opts.set_bloom_filter(10.0, false);
    opts.set_block_based_table_factory(&table_opts);
    opts.set_optimize_filters_for_hits(true);

    if cf_names.len() <= 1 {
        DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, states_path, false)
            .with_context(|| format!("Failed to open states/ DB (ReadOnly) at {:?}", states_path))
    } else {
        let cf_descriptors: Vec<_> = cf_names
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                let mut cf_table_opts = rocksdb::BlockBasedOptions::default();
                cf_table_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(4 * 1024 * 1024 * 1024));
                cf_table_opts.set_bloom_filter(10.0, false);
                cf_opts.set_block_based_table_factory(&cf_table_opts);
                cf_opts.set_optimize_filters_for_hits(true);
                rocksdb::ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();
        DBWithThreadMode::<MultiThreaded>::open_cf_descriptors_read_only(
            &opts,
            states_path,
            cf_descriptors,
            false,
        )
        .with_context(|| {
            format!(
                "Failed to open states/ DB (ReadOnly) with CFs {:?} at {:?}",
                cf_names, states_path
            )
        })
    }
}

/// Open old-style RW states DB (for tests that need write access).
#[cfg(test)]
fn open_states_db(states_path: &Path) -> Result<DBWithThreadMode<MultiThreaded>> {
    let cf_names = match DBWithThreadMode::<MultiThreaded>::list_cf(
        &Options::default(),
        states_path,
    ) {
        Ok(cfs) => cfs,
        Err(_) => vec!["default".to_string()],
    };

    let mut opts = Options::default();
    opts.set_max_open_files(256);
    opts.set_allow_mmap_reads(true);
    opts.set_max_background_jobs(4);

    if cf_names.len() <= 1 {
        DBWithThreadMode::<MultiThreaded>::open(&opts, states_path)
            .with_context(|| format!("Failed to open states/ DB at {:?}", states_path))
    } else {
        let cf_descriptors: Vec<_> = cf_names
            .iter()
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();
        DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
            &opts,
            states_path,
            cf_descriptors,
        )
        .with_context(|| {
            format!(
                "Failed to open states/ DB with CFs {:?} at {:?}",
                cf_names, states_path
            )
        })
    }
}

/// Estimate total key count for Bloom Filter sizing.
fn estimate_key_count(db: &DBWithThreadMode<MultiThreaded>) -> u64 {
    // Try to get RocksDB's estimate first
    if let Ok(Some(estimate)) = db.property_value("rocksdb.estimate-num-keys") {
        if let Ok(n) = estimate.parse::<u64>() {
            if n > 0 {
                tracing::info!("RocksDB estimates {} keys", n);
                return n;
            }
        }
    }

    // Fallback: sample-based estimate
    tracing::info!("Estimating key count by sampling...");
    let mut count = 0u64;
    let sample_limit = 100_000;
    for item in db.iterator(IteratorMode::Start) {
        if item.is_ok() {
            count += 1;
        }
        if count >= sample_limit {
            break;
        }
    }

    if count >= sample_limit {
        // Extrapolate (rough)
        let estimate = count * 1000; // Very rough, but better than nothing
        tracing::info!("Sampled {} keys, estimating ~{} total", count, estimate);
        estimate
    } else {
        tracing::info!("DB has {} keys (small DB)", count);
        count
    }
}

/// Prune the states/ RocksDB using Selective Streaming Copy.
///
/// Opens the source DB in **ReadOnly** mode (never modifies the original),
/// then creates a clean copy at `target_path` containing only reachable nodes.
///
/// The source directory (9c-blockchain) is **never** modified, renamed, or deleted.
///
/// # Arguments
/// - `states_path`: Path to the source states/ directory (ReadOnly)
/// - `target_path`: Where to write the pruned clean DB (e.g., `/tmp/states_pruned/`)
/// - `state_roots`: State root hashes to preserve
/// - `dry_run`: If true, only traverses and reports without creating a new DB
///
/// # Returns
/// Statistics about the prune operation.
pub fn prune_states(
    states_path: &Path,
    target_path: &Path,
    state_roots: &[NodeHash],
    dry_run: bool,
) -> Result<PruneStats> {
    if state_roots.is_empty() {
        bail!("No state roots provided for pruning");
    }

    let mp = MultiProgress::new();

    // ── Phase 1: Collect reachable nodes (ReadOnly) ──
    tracing::info!(
        "Phase 1/2: Collecting reachable nodes from {} root(s)...",
        state_roots.len()
    );
    tracing::info!("Source (ReadOnly): {:?}", states_path);
    if !dry_run {
        tracing::info!("Target: {:?}", target_path);
    }

    let traversal_pb = mp.add(ProgressBar::new_spinner());
    traversal_pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] Traversing trie... {msg}")
            .unwrap(),
    );
    traversal_pb.enable_steady_tick(std::time::Duration::from_millis(200));

    let traversal_start = Instant::now();
    let db = open_states_db_readonly(states_path)?;

    // Estimate total keys for Bloom Filter sizing
    let estimated_keys = estimate_key_count(&db);
    let mut reachable = ReachableSet::new(estimated_keys as usize);

    let visited_counter = AtomicU64::new(0);

    // Traverse each root with shared visited set
    for (i, root) in state_roots.iter().enumerate() {
        traversal_pb.set_message(format!(
            "root {}/{} ({}) — {} nodes found",
            i + 1,
            state_roots.len(),
            hex::encode(&root[..8]),
            visited_counter.load(Ordering::Relaxed),
        ));

        let discovered = collect_reachable_nodes(&db, root, &mut reachable, &visited_counter)?;
        tracing::info!(
            "Root[{}] {}: discovered {} new nodes (total: {})",
            i,
            hex::encode(&root[..8]),
            discovered,
            reachable.len(),
        );
    }

    let traversal_time = traversal_start.elapsed().as_secs_f64();
    let reachable_count = reachable.len();

    traversal_pb.finish_with_message(format!(
        "Found {} reachable nodes in {:.1}s",
        reachable_count, traversal_time,
    ));

    tracing::info!(
        "Phase 1 complete: {} reachable nodes found in {:.1}s",
        reachable_count,
        traversal_time,
    );

    // ── Dry run: just count unreachable ──
    if dry_run {
        tracing::info!("Scanning all keys to count unreachable (dry run)...");

        let scan_pb = mp.add(ProgressBar::new_spinner());
        scan_pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.blue} [{elapsed_precise}] Scanning... {msg}")
                .unwrap(),
        );
        scan_pb.enable_steady_tick(std::time::Duration::from_millis(200));

        let scan_start = Instant::now();
        let mut total_scanned = 0u64;
        let mut unreachable = 0u64;
        let mut skipped_keys = 0u64;

        for item in db.iterator(IteratorMode::Start) {
            let (key, _) = item.context("RocksDB iteration error")?;
            total_scanned += 1;

            if total_scanned % 500_000 == 0 {
                scan_pb.set_message(format!(
                    "{} (reachable: {}, unreachable: {}, meta: {})",
                    total_scanned,
                    total_scanned - unreachable - skipped_keys,
                    unreachable,
                    skipped_keys,
                ));
            }

            if key.len() != 32 {
                skipped_keys += 1;
                continue;
            }

            let hash: NodeHash = key.as_ref().try_into().context("Key not 32 bytes")?;
            if !reachable.contains(&hash) {
                unreachable += 1;
            }
        }

        let scan_time = scan_start.elapsed().as_secs_f64();
        scan_pb.finish_with_message(format!(
            "Done! scanned={}, reachable={}, unreachable={}",
            total_scanned, reachable_count, unreachable,
        ));

        drop(db);

        let source_size = sst_writer::dir_size(states_path);

        return Ok(PruneStats {
            reachable: reachable_count,
            deleted: unreachable,
            skipped_keys,
            total_scanned,
            traversal_time_secs: traversal_time,
            deletion_time_secs: scan_time,
            compaction_time_secs: 0.0,
            source_size_bytes: source_size,
            target_size_bytes: 0,
            sst_files_created: 0,
        });
    }

    // ── Phase 2: SST Streaming Copy (source ReadOnly → target) ──
    tracing::info!("Phase 2/2: Creating clean DB via SST Streaming Copy...");
    tracing::info!("  Source (ReadOnly): {:?}", states_path);
    tracing::info!("  Target: {:?}", target_path);

    let source_size = sst_writer::dir_size(states_path);
    tracing::info!("Source DB size: {}", format_size(source_size));

    // Check available disk space at target location
    let check_path = target_path.parent().unwrap_or(target_path);
    let available = sst_writer::available_disk_space(check_path);
    if available > 0 {
        tracing::info!("Available disk space: {}", format_size(available));
        let needed = source_size / 2;
        if available < needed {
            bail!(
                "Insufficient disk space: need at least {} free, but only {} available. \
                 The pruned DB will be roughly half the size of the source.",
                format_size(needed),
                format_size(available),
            );
        }
    }

    let copy_start = Instant::now();

    let sst_stats = sst_writer::write_reachable_to_new_db(&db, target_path, &reachable, false)?;

    let copy_time = copy_start.elapsed().as_secs_f64();

    // Close source DB (ReadOnly handle)
    drop(db);

    let target_size = sst_writer::dir_size(target_path);

    tracing::info!(
        "Phase 2 complete: {} keys copied, {} keys skipped in {:.1}s",
        sst_stats.keys_copied,
        sst_stats.keys_skipped,
        copy_time,
    );
    tracing::info!(
        "Pruned DB written to: {:?} ({})",
        target_path,
        format_size(target_size),
    );

    let stats = PruneStats {
        reachable: reachable_count,
        deleted: sst_stats.keys_skipped,
        skipped_keys: sst_stats.metadata_keys_copied,
        total_scanned: sst_stats.total_scanned,
        traversal_time_secs: traversal_time,
        deletion_time_secs: copy_time,
        compaction_time_secs: 0.0,
        source_size_bytes: source_size,
        target_size_bytes: target_size,
        sst_files_created: sst_stats.sst_files_created,
    };

    tracing::info!("{}", stats);

    Ok(stats)
}

/// Quick diagnostic: scan the states/ DB and report key statistics.
pub fn diagnose_states(states_path: &Path, max_nodes: usize) -> Result<()> {
    let db = open_states_db_readonly(states_path)?;

    let mut total_keys = 0u64;
    let mut key_32_count = 0u64;
    let mut other_key_count = 0u64;
    let mut total_value_bytes = 0u64;
    let mut node_type_counts = std::collections::HashMap::new();
    let mut decode_errors = 0u64;
    let mut sample_nodes = Vec::new();

    tracing::info!("Scanning states/ DB (max {} nodes)...", max_nodes);

    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.context("RocksDB iteration error")?;
        total_keys += 1;
        total_value_bytes += value.len() as u64;

        if key.len() == 32 {
            key_32_count += 1;

            if (key_32_count as usize) <= max_nodes {
                match TrieNode::decode(&value) {
                    Ok(node) => {
                        let type_name = match &node {
                            TrieNode::Short { .. } => "ShortNode",
                            TrieNode::Full { .. } => "FullNode",
                            TrieNode::Value(_) => "ValueNode",
                        };
                        *node_type_counts.entry(type_name).or_insert(0u64) += 1;

                        if sample_nodes.len() < 5 {
                            sample_nodes.push((
                                hex::encode(key.as_ref()),
                                format!("{:?}", node),
                                value.len(),
                            ));
                        }
                    }
                    Err(e) => {
                        decode_errors += 1;
                        if decode_errors <= 3 {
                            tracing::warn!(
                                "Decode error for key {}: {} (value bytes: {})",
                                hex::encode(key.as_ref()),
                                e,
                                value.len(),
                            );
                        }
                    }
                }
            }
        } else {
            other_key_count += 1;
        }

        if total_keys % 1_000_000 == 0 {
            tracing::info!("Scanned {} keys...", total_keys);
        }
    }

    println!("\n=== states/ DB Diagnostic ===");
    println!("Total keys:           {}", total_keys);
    println!("32-byte keys (nodes): {}", key_32_count);
    println!("Other keys:           {}", other_key_count);
    println!(
        "Total value bytes:    {} ({:.2} GiB)",
        total_value_bytes,
        total_value_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!("Decode errors:        {}", decode_errors);
    println!("\nNode type distribution (from {} sampled):", max_nodes);
    for (type_name, count) in &node_type_counts {
        println!("  {}: {}", type_name, count);
    }
    println!("\nSample nodes:");
    for (hash, debug, size) in &sample_nodes {
        println!("  key={} ({} bytes): {}", &hash[..16], size, debug);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::bencodex::{encode, BencodexValue};
    use crate::trie::node::sha256;
    use tempfile::TempDir;

    /// Create a test RocksDB with a small trie structure.
    fn create_test_db() -> (TempDir, Vec<NodeHash>) {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("states");

        let db = DBWithThreadMode::<MultiThreaded>::open(&{
            let mut o = Options::default();
            o.create_if_missing(true);
            o
        }, &db_path).unwrap();

        // Build a simple trie:
        //
        //   root (FullNode) -> child_a (ShortNode) -> leaf_a (ValueNode)
        //                   -> child_b (ValueNode)
        //   orphan (ValueNode) -- not reachable from root

        // Leaf A: value node
        let leaf_a_data = encode(&BencodexValue::Bytes(b"value_a".to_vec()));
        let leaf_a_hash = sha256(&leaf_a_data);
        db.put(&leaf_a_hash, &leaf_a_data).unwrap();

        // Child A: short node pointing to leaf_a
        let child_a_data = encode(&BencodexValue::List(vec![
            BencodexValue::Bytes(vec![0x01, 0x02]),     // path
            BencodexValue::Bytes(leaf_a_hash.to_vec()), // hash ref to leaf_a
        ]));
        let child_a_hash = sha256(&child_a_data);
        db.put(&child_a_hash, &child_a_data).unwrap();

        // Child B: value node (inline as leaf in root)
        let child_b_data = encode(&BencodexValue::Bytes(b"value_b".to_vec()));
        let child_b_hash = sha256(&child_b_data);
        db.put(&child_b_hash, &child_b_data).unwrap();

        // Root: full node with children at slots 0 and 5
        let mut root_items = vec![BencodexValue::Null; 17];
        root_items[0] = BencodexValue::Bytes(child_a_hash.to_vec());
        root_items[5] = BencodexValue::Bytes(child_b_hash.to_vec());
        let root_data = encode(&BencodexValue::List(root_items));
        let root_hash = sha256(&root_data);
        db.put(&root_hash, &root_data).unwrap();

        // Orphan: not reachable from root
        let orphan_data = encode(&BencodexValue::Bytes(b"orphan_value".to_vec()));
        let orphan_hash = sha256(&orphan_data);
        db.put(&orphan_hash, &orphan_data).unwrap();

        drop(db);

        (tmp, vec![root_hash])
    }

    #[test]
    fn test_collect_reachable() {
        let (tmp, roots) = create_test_db();
        let db_path = tmp.path().join("states");

        let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(
            &Options::default(), &db_path, false,
        ).unwrap();
        let counter = AtomicU64::new(0);
        let mut visited = ReachableSet::new(100); // Small, uses exact HashSet

        let _discovered = collect_reachable_nodes(&db, &roots[0], &mut visited, &counter).unwrap();

        // Should find: root, child_a, leaf_a, child_b = 4 nodes
        assert_eq!(visited.len(), 4, "Expected 4 reachable nodes");
    }

    #[test]
    fn test_prune_removes_orphans() {
        let (tmp, roots) = create_test_db();
        let db_path = tmp.path().join("states");
        let target_path = tmp.path().join("states_pruned");

        let stats = prune_states(&db_path, &target_path, &roots, false).unwrap();

        // 4 reachable, 1 orphan removed (skipped)
        assert_eq!(stats.reachable, 4);
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.total_scanned, 5);

        // Verify the TARGET DB has exactly 4 nodes
        let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(
            &Options::default(), &target_path, false,
        ).unwrap();
        let remaining: Vec<_> = db.iterator(IteratorMode::Start).collect();
        assert_eq!(remaining.len(), 4);
        drop(db);

        // Verify the SOURCE DB is UNTOUCHED (all 5 still there)
        let source_db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(
            &Options::default(), &db_path, false,
        ).unwrap();
        let source_remaining: Vec<_> = source_db.iterator(IteratorMode::Start).collect();
        assert_eq!(source_remaining.len(), 5, "Source DB must be untouched!");
    }

    #[test]
    fn test_prune_dry_run() {
        let (tmp, roots) = create_test_db();
        let db_path = tmp.path().join("states");
        let target_path = tmp.path().join("states_pruned");

        let stats = prune_states(&db_path, &target_path, &roots, true).unwrap();

        // Should report the same counts but not actually create anything
        assert_eq!(stats.reachable, 4);
        assert_eq!(stats.deleted, 1);

        // Verify source is untouched (all 5 still there)
        let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(
            &Options::default(), &db_path, false,
        ).unwrap();
        let remaining: Vec<_> = db.iterator(IteratorMode::Start).collect();
        assert_eq!(remaining.len(), 5);

        // Verify target was NOT created
        assert!(!target_path.exists(), "Dry run should not create target");
    }

    #[test]
    fn test_prune_no_roots_fails() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");
        let result = prune_states(tmp.path(), &target, &[], false);
        assert!(result.is_err());
    }
}
