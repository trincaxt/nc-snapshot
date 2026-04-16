//! Chain reader: extracts state root hashes from the chain/ RocksDB.
//!
//! In Libplanet's RocksDB storage layout:
//! - chain/ contains block metadata, indices, and transaction data
//! - The chain tip index stores the latest block hash
//! - Each block header contains a StateRootHash field
//!
//! Libplanet RocksDB key prefixes (from RocksDBStore.cs):
//! - "block/" + block_hash -> serialized block (Bencodex dict)
//! - "block-index/" + block_index (8-byte BE) -> block_hash
//! - "chain-tip/" -> block_hash of the latest block
//! - "tx/" + tx_hash -> serialized transaction
//!
//! The state root hash is stored in the block header as:
//! - Key "s" (Bencodex text) or key bytes for "StateRootHash"
//!
//! Alternative approach (more reliable):
//! The metadata/ RocksDB might contain the state root directly.
//! We try multiple strategies in order of reliability.

use crate::trie::bencodex::{self, BencodexKey, BencodexValue};
use crate::trie::node::NodeHash;
use anyhow::{bail, Context, Result};
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, Options};
use std::path::Path;

/// Known key prefixes in Libplanet's chain/ RocksDB.
const CHAIN_TIP_PREFIX: &[u8] = b"chain-tip/";
const BLOCK_PREFIX: &[u8] = b"block/";
const BLOCK_INDEX_PREFIX: &[u8] = b"block-index/";

/// Open a RocksDB at the given path, handling column families.
fn open_db(path: &Path) -> Result<DBWithThreadMode<MultiThreaded>> {
    let cf_names = match DBWithThreadMode::<MultiThreaded>::list_cf(&Options::default(), path) {
        Ok(cfs) => cfs,
        Err(_) => vec!["default".to_string()],
    };

    let mut opts = Options::default();
    opts.set_max_open_files(64);

    if cf_names.len() <= 1 {
        DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, path, false)
            .with_context(|| format!("Failed to open DB at {:?}", path))
    } else {
        let cf_descriptors: Vec<_> = cf_names
            .iter()
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();
        DBWithThreadMode::<MultiThreaded>::open_cf_descriptors_read_only(
            &opts,
            path,
            cf_descriptors,
            false,
        )
        .with_context(|| format!("Failed to open DB with CFs at {:?}", path))
    }
}

/// Get the last N state root hashes from the blockchain.
///
/// Uses the block-index/ to find the highest block index, then iteratively requests backwards.
pub fn get_last_n_state_roots(store_path: &Path, depth: usize) -> Result<Vec<NodeHash>> {
    let chain_path = store_path.join("chain");
    if !chain_path.exists() {
        bail!(
            "chain/ directory not found at {:?}. Expected Libplanet store structure.",
            store_path
        );
    }

    let db = open_db(&chain_path).context("Failed to open chain/ DB")?;

    tracing::info!("Discovering chain tip...");
    let mut tip_hash = None;
    
    // First try 'tip'
    if let Ok(Some(val)) = db.get(b"tip") {
        tip_hash = Some(val.to_vec());
    } else {
        // Fallback: chain-tip/ prefix using efficient seek
        for item in db.iterator(rocksdb::IteratorMode::From(b"chain-tip/", rocksdb::Direction::Forward)) {
            if let Ok((key, val)) = item {
                if key.starts_with(b"chain-tip/") {
                    tip_hash = Some(val.to_vec());
                    break;
                } else {
                    // Not a chain-tip/ anymore, so it doesn't exist
                    break;
                }
            } else {
                break;
            }
        }
    }

    let mut highest_index = 0u64;
    
    if tip_hash.is_none() {
        tracing::info!("No 'tip' key found. Scanning Libplanet 'I' (0x49) index for highest block...");
        let mut highest_hash = None;
        for item in db.iterator(rocksdb::IteratorMode::From(&[0x49], rocksdb::Direction::Forward)) {
            if let Ok((key, val)) = item {
                // 'I' (0x49) + 16 bytes chain_id + 8 bytes index = 25 bytes
                if key.len() == 25 && key[0] == 0x49 {
                    let index_bytes: [u8; 8] = key[17..25].try_into().unwrap();
                    let index = u64::from_be_bytes(index_bytes);
                    if index > highest_index {
                        highest_index = index;
                        highest_hash = Some(val.to_vec());
                    }
                } else if key[0] != 0x49 {
                    break;
                }
            } else {
                break;
            }
        }
        tip_hash = highest_hash;
    }

    let mut current_hash = match tip_hash {
        Some(h) => h,
        None => bail!("Could not find chain tip ('I' index) in chain/ DB. The snapshot might be empty or corrupted."),
    };

    tracing::info!("Tip hash found: {} at Index: {}", hex::encode(&current_hash), highest_index);

    let mut roots = Vec::new();

    for i in 0..depth {
        let current_index = highest_index.saturating_sub(i as u64);
        tracing::debug!("Fetching block (-{}) at Index {}", i, current_index);
        
        let block_data = match get_block(&db, store_path, &current_hash) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Could not read block {} (-{} from tip): {}", hex::encode(&current_hash), i, e);
                break;
            }
        };

        match extract_state_root_hash(&block_data) {
            Ok(root) => {
                tracing::info!(
                    "Retrieved StateRootHash (-{} from tip): {}",
                    i, hex::encode(&root)
                );
                roots.push(root);
            }
            Err(e) => {
                tracing::warn!("Could not extract StateRootHash from block (-{}): {}", i, e);
            }
        }

        if i < depth - 1 {
            match extract_previous_hash(&block_data) {
                Ok(Some(prev)) => {
                    current_hash = prev.to_vec();
                }
                Ok(None) => {
                    tracing::warn!("Reached genesis or missing PreviousHash at block (-{})", i);
                    break;
                }
                Err(e) => {
                    tracing::warn!("Failed to extract PreviousHash at block (-{}): {}", i, e);
                    break;
                }
            }
        }
    }

    if roots.is_empty() {
        bail!("Failed to extract any state root hashes from the latest blocks.");
    }

    Ok(roots)
}

/// Find the chain tip (latest block hash).
fn find_chain_tip(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<u8>> {
    // Try direct key lookup first
    // Libplanet stores the tip under various key patterns depending on version

    // Pattern 1: "chain-tip/" key with the chain ID appended
    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.context("Iterator error")?;
        if key.starts_with(CHAIN_TIP_PREFIX) {
            tracing::debug!(
                "Found chain tip key: {} -> {} bytes",
                String::from_utf8_lossy(&key),
                value.len()
            );
            return Ok(value.to_vec());
        }
    }

    // Pattern 2: Try "tip" key directly
    if let Ok(Some(value)) = db.get(b"tip") {
        return Ok(value.to_vec());
    }

    // Pattern 3: Scan for the highest block index
    tracing::warn!("Could not find chain tip via direct key. Scanning block indices...");
    let mut highest_index = 0u64;
    let mut highest_hash = None;

    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.context("Iterator error")?;
        if key.starts_with(BLOCK_INDEX_PREFIX) && key.len() == BLOCK_INDEX_PREFIX.len() + 8 {
            let index_bytes: [u8; 8] = key[BLOCK_INDEX_PREFIX.len()..]
                .try_into()
                .unwrap_or([0u8; 8]);
            let index = u64::from_be_bytes(index_bytes);
            if index >= highest_index {
                highest_index = index;
                highest_hash = Some(value.to_vec());
            }
        }
    }

    highest_hash.context("Could not find any block in chain/ DB")
}

/// Get a block's raw data by its hash, attempting RocksDB then epoch RocksDB.
fn get_block(db: &DBWithThreadMode<MultiThreaded>, store_path: &Path, hash: &[u8]) -> Result<BencodexValue> {
    // Try Libplanet v9 standard "B" (0x42) + hash
    let mut key = Vec::with_capacity(1 + hash.len());
    key.push(0x42);
    key.extend_from_slice(hash);

    // 1. Try legacy chain/ DB first
    if let Ok(Some(raw)) = db.get(&key) {
        return bencodex::decode(&raw).context("Failed to decode block Bencodex data");
    }
    
    // Some versions use "block/" + hash
    let mut key2 = Vec::with_capacity(6 + hash.len());
    key2.extend_from_slice(b"block/");
    key2.extend_from_slice(hash);
    if let Ok(Some(raw)) = db.get(&key2) {
        return bencodex::decode(&raw).context("Failed to decode block Bencodex data");
    }

    // 2. Protocol 9: Use blockindex to find the exact epoch directory!
    let blockindex_dir = store_path.join("block").join("blockindex");
    if blockindex_dir.exists() {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(false);
        if let Ok(index_db) = rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open_for_read_only(&opts, &blockindex_dir, false) {
            if let Ok(Some(epoch_val)) = index_db.get(&key) {
                let epoch_str = String::from_utf8_lossy(&epoch_val);
                let epoch_dir = store_path.join("block").join(epoch_str.as_ref());
                
                if epoch_dir.exists() {
                    // Temporarily open the specific epoch block storage read-only
                    if let Ok(epoch_db) = rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open_for_read_only(&opts, &epoch_dir, false) {
                        if let Ok(Some(raw)) = epoch_db.get(&key) {
                            return bencodex::decode(&raw).context("Failed to decode block Bencodex data");
                        }
                    }
                }
            }
        }
    }
    
    bail!("Block not found for hash: {}", hex::encode(hash))
}

/// Extract block index from a decoded block.
fn extract_block_index(block: &BencodexValue) -> Result<u64> {
    if let BencodexValue::Dict(entries) = block {
        for (key, value) in entries {
            let key_matches = match key {
                BencodexKey::Text(t) => t == "index" || t == "i",
                BencodexKey::Bytes(b) => b == b"index" || b == b"i",
            };
            if key_matches {
                if let BencodexValue::Integer(n) = value {
                    return Ok(n.try_into().context("Block index too large for u64")?);
                }
            }
        }
    }
    bail!(
        "Block index not found in block data. Block structure: {:?}",
        format!("{:?}", block).chars().take(200).collect::<String>()
    )
}

/// Get a block hash by its numeric index.
fn get_block_hash_by_index(db: &DBWithThreadMode<MultiThreaded>, index: u64) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(BLOCK_INDEX_PREFIX.len() + 8);
    key.extend_from_slice(BLOCK_INDEX_PREFIX);
    key.extend_from_slice(&index.to_be_bytes());

    db.get(&key)
        .context("RocksDB read error")?
        .with_context(|| format!("Block hash not found for index {}", index))
}

/// Extract PreviousHash from a decoded block
fn extract_previous_hash(block: &BencodexValue) -> Result<Option<NodeHash>> {
    if let BencodexValue::Dict(entries) = block {
        let text_keys: &[&str] = &["previousHash", "PreviousHash", "p", "P"];
        let byte_keys: &[&[u8]] = &[b"previousHash", b"PreviousHash", b"p", b"P"];

        for (key, value) in entries {
            let is_prev = match key {
                BencodexKey::Text(t) => text_keys.contains(&t.as_str()),
                BencodexKey::Bytes(b) => byte_keys.iter().any(|k| *k == b.as_slice()),
            };
            if is_prev {
                // In Protocol 9, PreviousHash is a dict with "BlockHash" => hash
                if let BencodexValue::Dict(inner) = value {
                    let bh_keys: &[&[u8]] = &[b"BlockHash", b"blockHash"];
                    for (inner_k, inner_v) in inner {
                        let is_bh = match inner_k {
                            BencodexKey::Text(t) => t == "BlockHash" || t == "blockHash",
                            BencodexKey::Bytes(b) => bh_keys.iter().any(|k| *k == b.as_slice()),
                        };
                        if is_bh {
                            if let BencodexValue::Bytes(hash_bytes) = inner_v {
                                if hash_bytes.len() == 32 {
                                    let mut hash = [0u8; 32];
                                    hash.copy_from_slice(hash_bytes);
                                    return Ok(Some(hash));
                                }
                            }
                        }
                    }
                } else if let BencodexValue::Bytes(hash_bytes) = value {
                    // Older versions direct byte array
                    if hash_bytes.len() == 32 {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(hash_bytes);
                        return Ok(Some(hash));
                    } else if hash_bytes.len() == 0 {
                        return Ok(None); // Genesis block
                    }
                } else if let BencodexValue::Null = value {
                    return Ok(None); // Genesis block
                }
            }
        }

        // Try nested header
        for (key, value) in entries {
            let is_header = match key {
                BencodexKey::Text(t) => t == "header" || t == "H" || t == "h",
                BencodexKey::Bytes(b) => b == b"header" || b == b"H" || b == b"h",
            };
            if is_header {
                if let Ok(Some(hash)) = extract_previous_hash(value) {
                    return Ok(Some(hash));
                }
            }
        }
    }
    Ok(None)
}

/// Extract the StateRootHash from a decoded block.
///
/// Libplanet block headers store the state root hash under key "s" or "stateRootHash", or as a byte element.
fn extract_state_root_hash(block: &BencodexValue) -> Result<NodeHash> {
    if let BencodexValue::Dict(entries) = block {
        // Try various known key names for state root hash ("S" is Signature, do not use)
        let state_root_keys: &[&str] = &["s", "stateRootHash", "StateRootHash", "state_root_hash"];
        let state_root_byte_keys: &[&[u8]] = &[b"s", b"stateRootHash", b"StateRootHash"];

        for (key, value) in entries {
            let is_state_root = match key {
                BencodexKey::Text(t) => state_root_keys.contains(&t.as_str()),
                BencodexKey::Bytes(b) => state_root_byte_keys.iter().any(|k| *k == b.as_slice()),
            };

            if is_state_root {
                if let BencodexValue::Bytes(hash_bytes) = value {
                    if hash_bytes.len() == 32 {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(hash_bytes);
                        return Ok(hash);
                    } else {
                        // Might be a conflicting key like protocol string, signature, etc.
                        tracing::debug!("Found state root key clone but len is {}", hash_bytes.len());
                    }
                }
            }
        }

        // If not found at top level, try nested "header", "H", or "h" dict
        for (key, value) in entries {
            let is_header = match key {
                BencodexKey::Text(t) => t == "header" || t == "H" || t == "h",
                BencodexKey::Bytes(b) => b == b"header" || b == b"H" || b == b"h",
            };
            if is_header {
                match extract_state_root_hash(value) {
                    Ok(hash) => return Ok(hash),
                    Err(e) => {
                        // Print what was inside the header so we know what keys it has!
                        let header_keys: Vec<String> = if let BencodexValue::Dict(inner) = value {
                            inner.iter().map(|(k, _)| match k {
                                BencodexKey::Text(t) => format!("t:{}", t),
                                BencodexKey::Bytes(b) => format!("b:{}", hex::encode(b)), // b:53 is "S"
                            }).collect()
                        } else { vec!["not_a_dict".to_string()] };
                        tracing::warn!("Header recursion failed: {}. Header keys: {:?}", e, header_keys);
                    }
                }
            }
        }
    }

    // Protocol 9 might use index parsing if structured as a List (fallback for older protocols)
    if let BencodexValue::List(items) = block {
        // Heuristic: state root is often the 32-byte array near the front or at the end
        for item in items.iter().rev() {
            if let BencodexValue::Bytes(b) = item {
                if b.len() == 32 {
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(b);
                    return Ok(hash);
                }
            }
        }
    }

    bail!(
        "StateRootHash not found in block. Keys present: {:?}",
        if let BencodexValue::Dict(entries) = block {
            entries
                .iter()
                .map(|(k, _)| format!("{:?}", k))
                .collect::<Vec<_>>()
        } else {
            vec![format!("Not a dict: {:?}", block)]
        }
    )
}

/// Diagnostic: dump the structure of chain/ DB to understand its layout.
pub fn diagnose_chain(store_path: &Path, max_keys: usize) -> Result<()> {
    let chain_path = store_path.join("chain");
    if !chain_path.exists() {
        bail!("chain/ directory not found at {:?}", store_path);
    }

    let db = open_db(&chain_path)?;

    println!("\n=== chain/ DB Diagnostic ===");

    let mut key_len_dist: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();
    let mut val_len_dist: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();
    let mut total_keys = 0usize;
    let mut sample_keys = Vec::new();
    let mut candidates_32: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.context("Iterator error")?;
        total_keys += 1;

        *key_len_dist.entry(key.len()).or_insert(0) += 1;
        *val_len_dist.entry(value.len()).or_insert(0) += 1;

        if value.len() == 32 {
            candidates_32.push((key.to_vec(), value.to_vec()));
        }

        if sample_keys.len() < max_keys {
            sample_keys.push((key.to_vec(), value.len()));
        }
    }

    println!("Total keys: {}", total_keys);

    // Key length distribution
    println!("\nKey length distribution:");
    let mut kl: Vec<_> = key_len_dist.into_iter().collect();
    kl.sort_by_key(|&(len, _)| len);
    for (len, count) in &kl {
        println!("  {} bytes : {} keys", len, count);
    }

    // Value length distribution
    println!("\nValue length distribution:");
    let mut vl: Vec<_> = val_len_dist.into_iter().collect();
    vl.sort_by_key(|&(len, _)| len);
    for (len, count) in &vl {
        println!("  {} bytes : {} entries", len, count);
    }

    // 32-byte value entries (potential state roots)
    println!("\n32-byte value entries: {} total", candidates_32.len());
    if !candidates_32.is_empty() {
        // Show first and last few
        println!("  First 3:");
        for (key, val) in candidates_32.iter().take(3) {
            println!("    key={} val={}", hex::encode(key), hex::encode(val));
        }
        if candidates_32.len() > 6 {
            println!("  ...");
        }
        println!("  Last 3:");
        for (key, val) in candidates_32
            .iter()
            .rev()
            .take(3)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            println!("    key={} val={}", hex::encode(key), hex::encode(val));
        }

        // Detect sequential groups using depth backtracking
        println!("\nBacktracking 5 blocks from tip...");
        match get_last_n_state_roots(store_path, 5) {
            Ok(roots) => {
                println!(
                    "  Found {} state roots:",
                    roots.len()
                );
                for root in &roots {
                    println!("    hash={}", hex::encode(root));
                }
            }
            Err(e) => println!("  Chain read failed: {}", e),
        }
    }

    // Sample keys
    println!("\nSample keys (first {}):", sample_keys.len());
    for (i, (key, val_len)) in sample_keys.iter().enumerate().take(20) {
        println!("  [{}] key={} val_len={}", i, hex::encode(key), val_len);
    }

    Ok(())
}

/// Find state root hashes directly from chain/ DB.
/// Included for backward compatibility, now uses backwards depth traversal.
pub fn find_state_roots(store_path: &Path, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut entries = Vec::new();
    let roots = get_last_n_state_roots(store_path, n)?;
    for root in roots {
        entries.push((b"dummy_key".to_vec(), root.to_vec()));
    }
    Ok(entries)
}

/// Manual state root input: parse hex-encoded hashes from command line or file.
/// This is a fallback when automatic chain reading fails.
pub fn parse_state_root_hashes(hex_hashes: &[String]) -> Result<Vec<NodeHash>> {
    hex_hashes
        .iter()
        .map(|h| {
            let h = h.trim().trim_start_matches("0x");
            let bytes = hex::decode(h).with_context(|| format!("Invalid hex hash: {}", h))?;
            if bytes.len() != 32 {
                bail!("Hash must be 32 bytes, got {} from: {}", bytes.len(), h);
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&bytes);
            Ok(hash)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_state_root_hashes() {
        let hashes = vec![
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            "0x0000000000000000000000000000000000000000000000000000000000000002".to_string(),
        ];
        let result = parse_state_root_hashes(&hashes).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][31], 0x01);
        assert_eq!(result[1][31], 0x02);
    }

    #[test]
    fn test_parse_invalid_hash() {
        let hashes = vec!["not-a-hash".to_string()];
        assert!(parse_state_root_hashes(&hashes).is_err());
    }

    #[test]
    fn test_parse_wrong_length_hash() {
        let hashes = vec!["0102030405".to_string()]; // only 5 bytes
        assert!(parse_state_root_hashes(&hashes).is_err());
    }
}
