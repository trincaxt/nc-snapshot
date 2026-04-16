//! Bencodex Merkle trie parser for Nine Chronicles (Libplanet).
//!
//! This module provides a focused parser for extracting child hash references
//! from Merkle Patricia Trie nodes stored in RocksDB. Libplanet's trie uses
//! Bencodex encoding with three node types:
//!
//! - **ShortNode** (path compression): Bencodex list `[encoded_path, child]`
//! - **FullNode** (branch): Bencodex list with 17 elements (16 children + value)
//! - **ValueNode** (leaf): Bencodex list `[encoded_path, value]` or raw bytes
//!
//! Keys in RocksDB are SHA256 hashes of the node data; values are raw Bencodex bytes.
//!
//! The approach here is intentionally lightweight: instead of building a full
//! Bencodex AST, we scan raw bytes for 32-byte hash patterns that represent
//! child pointers in the trie. A heuristic filter avoids false positives.

use anyhow::{bail, Context, Result};
use std::fmt;

/// SHA256 hash size in bytes.
const HASH_SIZE: usize = 32;

/// A 32-byte SHA256 hash used as a node identifier in the trie.
pub type NodeHash = [u8; HASH_SIZE];

// ---------------------------------------------------------------------------
// TrieNode enum
// ---------------------------------------------------------------------------

/// Classification of a Merkle Patricia Trie node.
///
/// Derived from the Bencodex structure of the raw node bytes stored in RocksDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrieNode {
    /// ShortNode (extension or leaf): 2-element list where the first element
    /// is a Binary (Bytes) value encoding a nibble path.
    ///
    /// Fields:
    /// - `path`: the raw nibble-encoded path bytes
    /// - `child_hashes`: hash references found in the child slot
    Short {
        path: Vec<u8>,
        child_hashes: Vec<NodeHash>,
    },

    /// FullNode (branch): 17-element list where indices 0..15 are child slots
    /// and index 16 is the optional value.
    ///
    /// Fields:
    /// - `children_hashes`: hash references found across all 16 child slots
    /// - `value_hashes`: hash references found in the value slot (index 16)
    Full {
        children_hashes: Vec<NodeHash>,
        value_hashes: Vec<NodeHash>,
    },

    /// ValueNode (leaf value): `[Null, value]` list or raw bytes.
    ///
    /// Fields:
    /// - `nested_hashes`: hash references found inside the value payload
    Value { nested_hashes: Vec<NodeHash> },
}

impl fmt::Display for TrieNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrieNode::Short {
                path,
                child_hashes,
            } => {
                write!(
                    f,
                    "ShortNode(path={} bytes, {} children)",
                    path.len(),
                    child_hashes.len()
                )
            }
            TrieNode::Full {
                children_hashes,
                value_hashes,
            } => {
                write!(
                    f,
                    "FullNode({} child hashes, {} value hashes)",
                    children_hashes.len(),
                    value_hashes.len()
                )
            }
            TrieNode::Value { nested_hashes } => {
                write!(f, "ValueNode({} nested hashes)", nested_hashes.len())
            }
        }
    }
}

impl TrieNode {
    /// Collect every child hash reference reachable from this node.
    pub fn all_child_hashes(&self) -> &[NodeHash] {
        // Returns a reference to the internal vec depending on variant.
        // For convenience we provide a borrowing accessor below.
        match self {
            TrieNode::Short { child_hashes, .. } => child_hashes,
            TrieNode::Full {
                children_hashes, ..
            } => children_hashes,
            TrieNode::Value { nested_hashes } => nested_hashes,
        }
    }

    /// Parse raw Bencodex bytes into a classified TrieNode with extracted hashes.
    pub fn parse(data: &[u8]) -> Result<Self> {
        parse_trie_node(data)
    }
}

// ---------------------------------------------------------------------------
// Core parsing
// ---------------------------------------------------------------------------

/// Parse raw Bencodex-encoded trie node bytes into a [`TrieNode`].
///
/// This performs a structural scan of the Bencodex envelope to classify the
/// node type, then extracts 32-byte hash references from the appropriate slots.
///
/// # Errors
///
/// Returns an error if the data is empty or does not start with a valid Bencodex
/// tag byte.
pub fn parse_trie_node(data: &[u8]) -> Result<TrieNode> {
    if data.is_empty() {
        bail!("Empty trie node data");
    }

    match data[0] {
        b'l' => parse_list_node(data),
        b'n' => {
            // Null node — no children
            Ok(TrieNode::Value {
                nested_hashes: vec![],
            })
        }
        b'0'..=b'9' => {
            // Top-level binary value — scan for hashes inside
            let hashes = extract_child_hashes_from_bencodex(data);
            Ok(TrieNode::Value {
                nested_hashes: hashes,
            })
        }
        other => bail!(
            "Unexpected Bencodex tag byte 0x{:02x} ({:?}) — expected list ('l'), null ('n'), or digit",
            other,
            other as char
        ),
    }
}

/// Parse a Bencodex list node, classifying as ShortNode, FullNode, or ValueNode.
fn parse_list_node(data: &[u8]) -> Result<TrieNode> {
    // Count top-level list elements without full decoding.
    let element_positions = enumerate_list_elements(data)
        .context("Failed to enumerate Bencodex list elements")?;

    match element_positions.len() {
        2 => classify_two_element_list(data, &element_positions),
        17 => classify_full_node(data, &element_positions),
        33 => {
            // Some Libplanet versions use 33-element lists for account tries.
            classify_full_node(data, &element_positions)
        }
        n => bail!(
            "Unexpected list length {n} for trie node (expected 2, 17, or 33)"
        ),
    }
}

/// Classify a 2-element list as either a ShortNode or a ValueNode.
///
/// - If element[0] is a Binary (Bytes), it is a ShortNode's path.
/// - If element[0] is Null (`n`), it is a ValueNode wrapper `[Null, value]`.
fn classify_two_element_list(
    data: &[u8],
    positions: &[(usize, usize)],
) -> Result<TrieNode> {
    let (start, end) = positions[0];
    let first_element = &data[start..end];

    if first_element.is_empty() {
        bail!("Empty first element in 2-element list");
    }

    match first_element[0] {
        b'n' => {
            // ValueNode: [Null, <value>]
            // Extract hashes from element[1]
            let (vstart, vend) = positions[1];
            let value_data = &data[vstart..vend];
            let hashes = extract_child_hashes_from_bencodex(value_data);
            Ok(TrieNode::Value {
                nested_hashes: hashes,
            })
        }
        b'0'..=b'9' => {
            // ShortNode: [path_bytes, child]
            let path = decode_binary_bytes(first_element)
                .context("Failed to decode ShortNode path bytes")?;

            // Extract hashes from the child element
            let (cstart, cend) = positions[1];
            let child_data = &data[cstart..cend];
            let child_hashes = extract_child_hashes_from_bencodex(child_data);

            Ok(TrieNode::Short {
                path,
                child_hashes,
            })
        }
        other => bail!(
            "Unexpected first element tag 0x{:02x} in 2-element list",
            other
        ),
    }
}

/// Classify a 17 (or 33) element list as a FullNode.
///
/// Indices 0..15 are child slots; index 16 is the value slot.
/// For 33-element lists (account tries), indices 0..31 are child slots and 32 is value.
fn classify_full_node(
    data: &[u8],
    positions: &[(usize, usize)],
) -> Result<TrieNode> {
    let n = positions.len();
    let child_count = n - 1;

    let mut children_hashes = Vec::new();
    for i in 0..child_count {
        let (start, end) = positions[i];
        let child_data = &data[start..end];
        let hashes = extract_child_hashes_from_bencodex(child_data);
        children_hashes.extend(hashes);
    }

    let (vstart, vend) = positions[child_count];
    let value_data = &data[vstart..vend];
    let value_hashes = extract_child_hashes_from_bencodex(value_data);

    Ok(TrieNode::Full {
        children_hashes,
        value_hashes,
    })
}

// ---------------------------------------------------------------------------
// Bencodex element scanner
// ---------------------------------------------------------------------------

/// Enumerate top-level elements in a Bencodex list.
///
/// Returns `(start, end)` byte-offset pairs for each element's payload
/// (including its type prefix). The outer `l`...`e` wrapper is expected.
///
/// This skips over nested containers by tracking depth.
fn enumerate_list_elements(data: &[u8]) -> Result<Vec<(usize, usize)>> {
    if data.is_empty() || data[0] != b'l' {
        bail!("Not a Bencodex list (expected 'l' prefix)");
    }

    let mut pos = 1usize; // skip 'l'
    let len = data.len();
    let mut elements = Vec::new();

    while pos < len {
        if data[pos] == b'e' {
            break; // end of list
        }

        let elem_start = pos;
        let elem_end = skip_element(data, pos)?;
        elements.push((elem_start, elem_end));
        pos = elem_end;
    }

    Ok(elements)
}

/// Skip over one Bencodex element starting at `pos`, returning the position
/// immediately after the element.
fn skip_element(data: &[u8], pos: usize) -> Result<usize> {
    if pos >= data.len() {
        bail!("Unexpected end of data at position {pos}");
    }

    match data[pos] {
        // Null, Bool true, Bool false — single-byte values
        b'n' | b't' | b'f' => Ok(pos + 1),

        // Integer: i<digits>e
        b'i' => {
            let end = find_byte(data, pos + 1, b'e')
                .context("Unterminated Bencodex integer")?;
            Ok(end + 1)
        }

        // Text: u<length>:<data>
        b'u' => {
            let (len, after_len) = parse_length(data, pos + 1, b':')?;
            Ok(after_len + len)
        }

        // List: l<items>e
        b'l' => {
            let mut p = pos + 1;
            while p < data.len() && data[p] != b'e' {
                p = skip_element(data, p)?;
            }
            if p >= data.len() {
                bail!("Unterminated Bencodex list starting at position {pos}");
            }
            Ok(p + 1) // skip 'e'
        }

        // Dict: d<pairs>e
        b'd' => {
            let mut p = pos + 1;
            while p < data.len() && data[p] != b'e' {
                // skip key
                p = skip_element(data, p)?;
                // skip value
                if p < data.len() && data[p] != b'e' {
                    p = skip_element(data, p)?;
                }
            }
            if p >= data.len() {
                bail!("Unterminated Bencodex dict starting at position {pos}");
            }
            Ok(p + 1) // skip 'e'
        }

        // Bytes: <length>:<data>
        b'0'..=b'9' => {
            let (len, after_len) = parse_length(data, pos, b':')?;
            Ok(after_len + len)
        }

        other => bail!(
            "Unknown Bencodex tag byte 0x{:02x} ({:?}) at position {pos}",
            other,
            other as char
        ),
    }
}

/// Parse a decimal length prefix (e.g. `"32:"`) starting at `pos`.
/// Returns `(length, position_after_colon)`.
fn parse_length(data: &[u8], pos: usize, delimiter: u8) -> Result<(usize, usize)> {
    let start = pos;
    let mut p = pos;
    while p < data.len() && data[p].is_ascii_digit() {
        p += 1;
    }
    if p == start || p >= data.len() || data[p] != delimiter {
        bail!(
            "Expected length prefix at position {start} (got byte 0x{:02x})",
            data.get(p).copied().unwrap_or(0)
        );
    }
    let s = std::str::from_utf8(&data[start..p]).context("Invalid UTF-8 in length prefix")?;
    let length = s
        .parse::<usize>()
        .with_context(|| format!("Invalid length value: {s:?}"))?;
    Ok((length, p + 1))
}

/// Find the first occurrence of `byte` in `data` starting from `pos`.
fn find_byte(data: &[u8], pos: usize, byte: u8) -> Option<usize> {
    data[pos..].iter().position(|&b| b == byte).map(|i| pos + i)
}

/// Decode a Bencodex binary value (`<len>:<bytes>`) into its raw byte payload.
fn decode_binary_bytes(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        bail!("Empty binary data");
    }
    if !data[0].is_ascii_digit() {
        bail!(
            "Expected binary length prefix, got 0x{:02x}",
            data[0]
        );
    }
    let (len, after_colon) = parse_length(data, 0, b':')?;
    if after_colon + len > data.len() {
        bail!(
            "Binary data truncated: expected {len} bytes at position {after_colon}, have {}",
            data.len() - after_colon
        );
    }
    Ok(data[after_colon..after_colon + len].to_vec())
}

// ---------------------------------------------------------------------------
// Hash extraction
// ---------------------------------------------------------------------------

/// Extract all plausible 32-byte SHA256 hash references from Bencodex-encoded
/// trie node data.
///
/// This function scans the raw bytes for Bencodex binary values that are
/// exactly 32 bytes long — the signature of a child hash pointer in
/// Libplanet's Merkle Patricia Trie.
///
/// # How it works
///
/// A 32-byte hash in Bencodex is encoded as `"32:"` followed by 32 raw bytes.
/// The function walks the Bencodex structure looking for length-prefixed
/// binaries of exactly 32 bytes. A heuristic filter ([`is_valid_hash_candidate`])
/// reduces false positives.
///
/// # Example
///
/// ```ignore
/// // Bencodex encoding of a 32-byte hash: b"32:" + 32 bytes of 0xAB
/// let mut data = b"l32:".to_vec();
/// data.extend(std::iter::repeat(0xAB).take(32));
/// data.push(b'e');
/// let hashes = extract_child_hashes_from_bencodex(&data);
/// assert_eq!(hashes.len(), 1);
/// assert_eq!(hashes[0], [0xAB; 32]);
/// ```
pub fn extract_child_hashes_from_bencodex(data: &[u8]) -> Vec<NodeHash> {
    let mut hashes = Vec::new();
    scan_for_hashes(data, &mut hashes);
    hashes
}

/// Recursively scan Bencodex data for 32-byte binary values.
fn scan_for_hashes(data: &[u8], hashes: &mut Vec<NodeHash>) {
    let mut pos = 0usize;
    let len = data.len();

    while pos < len {
        match data[pos] {
            // Single-byte values: skip
            b'n' | b't' | b'f' => {
                pos += 1;
            }

            // Integer: skip to 'e'
            b'i' => {
                if let Some(e_pos) = find_byte(data, pos + 1, b'e') {
                    pos = e_pos + 1;
                } else {
                    return; // malformed
                }
            }

            // Text: u<length>:<data>
            b'u' => {
                match parse_length(data, pos + 1, b':') {
                    Ok((text_len, after_colon)) => {
                        pos = after_colon + text_len;
                    }
                    Err(_) => return, // malformed
                }
            }

            // List or Dict: recurse into nested containers
            b'l' | b'd' => {
                // Find matching 'e' by tracking nested depth
                if let Some(end) = find_container_end(data, pos) {
                    // Recurse into the container body (between opener and 'e')
                    scan_for_hashes(&data[pos + 1..end], hashes);
                    pos = end + 1;
                } else {
                    return; // malformed
                }
            }

            // Binary: <length>:<data>
            b'0'..=b'9' => {
                match parse_length(data, pos, b':') {
                    Ok((bin_len, after_colon)) => {
                        if after_colon + bin_len > len {
                            return; // truncated
                        }

                        // Check if this is a 32-byte hash candidate
                        if bin_len == HASH_SIZE
                            && is_valid_hash_candidate(data, after_colon)
                        {
                            let mut hash = [0u8; HASH_SIZE];
                            hash.copy_from_slice(&data[after_colon..after_colon + HASH_SIZE]);
                            hashes.push(hash);
                        }

                        pos = after_colon + bin_len;
                    }
                    Err(_) => return, // malformed
                }
            }

            // Unknown byte — skip one to avoid infinite loops
            _ => {
                pos += 1;
            }
        }
    }
}

/// Find the closing `'e'` for a Bencodex container (list or dict) starting at
/// `pos` (which should point to `'l'` or `'d'`). Returns the position of `'e'`.
fn find_container_end(data: &[u8], pos: usize) -> Option<usize> {
    if pos >= data.len() {
        return None;
    }
    let opener = data[pos];
    if opener != b'l' && opener != b'd' {
        return None;
    }

    let mut depth = 1i32;
    let mut p = pos + 1;
    while p < data.len() && depth > 0 {
        match data[p] {
            b'l' | b'd' => depth += 1,
            b'e' => {
                depth -= 1;
                if depth == 0 {
                    return Some(p);
                }
            }
            // Skip over integers
            b'i' => {
                if let Some(ep) = find_byte(data, p + 1, b'e') {
                    p = ep;
                } else {
                    return None;
                }
            }
            // Skip over length-prefixed values (binary or text)
            b'u' | b'0'..=b'9' => {
                // Determine the delimiter: ':' for both text and binary
                match parse_length(data, if data[p] == b'u' { p + 1 } else { p }, b':') {
                    Ok((item_len, after_colon)) => {
                        p = after_colon + item_len - 1; // -1 because loop increments
                    }
                    Err(_) => return None,
                }
            }
            _ => {}
        }
        p += 1;
    }

    if depth == 0 {
        Some(p)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Hash candidate heuristic
// ---------------------------------------------------------------------------

/// Heuristic filter to reduce false positives when scanning for 32-byte hashes.
///
/// A Bencodex binary of exactly 32 bytes *might* be a child hash reference, or
/// it might be opaque data. This function applies lightweight checks:
///
/// 1. The 32-byte value must be preceded by the literal `"32:"` at the correct
///    position in the Bencodex stream.
/// 2. The bytes should not be all-zeros (which would indicate an empty/unused slot
///    rather than a real hash).
/// 3. Optionally, surrounding bytes should look like valid Bencodex structure.
///
/// This is intentionally permissive — false negatives (missing a real hash) are
/// worse than false positives (including an extra candidate) for the pruning use case.
pub fn is_valid_hash_candidate(data: &[u8], pos: usize) -> bool {
    // Must have 32 bytes available
    if pos + HASH_SIZE > data.len() {
        return false;
    }

    // Check that preceding bytes form "32:"
    if pos < 3 && data.len() >= 3 {
        // Not enough room for "32:" prefix
        return false;
    }
    if pos >= 3 {
        let prefix = &data[pos - 3..pos];
        if prefix != b"32:" {
            // Might be a different length prefix — not a 32-byte binary
            return false;
        }
    } else {
        return false;
    }

    // Reject all-zeros (likely empty slot, not a real hash)
    let candidate = &data[pos..pos + HASH_SIZE];
    if candidate.iter().all(|&b| b == 0) {
        return false;
    }

    true
}

// ---------------------------------------------------------------------------
// Convenience helpers
// ---------------------------------------------------------------------------

/// Open a RocksDB state store at the given path and read a trie node by its hash.
///
/// The `states/` column family (or default CF) stores the trie nodes.
/// Keys are 32-byte SHA256 hashes; values are raw Bencodex node bytes.
///
/// Returns `Ok(None)` if the hash is not found in the database.
pub fn read_trie_node_from_db(
    db: &rocksdb::DB,
    hash: &NodeHash,
) -> Result<Option<TrieNode>> {
    let cf = db
        .cf_handle("states")
        .unwrap_or_else(|| db.cf_handle("default").expect("No 'states' or 'default' CF found"));

    match db.get_cf(&cf, hash) {
        Ok(Some(data)) => {
            let node = parse_trie_node(&data)
                .with_context(|| {
                    format!(
                        "Failed to parse trie node for hash {}",
                        hex::encode(hash)
                    )
                })?;
            Ok(Some(node))
        }
        Ok(None) => Ok(None),
        Err(e) => bail!("RocksDB error reading trie node: {e}"),
    }
}

/// Walk the trie from a root hash, collecting all reachable node hashes.
///
/// Uses the provided RocksDB handle to look up nodes. The `max_depth` parameter
/// limits recursion depth to prevent runaway traversal on corrupted data.
///
/// Returns the set of all hashes reachable from `root`, including `root` itself.
pub fn collect_reachable_hashes(
    db: &rocksdb::DB,
    root: &NodeHash,
    max_depth: usize,
) -> Result<Vec<NodeHash>> {
    use std::collections::HashSet;

    let mut visited: HashSet<NodeHash> = HashSet::new();
    let mut frontier: Vec<(NodeHash, usize)> = vec![(*root, 0)];
    visited.insert(*root);

    while let Some((hash, depth)) = frontier.pop() {
        if depth >= max_depth {
            tracing::warn!(
                "Max depth {max_depth} reached at hash {}",
                hex::encode(hash)
            );
            continue;
        }

        if let Some(node) = read_trie_node_from_db(db, &hash)? {
            for child_hash in node.all_child_hashes() {
                if visited.insert(*child_hash) {
                    frontier.push((*child_hash, depth + 1));
                }
            }
        }
    }

    Ok(visited.into_iter().collect())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Bencodex binary encoding: `<len>:<bytes>`.
    fn encode_binary(data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("{}:", data.len()).as_bytes());
        buf.extend_from_slice(data);
        buf
    }

    /// Build a Bencodex list: `l<items>e`.
    fn encode_list(items: &[Vec<u8>]) -> Vec<u8> {
        let mut buf = vec![b'l'];
        for item in items {
            buf.extend_from_slice(item);
        }
        buf.push(b'e');
        buf
    }

    /// Bencodex null: `n`.
    fn encode_null() -> Vec<u8> {
        vec![b'n']
    }

    #[test]
    fn test_extract_hash_from_list() {
        let hash = [0xAB_u8; 32];
        let data = encode_list(&[
            encode_binary(b"some_path"),
            encode_binary(&hash),
        ]);

        let hashes = extract_child_hashes_from_bencodex(&data);
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], hash);
    }

    #[test]
    fn test_extract_multiple_hashes_from_full_node() {
        let h1 = [0x11; 32];
        let h2 = [0x22; 32];
        let h3 = [0x33; 32];

        // FullNode: 16 children + value
        let mut items = Vec::new();
        for i in 0..17u8 {
            match i {
                0 => items.push(encode_binary(&h1)),
                7 => items.push(encode_binary(&h2)),
                15 => items.push(encode_binary(&h3)),
                _ => items.push(encode_null()),
            }
        }
        let data = encode_list(&items);

        let hashes = extract_child_hashes_from_bencodex(&data);
        assert_eq!(hashes.len(), 3);
        assert!(hashes.contains(&h1));
        assert!(hashes.contains(&h2));
        assert!(hashes.contains(&h3));
    }

    #[test]
    fn test_parse_short_node() {
        let path = vec![0x01, 0x02, 0x0A];
        let hash = [0xFF; 32];
        let data = encode_list(&[encode_binary(&path), encode_binary(&hash)]);

        let node = parse_trie_node(&data).unwrap();
        match node {
            TrieNode::Short {
                path: p,
                child_hashes,
            } => {
                assert_eq!(p, path);
                assert_eq!(child_hashes.len(), 1);
                assert_eq!(child_hashes[0], hash);
            }
            _ => panic!("Expected ShortNode, got {node:?}"),
        }
    }

    #[test]
    fn test_parse_value_node() {
        let value = b"my_state_data";
        let data = encode_list(&[encode_null(), encode_binary(value)]);

        let node = parse_trie_node(&data).unwrap();
        match node {
            TrieNode::Value { nested_hashes } => {
                // A plain string value has no nested 32-byte hashes
                assert!(nested_hashes.is_empty());
            }
            _ => panic!("Expected ValueNode, got {node:?}"),
        }
    }

    #[test]
    fn test_parse_full_node() {
        let h1 = [0xAA; 32];
        let h2 = [0xBB; 32];

        let mut items = Vec::new();
        for i in 0..17u8 {
            match i {
                3 => items.push(encode_binary(&h1)),
                12 => items.push(encode_binary(&h2)),
                16 => items.push(encode_binary(b"branch_value")),
                _ => items.push(encode_null()),
            }
        }
        let data = encode_list(&items);

        let node = parse_trie_node(&data).unwrap();
        match node {
            TrieNode::Full {
                children_hashes,
                value_hashes,
            } => {
                assert_eq!(children_hashes.len(), 2);
                assert!(children_hashes.contains(&h1));
                assert!(children_hashes.contains(&h2));
                assert!(value_hashes.is_empty());
            }
            _ => panic!("Expected FullNode, got {node:?}"),
        }
    }

    #[test]
    fn test_null_node() {
        let node = parse_trie_node(b"n").unwrap();
        assert_eq!(
            node,
            TrieNode::Value {
                nested_hashes: vec![]
            }
        );
    }

    #[test]
    fn test_is_valid_hash_candidate_rejects_zeros() {
        let mut data = b"l32:".to_vec();
        data.extend(std::iter::repeat(0x00).take(32));
        data.push(b'e');

        let hashes = extract_child_hashes_from_bencodex(&data);
        assert!(hashes.is_empty(), "All-zero data should be rejected");
    }

    #[test]
    fn test_is_valid_hash_candidate_accepts_real_hash() {
        let hash = [0xDE; 32];
        let data = encode_binary(&hash);

        // The "32:" prefix is at positions 0..3, hash starts at 3
        assert!(is_valid_hash_candidate(&data, 3));
    }

    #[test]
    fn test_nested_inline_hashes() {
        let inner_hash = [0xCC; 32];
        // ShortNode with an inline FullNode child
        let inner_full = encode_list(&[
            encode_null(),
            encode_binary(&inner_hash),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
            encode_null(),
        ]);

        let outer = encode_list(&[encode_binary(b"path"), inner_full]);
        let hashes = extract_child_hashes_from_bencodex(&outer);
        assert!(hashes.contains(&inner_hash));
    }

    #[test]
    fn test_empty_data_errors() {
        let result = parse_trie_node(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_value_scanning() {
        // A raw binary value at top level (not in a list)
        let hash = [0x42; 32];
        let data = encode_binary(&hash);

        let hashes = extract_child_hashes_from_bencodex(&data);
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], hash);
    }
}
