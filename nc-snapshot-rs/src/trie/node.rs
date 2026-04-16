//! Trie node types for Libplanet's Merkle Patricia Trie.
//!
//! The states/ RocksDB stores a Merkle Patricia Trie where:
//! - Keys are SHA256 hashes (32 bytes) of the Bencodex-encoded node
//! - Values are Bencodex-encoded node data
//!
//! Node types (matching Libplanet.Mpt.Nodes):
//! - **ShortNode** (leaf or extension): `[path_bytes, child_ref]`
//!   - path_bytes: nibble-encoded path segment
//!   - child_ref: either a 32-byte hash (HashNode) or an inline Bencodex value
//!
//! - **FullNode** (branch): `[c0, c1, ..., c15, value]` (17 elements)
//!   - c0..c15: child references (null if empty, hash bytes, or inline node)
//!   - value: optional leaf value at this branch point
//!
//! - **ValueNode** (leaf value): raw bytes stored as-is
//!
//! - **HashNode** (reference): 32-byte SHA256 hash pointing to another node in RocksDB

use crate::trie::bencodex::{self, BencodexValue};
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};

/// SHA256 hash used as node identifier in the trie.
pub type NodeHash = [u8; 32];

/// A decoded trie node.
#[derive(Debug, Clone)]
pub enum TrieNode {
    /// Short node (leaf or extension path).
    /// Contains a nibble path segment and a reference to a child.
    Short {
        /// Nibble-encoded path segment.
        path: Vec<u8>,
        /// Reference to child node.
        child: NodeRef,
    },

    /// Full node (branch with 16 children + optional value).
    /// Index 0-15 correspond to hex nibbles 0x0-0xF.
    /// Index 16 is the optional value stored at this branch point.
    Full {
        /// 16 child slots, None if empty.
        children: [Option<NodeRef>; 16],
        /// Optional raw value at this branch point (for Bytes values).
        value: Option<Vec<u8>>,
        /// Optional node reference at the value slot (index 16).
        /// Used when the value slot contains a ValueNode, HashNode, or
        /// inline node that may have child hash references.
        value_ref: Option<NodeRef>,
    },

    /// Value node (leaf data). We store nested Trie hashes discovered.
    Value(Vec<NodeHash>),
}

/// A reference to another trie node. Can be either:
/// - A hash reference (lazy load from RocksDB)
/// - An inline node (small nodes embedded directly in parent)
#[derive(Debug, Clone)]
pub enum NodeRef {
    /// Hash reference to a node stored separately in RocksDB.
    Hash(NodeHash),
    /// Small node embedded directly in the parent node's encoding.
    /// Libplanet inlines nodes whose Bencodex encoding is < 32 bytes.
    Inline(Box<TrieNode>),
}

impl TrieNode {
    /// Decode a raw Bencodex-encoded byte slice into a TrieNode.
    ///
    /// This handles all node types used by Libplanet's TrieStateStore:
    /// - List with 2 elements -> ShortNode
    /// - List with 17 elements -> FullNode
    /// - Bytes (raw) -> ValueNode or HashNode reference
    pub fn decode(raw: &[u8]) -> Result<Self> {
        let benc = bencodex::decode(raw)
            .context("Failed to decode Bencodex for trie node")?;

        Self::from_bencodex(&benc)
            .context("Failed to convert Bencodex to TrieNode")
    }

    /// Convert a Bencodex value to a TrieNode.
    ///
    /// Matches the logic in Libplanet's `NodeDecoder.Decode()`:
    /// - `List(17)` → FullNode (branch with 16 children + value)
    /// - `List(2)` with `list[0]` == Bytes → ShortNode (extension/leaf path)
    /// - `List(2)` with `list[0]` == Null → **ValueNode** (leaf value wrapper)
    /// - `Bytes` → ValueNode (raw) or HashNode (if used as child ref)
    /// - `Null` → empty ValueNode
    pub fn from_bencodex(benc: &BencodexValue) -> Result<Self> {
        match benc {
            BencodexValue::List(items) => {
                match items.len() {
                    2 => {
                        // Distinguish between ShortNode and ValueNode:
                        // - ShortNode: list[0] is Bytes (path nibbles)
                        // - ValueNode: list[0] is Null (value wrapper: [null, value])
                        match &items[0] {
                            BencodexValue::Bytes(_) => Self::decode_short_node(items),
                            BencodexValue::Null => {
                                // ValueNode format: [Null, <actual_value>]
                                // The actual value at items[1] can be any Bencodex type.
                                // For pruning, we don't need the value contents — just
                                // need to identify any hash references within it.
                                Self::decode_value_node_from_list(items)
                            }
                            other => bail!(
                                "List(2) with unexpected first element type: {:?} \
                                 (expected Bytes for ShortNode or Null for ValueNode)",
                                other
                            ),
                        }
                    }
                    // FullNode: [c0, c1, ..., c15, value]
                    17 => Self::decode_full_node(items),
                    n => bail!("Unexpected list length {n} for trie node (expected 2 or 17)"),
                }
            }
            // Raw bytes → ValueNode, usually no nested tries inside raw data (unless it's bencodex bytes directly, but standard is null,val)
            BencodexValue::Bytes(b) => {
                let mut hashes = Vec::new();
                if b.len() == 32 {
                    let mut h = [0u8; 32];
                    h.copy_from_slice(b);
                    hashes.push(h);
                }
                Ok(TrieNode::Value(hashes))
            }
            // Null → empty ValueNode
            BencodexValue::Null => Ok(TrieNode::Value(vec![])),
            other => bail!("Unexpected Bencodex type for trie node: {other:?}"),
        }
    }

    /// Decode a ValueNode from `[Null, <value>]` format.
    ///
    /// In Libplanet, `ValueNode` stores arbitrary Bencodex data wrapped as
    /// `[Null, value]`. The Null marker distinguishes it from ShortNode `[path, child]`.
    ///
    /// For pruning purposes, the value content doesn't contain child hash references
    /// (it's actual state data), so we store the raw Bencodex encoding.
    fn decode_value_node_from_list(items: &[BencodexValue]) -> Result<Self> {
        // items[0] is Null (already verified by caller)
        // items[1] is the actual value
        let mut hashes = Vec::new();
        if items.len() > 1 {
            Self::extract_nested_hashes(&items[1], &mut hashes);
        }
        Ok(TrieNode::Value(hashes))
    }

    /// Extract account state root hashes from metadata dictionaries.
    ///
    /// In Libplanet, the metadata node at the empty key is a Bencodex Dictionary
    /// mapping account addresses (32-byte Binary keys) to their state root hashes
    /// (32-byte Binary values). Only Dictionary VALUES at this level are hashes
    /// we need to follow.
    ///
    /// IMPORTANT: We do NOT recurse into Lists or other structures, because
    /// game state data can contain arbitrary 32-byte values that are NOT trie
    /// references. This was the root cause of the "Node not found" flood —
    /// millions of false positive hash extractions from game state data.
    fn extract_nested_hashes(benc: &BencodexValue, out: &mut Vec<NodeHash>) {
        match benc {
            BencodexValue::Dict(d) => {
                // Only extract from Dictionary VALUES — these are account state roots
                for (_, v) in d {
                    if let BencodexValue::Bytes(b) = v {
                        if b.len() == 32 {
                            let mut hash = [0u8; 32];
                            hash.copy_from_slice(b);
                            out.push(hash);
                        }
                    }
                    // Don't recurse deeper — one level of Dict values is enough
                    // for account state roots. Going deeper risks false positives.
                }
            }
            // Don't extract from Lists, raw Bytes, or other types.
            // Game state data can have arbitrary 32-byte values.
            _ => {}
        }
    }

    /// Decode a ShortNode from a 2-element Bencodex list.
    /// Format: `[path_bytes, child_ref]` where path_bytes is Binary.
    fn decode_short_node(items: &[BencodexValue]) -> Result<Self> {
        let path = items[0]
            .as_bytes()
            .context("ShortNode path must be Bytes")?
            .to_vec();

        let child = NodeRef::from_bencodex(&items[1])
            .context("Failed to decode ShortNode child")?;

        Ok(TrieNode::Short { path, child })
    }

    /// Decode a FullNode from a 17-element Bencodex list.
    ///
    /// Matches `NodeDecoder.DecodeFull()`:
    /// - Children slots 0-15: decoded with `FullChildNodeType`
    ///   (Null | Value | Short | Full | Hash)
    /// - Value slot 16: decoded with `FullValueNodeType`
    ///   (Null | Value | Hash)
    fn decode_full_node(items: &[BencodexValue]) -> Result<Self> {
        let mut children: [Option<NodeRef>; 16] = Default::default();

        for i in 0..16 {
            children[i] = match &items[i] {
                BencodexValue::Null => None,
                other => Some(
                    NodeRef::from_bencodex(other)
                        .with_context(|| format!("Failed to decode FullNode child[{i}]"))?,
                ),
            };
        }

        // Value slot (index 16): can be Null, a ValueNode [Null, val], a HashNode,
        // or any other node type. We only care about hash references for traversal.
        let value = match &items[16] {
            BencodexValue::Null => None,
            BencodexValue::Bytes(b) => Some(b.clone()),
            // Value slot can contain a List (ValueNode [Null, val]), Dict, or other types.
            // For pruning, we need to check if it contains any hash references.
            // The value at slot 16 is an INode, so we attempt to decode it as NodeRef.
            other => {
                // Slot 16 can be a ValueNode [Null, val], HashNode, inline node, etc.
                // Parse as NodeRef to capture any embedded hash references.
                match NodeRef::from_bencodex(other) {
                    Ok(node_ref) => {
                        return Ok(TrieNode::Full { children, value: None, value_ref: Some(node_ref) });
                    }
                    Err(_) => None, // Not a recognized node format, treat as opaque data
                }
            }
        };

        Ok(TrieNode::Full { children, value, value_ref: None })
    }

    /// Collect all child hash references from this node.
    /// This is used by the pruner to traverse the trie.
    pub fn child_hashes(&self) -> Vec<NodeHash> {
        let mut hashes = Vec::new();
        self.collect_child_hashes(&mut hashes);
        hashes
    }

    /// Recursively collect all hash references, including from inline nodes.
    fn collect_child_hashes(&self, out: &mut Vec<NodeHash>) {
        match self {
            TrieNode::Short { child, .. } => {
                child.collect_hashes(out);
            }
            TrieNode::Full { children, value_ref, .. } => {
                for child in children.iter().flatten() {
                    child.collect_hashes(out);
                }
                // Also collect hashes from the value slot (index 16)
                if let Some(vref) = value_ref {
                    vref.collect_hashes(out);
                }
            }
            TrieNode::Value(nested_hashes) => {
                out.extend_from_slice(nested_hashes);
            }
        }
    }
}

impl NodeRef {
    /// Decode a NodeRef from a Bencodex value.
    ///
    /// In Libplanet, a child reference can be:
    /// - 32 bytes -> HashNode (reference to another node in RocksDB)
    /// - Null -> empty (no child)
    /// - List/other -> inline node (small node embedded in parent)
    pub fn from_bencodex(benc: &BencodexValue) -> Result<Self> {
        match benc {
            BencodexValue::Bytes(b) if b.len() == 32 => {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(b);
                Ok(NodeRef::Hash(hash))
            }
            BencodexValue::Bytes(b) => {
                // Non-32-byte bytes: could be a value node or needs further interpretation
                // In Libplanet, if it's not exactly 32 bytes, treat as inline value node
                let mut hashes = Vec::new();
                if b.len() == 32 {
                    let mut h = [0u8; 32];
                    h.copy_from_slice(b);
                    hashes.push(h);
                }
                Ok(NodeRef::Inline(Box::new(TrieNode::Value(hashes))))
            }
            BencodexValue::List(_) => {
                // Inline node: decode the list as a trie node directly
                let node = TrieNode::from_bencodex(benc)
                    .context("Failed to decode inline trie node")?;
                Ok(NodeRef::Inline(Box::new(node)))
            }
            BencodexValue::Null => {
                // Null reference: no child. This shouldn't normally be called
                // but handle gracefully.
                Ok(NodeRef::Inline(Box::new(TrieNode::Value(vec![]))))
            }
            other => bail!("Unexpected Bencodex type for NodeRef: {other:?}"),
        }
    }

    /// Collect hash references from this NodeRef (including recursing into inline nodes).
    fn collect_hashes(&self, out: &mut Vec<NodeHash>) {
        match self {
            NodeRef::Hash(h) => out.push(*h),
            NodeRef::Inline(node) => node.collect_child_hashes(out),
        }
    }
}

/// Compute the SHA256 hash of raw bytes (used to verify node hashes).
pub fn sha256(data: &[u8]) -> NodeHash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::bencodex::{encode, BencodexValue};

    #[test]
    fn test_decode_value_node() {
        let raw_data = b"hello world";
        let benc = BencodexValue::Bytes(raw_data.to_vec());
        let encoded = encode(&benc);

        let node = TrieNode::decode(&encoded).unwrap();
        match node {
            TrieNode::Value(v) => assert_eq!(v, raw_data),
            _ => panic!("Expected ValueNode"),
        }
    }

    #[test]
    fn test_decode_short_node_with_hash_ref() {
        // ShortNode: [path_bytes, 32_byte_hash]
        let path = vec![0x01, 0x02, 0x03];
        let hash = [0xAB_u8; 32];
        let benc = BencodexValue::List(vec![
            BencodexValue::Bytes(path.clone()),
            BencodexValue::Bytes(hash.to_vec()),
        ]);
        let encoded = encode(&benc);

        let node = TrieNode::decode(&encoded).unwrap();
        match node {
            TrieNode::Short { path: p, child } => {
                assert_eq!(p, path);
                match child {
                    NodeRef::Hash(h) => assert_eq!(h, hash),
                    _ => panic!("Expected HashNode ref"),
                }
            }
            _ => panic!("Expected ShortNode"),
        }
    }

    #[test]
    fn test_decode_short_node_with_inline_value() {
        // ShortNode with inline value (non-32-byte bytes)
        let path = vec![0x01, 0x02];
        let value = vec![0x0A, 0x0B, 0x0C]; // 3 bytes, not 32
        let benc = BencodexValue::List(vec![
            BencodexValue::Bytes(path.clone()),
            BencodexValue::Bytes(value.clone()),
        ]);
        let encoded = encode(&benc);

        let node = TrieNode::decode(&encoded).unwrap();
        match node {
            TrieNode::Short { child, .. } => {
                match child {
                    NodeRef::Inline(inner) => {
                        match *inner {
                            TrieNode::Value(v) => assert_eq!(v, value),
                            _ => panic!("Expected inline ValueNode"),
                        }
                    }
                    _ => panic!("Expected Inline ref"),
                }
            }
            _ => panic!("Expected ShortNode"),
        }
    }

    #[test]
    fn test_decode_full_node() {
        // FullNode: [null, hash, null, ...(13 more nulls), value]
        let hash = [0xCD_u8; 32];
        let mut items = vec![BencodexValue::Null; 17];
        items[1] = BencodexValue::Bytes(hash.to_vec()); // child at nibble 1
        items[16] = BencodexValue::Bytes(b"leaf_value".to_vec()); // value
        let benc = BencodexValue::List(items);
        let encoded = encode(&benc);

        let node = TrieNode::decode(&encoded).unwrap();
        match node {
            TrieNode::Full { children, value, .. } => {
                // Only child[1] should be set
                assert!(children[0].is_none());
                assert!(children[1].is_some());
                for i in 2..16 {
                    assert!(children[i].is_none(), "child[{i}] should be None");
                }
                // Check child hash
                match &children[1] {
                    Some(NodeRef::Hash(h)) => assert_eq!(h, &hash),
                    _ => panic!("Expected Hash ref at child[1]"),
                }
                // Check value
                assert_eq!(value, Some(b"leaf_value".to_vec()));
            }
            _ => panic!("Expected FullNode"),
        }
    }

    #[test]
    fn test_child_hashes_collects_all() {
        // FullNode with 3 hash children
        let h1 = [0x11_u8; 32];
        let h2 = [0x22_u8; 32];
        let h3 = [0x33_u8; 32];
        let mut items = vec![BencodexValue::Null; 17];
        items[0] = BencodexValue::Bytes(h1.to_vec());
        items[5] = BencodexValue::Bytes(h2.to_vec());
        items[15] = BencodexValue::Bytes(h3.to_vec());
        items[16] = BencodexValue::Null; // no value

        let node = TrieNode::from_bencodex(&BencodexValue::List(items)).unwrap();
        let hashes = node.child_hashes();
        assert_eq!(hashes.len(), 3);
        assert!(hashes.contains(&h1));
        assert!(hashes.contains(&h2));
        assert!(hashes.contains(&h3));
    }

    #[test]
    fn test_child_hashes_includes_inline_children() {
        // ShortNode -> Inline FullNode -> Hash children
        // This tests the CRITICAL inline traversal that your original plan missed
        let inner_hash = [0xFF_u8; 32];
        let mut inner_items = vec![BencodexValue::Null; 17];
        inner_items[3] = BencodexValue::Bytes(inner_hash.to_vec());

        let outer = BencodexValue::List(vec![
            BencodexValue::Bytes(vec![0x01]),
            BencodexValue::List(inner_items), // inline FullNode
        ]);

        let node = TrieNode::from_bencodex(&outer).unwrap();
        let hashes = node.child_hashes();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], inner_hash);
    }

    #[test]
    fn test_sha256_hash() {
        let data = b"test data";
        let hash = sha256(data);
        assert_eq!(hash.len(), 32);
        // SHA256 of "test data" is deterministic
        let hash2 = sha256(data);
        assert_eq!(hash, hash2);
    }
}
