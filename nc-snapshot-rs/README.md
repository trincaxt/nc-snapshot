# nc-snapshot-rs

Native Rust pruner for Nine Chronicles Libplanet `TrieStateStore` (`states/` RocksDB).

## Why?

The current C# `CopyStates` bridge reads the **entire blockchain** (~132 GiB read, ~144 GiB write) to prune the `states/` directory. This takes 2-3 hours.

This Rust implementation reads **only the reachable nodes** (~35 GiB IO total) and completes in ~20-30 minutes.

| Metric | C# Bridge | Rust (this) |
|--------|-----------|-------------|
| IO Read | 132 GiB | ~34 GiB |
| IO Write | 144 GiB | ~1 GiB |
| Time | 2-3 hours | ~20-30 min |
| states/ before | ~34 GiB | ~34 GiB |
| states/ after | ~2-3 GiB | ~2-3 GiB |

## How It Works

1. **Collect** state root hashes from the last N blocks (from `chain/` RocksDB)
2. **Traverse** the Merkle Patricia Trie via DFS from each root
3. **Mark** all reachable nodes (stored in a `HashSet<[u8; 32]>`)
4. **Scan** all keys in `states/` and delete unreachable ones
5. **Compact** RocksDB to physically reclaim disk space

## Prerequisites

```bash
# Ubuntu/Debian
sudo apt-get install libclang-dev clang build-essential

# macOS
xcode-select --install

# Rust (stable 1.75+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Build

```bash
cargo build --release
```

## Usage

### Prune states/ (main operation)

```bash
# Auto-detect state roots from chain/ (last 2 blocks)
nc-snapshot prune --store-path ~/9c-blockchain

# Manual state root hashes
nc-snapshot prune --store-path ~/9c-blockchain \
  --roots abc123...def456 789012...345678

# Dry run (report without deleting)
nc-snapshot prune --store-path ~/9c-blockchain --dry-run

# Keep last 5 blocks instead of 2
nc-snapshot prune --store-path ~/9c-blockchain --keep-blocks 5
```

### Diagnostics (FASE 1)

```bash
# Inspect states/ DB structure and node types
nc-snapshot diagnose --store-path ~/9c-blockchain --max-nodes 100

# Inspect chain/ DB layout and key patterns
nc-snapshot diagnose-chain --store-path ~/9c-blockchain

# Verify reachable nodes without deleting
nc-snapshot verify --store-path ~/9c-blockchain
```

### Verbose logging

```bash
RUST_LOG=debug nc-snapshot prune --store-path ~/9c-blockchain
```

## Architecture

```
nc-snapshot-rs/
├── Cargo.toml
├── src/
│   ├── main.rs              # CLI entry point (clap)
│   ├── lib.rs               # Library exports
│   └── trie/
│       ├── mod.rs
│       ├── bencodex.rs       # Bencodex decoder/encoder
│       ├── node.rs           # TrieNode types (Short, Full, Value)
│       ├── pruner.rs         # DFS traversal + deletion + compaction
│       └── chain_reader.rs   # Extract state roots from chain/ DB
├── tests/
│   └── integration_test.rs   # Full prune cycle tests
└── benches/
    └── bencodex_bench.rs     # Decoder performance benchmarks
```

## Testing

```bash
# Unit tests
cargo test

# Integration tests (creates temp RocksDB instances)
cargo test --test integration_test

# Benchmarks
cargo bench --bench bencodex_bench
```

## Key Design Decisions

1. **In-place deletion** instead of copy-to-new-DB: Avoids doubling disk usage
2. **Column family awareness**: Handles CFs correctly (Libplanet may use them)
3. **Inline node traversal**: Recurses into embedded nodes, not just hash references
4. **Graceful error handling**: Warns on decode errors instead of crashing
5. **Dry-run mode**: Always test before actual pruning
6. **Progress reporting**: Real-time progress bars for long operations

---

## Development Log

### Phase 1: states/ DB Reverse Engineering (COMPLETE)

The `states/` RocksDB stores the Merkle Patricia Trie nodes for Nine Chronicles.

**Discoveries:**
- All keys are 32 bytes (SHA-256 hash of node data)
- Values are Bencodex-encoded `TrieNode` variants: `ShortNode`, `FullNode`, `Value`
- `ShortNode`: path (nibble-encoded) + child hash reference
- `FullNode`: 16 children (each `Option<Hash[32]>`) + optional value
- `Value`: raw byte payload
- ~110.7M total nodes, ~80.6 GiB total
- Decode error rate: 0.000002% (2 edge cases out of 110M)

**Performance:** ~10k keys/s scan rate on CachyOS.

### Phase 2: chain/ DB Reverse Engineering (COMPLETE)

The `chain/` RocksDB stores block metadata, indices, and state root hashes.

**Discoveries:**
- ~18.4M keys total
- Key format: binary-encoded (NOT text-based as assumed from C# source)
- 25-byte keys with 32-byte values = state root hash entries (~17.9M)
- Key structure: `[type_byte][chain_id/guid][index_bytes][suffix]`
- Multiple chain IDs present (different GUIDs in prefix)
- Values are 32-byte hashes that should exist as keys in `states/`
- Other key types: 1-byte (chain metadata), 17-byte, 33-byte, 37-byte entries

**Challenge:** The chain/ DB contains entries from multiple chains. Only the
active chain's state roots exist in `states/`. Detection uses sample-based
validation to find the correct chain prefix.

### Phase 3: State Root Detection Algorithm (COMPLETE)

**Problem:** How to find which 32-byte values in chain/ are the actual state
roots for the current chain?

**Solution:** Sample-based validation:
1. Collect all 32-byte value entries from chain/ (~17.9M candidates)
2. Sample 10K evenly-spread candidates
3. Check each against states/ DB (point lookup, ~0.1ms each)
4. If any match → extract common prefix → filter all candidates to that prefix
5. Return last N entries from the active chain
6. Fallback: 100K sample, then full scan

**Performance:** ~10 seconds total (8s chain scan + 1s sample + 1s filter)
vs. naive full scan: ~30 minutes.

---

## State Root Detection Algorithm

### How it works

```
chain/ DB (18.4M keys)
  └─ 17.9M entries with 32-byte values (candidates)
       └─ Sample 10K → validate against states/ DB
            └─ Match found → extract prefix
                 └─ Filter candidates by prefix
                      └─ Return last N (most recent blocks)
```

### Validation logic

A 32-byte value from chain/ is a valid state root if it exists as a key
in states/ (because states/ stores trie nodes indexed by their hash).

```rust
// Point lookup in states/ RocksDB
states_db.get(candidate_value)  // ~0.1ms per lookup
```

### Fallback chain

1. **10K sample** (~1s): Finds active chain if it has >100 entries in sample
2. **100K sample** (~10s): Finds active chain with >99.99% probability
3. **Full scan** (~30min): Validates all 17.9M candidates (last resort)
4. **Manual --roots**: User provides hex hashes directly

### Example output

```
INFO Found 17877319 entries with 32-byte values in chain/
INFO Sampling 10000 candidates (step=1787) to find active chain...
INFO Sample result: 5/10000 candidates exist in states/ DB
INFO Detected active chain prefix: 24 bytes (49000e42b831ad20...)
INFO Filtered to 5946 candidates with matching prefix (of 17877319 total)
INFO Returning last 2 roots from active chain (5946 total entries)
INFO State root: key_suffix=0x79 hash=89070e53de36e0bf...
INFO State root: key_suffix=0x7a hash=405ed77f6afd5edb...
```

---

## chain/ DB Structure Reference

### Key types discovered

| Key Len | Count | Value Len | Description |
|---------|-------|-----------|-------------|
| 1 byte  | 1     | 16 bytes  | Chain metadata (chain GUID?) |
| 17 bytes| 16,333| varies    | Unknown (possibly tx indices) |
| 25 bytes| 17.9M | 32 bytes  | **State root hashes** |
| 33 bytes| 3,026 | varies    | Unknown |
| 37 bytes| 479K  | varies    | Unknown (possibly blocks) |

### 25-byte key structure (state roots)

```
Byte 0:      Type marker (0x49 = 'I')
Bytes 1-16:  Chain/store GUID (16 bytes, varies per chain)
Bytes 17-23: Index/padding (7 bytes, appears constant per chain)
Byte 24:     Suffix (1 byte, sequential within chain)
```

### Value types in chain/

| Value Len | Count | Description |
|-----------|-------|-------------|
| 0 bytes   | 6,052 | Empty values |
| 8 bytes   | 485K  | Possibly timestamps/indices |
| 16 bytes  | 6,054 | Possibly GUIDs |
| 32 bytes  | 17.9M | **State root hashes** |
| 284-948 bytes | varies | Serialized block/tx data |

---

## Known Issues / Limitations

1. **State root detection may fail** if the chain/ DB doesn't match the
   states/ DB (e.g., from a different node version or pruned state).
   Workaround: use `--roots` to specify hashes manually.

2. **Phase 2 scan is slow** for large stores (~110M nodes).
   The scan iterates all keys in states/ to find unreachable nodes.
   This can take 5-10 minutes on large stores.

3. **GCC 15 compatibility** required upgrading rocksdb crate from 0.22
   to 0.24 (GCC 15 removed transitive `<cstdint>` includes).

4. **Multiple chains in chain/**: The chain/ DB may contain entries from
   multiple chains (different GUIDs). The sample-based algorithm finds
   the active chain by validating against states/.

5. **Column families**: The states/ DB typically has only a "default" CF.
   If Libplanet uses additional CFs, the tool may need updates.

---

## Troubleshooting

### "No valid roots found in states/ DB"

The chain/ DB and states/ DB don't match. This can happen if:
- The node was partially synced
- The states/ was pruned by a different tool
- The chain/ DB contains entries from an older chain

**Fix:** Get the state root hashes from your running node:
```bash
# From your Nine Chronicles node, get the latest state root:
# Then use --roots to specify it manually:
nc-snapshot prune --store-path ~/9c-blockchain --roots <hex_hash_1> <hex_hash_2>
```

### "Node not found in states/ DB" during prune

The detected state root hash doesn't exist in the states/ DB.
The prune will complete but won't find any reachable nodes (dangerous!).

**Fix:** Always use `--dry-run` first to verify the roots are correct.

### Build fails with "error occurred in cc-rs"

RocksDB C++ compilation error, usually caused by GCC version mismatch.
**Fix:** Ensure GCC 13+ or Clang is installed, and rocksdb crate is 0.24+.
