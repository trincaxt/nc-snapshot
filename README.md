# ⚡ nc-snapshot

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance snapshot engine for the **Nine Chronicles** blockchain.

`nc-snapshot` generates consistent live snapshots from running nodes using a Rust-based State Trie Garbage Collection pipeline, multi-threaded archive generation, and official Libplanet validation.

---

## Highlights

- 🚀 Live snapshots without stopping the node
- 🦀 Rust implementation
- 🌳 State Trie Garbage Collection
- 💾 RocksDB checkpoints
- ⚡ mmap-based BFS traversal
- 📦 Multi-threaded Zstandard compression
- 🔐 BLAKE3 integrity manifests
- ✅ Official Libplanet validation
- 🔄 Production-tested on the Odin network

---

# Why?

Long-running Nine Chronicles nodes accumulate millions of unreachable trie nodes inside the state database.

The official snapshot workflow preserves these nodes during snapshot generation.

`nc-snapshot` introduces an additional garbage collection stage before archive creation, producing cleaner snapshots while remaining fully compatible with the official ecosystem.

---

# Pipeline

```text
Production Node
        │
        ▼
 Live RocksDB Checkpoint
        │
        ▼
 Export (Rust)
        │
        ▼
 BFS Traversal (mmap)
        │
        ▼
 State Trie GC
        │
        ▼
 Official Libplanet Validation
        │
        ▼
 Snapshot Builder
        │
        ▼
 tar.zst + BLAKE3
```

---

# Architecture Comparison

```text
Official

CopyStates
     │
     ▼
 Archive



nc-snapshot

Checkpoint
     │
     ▼
Export
     │
     ▼
BFS
     │
     ▼
Prune
     │
     ▼
Validate
     │
     ▼
Archive
```

---

# Performance

Benchmarks collected from a production Odin node.

| Metric | NineChronicles.Snapshot (GC) | nc-snapshot |
|---------|---------:|------------:|
| Total Snapshot | ~2h14m | **1h16m** |
| GC Pipeline | ~109 min | **35 min** |
| Pruning | ~14.7 min | **10.3 min** |

---

# Production Dataset

| Metric | Value |
|---------|------:|
| Blockchain | ~360 GiB |
| State Database | ~31 GiB |
| Trie Nodes Scanned | 100,357,392 |
| Live Nodes | 66,292,704 |
| Garbage Removed | 34,064,688 |
| Restored Database | ~245 GiB |

---

# Example Output

```text
🔍 Running GC Pipeline...

✅ Prune complete
   Kept nodes    : 66,292,704
   Deleted nodes : 34,064,688
   Scanned nodes : 100,357,392
   Duration      : 10.3 min

✅ Validation complete (Official Libplanet)

📦 State Snapshot
   Original      : 68.25 GiB
   Compressed    : 58.17 GiB
   Compression   : 14.8%

🧹 Cleanup complete
```

---

# Safety

`nc-snapshot` never modifies the live blockchain database.

All processing is performed against RocksDB checkpoints.

Every generated snapshot is:

- validated using the official Libplanet implementation;
- restored into a clean test node;
- synchronized against the live Odin network.

Only then is the snapshot considered valid.

---

# Current Status

| Component | Status |
|------------|--------|
| Snapshot Engine | ✅ Rust |
| Export | ✅ Rust |
| BFS | ✅ Rust |
| State GC | ✅ Rust |
| Snapshot Creation | ✅ Rust |
| Verification | ✅ Rust |
| Checkpoints | ✅ C# |
| Metadata | ✅ C# |
| Validation | ✅ Official Libplanet |
| Chain Reader | 🚧 Rust (WIP) |

---

# Roadmap

### Completed

- Live snapshots
- Rust snapshot engine
- State Trie GC
- mmap traversal
- BLAKE3 verification
- Multi-threaded compression

### In Progress

- Rust Chain Reader (Official Bencodex)
- Rust metadata generation

### Future

- Remove remaining C# bridge components
- Native Rust validation layer
- Preserve compatibility with official Libplanet validation

---

# Design Goals

- Keep production nodes online
- Never touch the live database
- Produce deterministic snapshots
- Reduce storage usage
- Improve snapshot generation performance
- Maintain compatibility with Nine Chronicles

---

# Acknowledgments

This project was inspired by the original **NineChronicles.Snapshot** developed by Planetarium.

The State Trie Garbage Collection pipeline implemented in `nc-snapshot` is an original engineering extension designed to improve snapshot generation performance while preserving compatibility with the official ecosystem.

---

# License

MIT
