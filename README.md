# ⚡ nc-snapshot

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance snapshot engine for the **Nine Chronicles** blockchain. `nc-snapshot` generates consistent live snapshots from running nodes using a Rust-based State Trie Garbage Collection pipeline, multi-threaded archive generation, and official Libplanet validation.

---

## 🚀 Highlights

- **Live snapshots** without stopping the node
- **🦀 Rust snapshot engine** — export, BFS, GC, archive, verify
- **🌳 State Trie Garbage Collection** — removes unreachable trie nodes
- **💾 RocksDB secondary mode checkpoints** — consistent, captures memtable
- **⚡ mmap-based fixpoint BFS traversal** — low RAM footprint
- **📦 Multi-threaded** Zstandard compression
- **🔐 BLAKE3** integrity manifests
- **✅ Native** Rust validation
- **🔄 Production-tested** on the Odin network

---

## ❓ Why?

Long-running Nine Chronicles nodes accumulate millions of unreachable trie nodes inside the state database. The official snapshot workflow preserves these nodes during snapshot generation.

`nc-snapshot` introduces an additional garbage-collection stage before archive creation, producing smaller snapshots while remaining fully compatible with the official ecosystem.

---

## 📊 Official C# vs nc-snapshot — Same Hardware, Same GC Concept

The GC concept was first validated on a forked `NineChronicles.Snapshot` in C#, then ported to Rust. Same Ryzen 5 2400G, same live workflow.

| Stage | Official C# (forked) | nc-snapshot (Rust) | Speedup |
| :--- | :---: | :---: | :---: |
| Export | 77.6 min | 10.3 min | ~7.5× |
| BFS | 195.2 min | ~26 min | ~7.5× |
| Prune / Write | 67.9 min | ~10 min | ~6.8× |
| **GC Total** | **340.7 min** | **~46 min** | **~7.4×** |
| State Archive | 40.1 min | ~7–8 min | ~5× |
| **Total Run** | **392.4 min (~6h 32m)** | **55m 49s** | **~7×** |

> **Correctness equivalence:** Both pipelines converge to essentially the same live-node set — C# kept **66,240,058** nodes, Rust kept **66,292,704** (difference is chain progression between runs) — and both reduce `states/` to ~18 GiB. Identical result, ~7× faster on the same machine.
>
> *The two runs scanned different totals (C# 226.9M vs Rust 100.4M nodes) because they were taken at different chain states. The invariant that matters — the reachable live set — matches.*

---

## ⚙️ Install

### Requirements

- Rust 1.75+
- Linux x86_64
- Git
- A Nine Chronicles blockchain directory (e.g. `~/9c-blockchain`)

### Clone

```bash
git clone --recurse-submodules https://github.com/trincaxt/nc-snapshot.git
cd nc-snapshot
```

> If you already cloned the repository without submodules:

```bash
git submodule update --init --recursive
```

### Build

```bash
cargo build --release
```

Binary:

```text
./target/release/nc-snapshot
```

## 🛠️ Usage

> Use `--output-dir` to auto-name the archive by epoch. Use `-o` only for a fixed filename.


### Live partition/base snapshot

```bash
./target/release/nc-snapshot create \
  --live --prune \
  --mode partition \
  --apv "<APV>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots \
  --level 1 \
  --epoch-validate <N> \
  --epoch-limit <N+1>
```

### Verify integrity

```bash
./target/release/nc-snapshot verify ~/snapshots/state/state_latest.tar.zst
```

### Restore

```bash
# Partition first, then state on top
tar -I zstd -xf ~/snapshots/partition/snapshot-XXXXX-XXXXX.tar.zst -C ~/9c-blockchain/
tar -I zstd -xf ~/snapshots/state/state_latest.tar.zst -C ~/9c-blockchain/
```

### Key Flags

| Flag | Meaning |
| :--- | :--- |
| `--live` | Snapshot without stopping the node (uses checkpoints) |
| `--prune` | Run the State Trie GC pipeline before archiving |
| `--mode` | `state` \| `partition` \| `full` |
| `--level` | Zstd level 1–19 (1 recommended; SSTs already compressed) |
| `--epoch-validate <N>` | Only checkpoint epochs ≥ N |
| `--epoch-limit <N>` | Only archive epochs ≥ N (incremental base) |
| `--block-before <N>` | Tip offset for live consistency |

---

## 🔄 Pipeline

```
Production Node
      │
      ▼
Live RocksDB Checkpoint
(secondary mode, Rust 🦀)
      │
      ▼
Export states/  (Rust 🦀)
      │
      ▼
Fixpoint BFS traversal  (mmap, Rust 🦀)
      │
      ▼
State Trie GC / Prune  (Rust 🦀)
      │
      ▼
Native Rust Validation  (Rust 🦀)
      │
      ▼
Metadata Generation  (Rust 🦀)
      │
      ▼
tar.zst + BLAKE3 manifest
```

### Checkpoint strategy (100% Rust 🦀)

All checkpoints use **RocksDB secondary mode** — opens the source DB as a secondary instance, syncs with the primary's WAL and memtable via `try_catch_up_with_primary()`, then creates a consistent checkpoint. This captures data from the memtable that hasn't been flushed to SST files yet, ensuring tip lookups and metadata never break.

Previously this required the C# CheckpointBridge. Now it's 100% Rust using the native `rocksdb` crate.

---

## ⚡ Performance

All numbers from a single production run on the Odin network.

**Hardware:** AMD Ryzen 5 2400G (4c/8t) · 32 GB RAM · NVMe Gen3/4 · node running live during the snapshot.  
**Run Date:** 2026-07-09  
**Total Wall Time:** 55m 49s

| Stage | Result |
| :--- | :--- |
| Live checkpoint | Seconds (hard-link + consistent index) |
| Export `states/` | 615.7s (~10.3 min) · 100.4M entries · 85.5 GB |
| Fixpoint BFS | 21 passes · ~26 min |
| Prune | ~10 min |
| Official validation | Passed ✅ |

### Production Dataset

| Metric | Value |
| :--- | :--- |
| Network | Odin |
| Blockchain | ~360 GiB |
| State database | 62.30 GiB |
| Trie nodes scanned | 100,357,392 |
| Live nodes (kept) | 66,292,704 |
| Garbage removed | 34,064,688 |
| State root block | #18,936,103 |
| `states/` before prune | ~31 GiB |
| `states/` after prune | ~18.1 GiB |
| State snapshot (original) | 68.25 GiB |
| State snapshot (compressed) | 58.17 GiB |

> The GC operates on the `states/` folder specifically (~31 GiB), not the full 62.30 GiB state database. Both the official C# tool and nc-snapshot converge to the same pruned size (~18 GiB) and the same live-node set (~66.2M) — the strongest available proof of correctness equivalence.

---

## 🧠 Fixpoint BFS

The garbage collector walks the state trie from the tip state root and marks every reachable node. Instead of one full file scan per trie level (~33 scans), nc-snapshot keeps a single mutable working set and lets children discovered mid-scan be resolved in the same sequential pass. Because trie nodes are content-addressed (randomly ordered on disk), roughly half of each node's children lie ahead of it in the file and are captured for free.

This converges to the full live set in a bounded number of passes using only sequential I/O and a low, RAM-friendly footprint. No key→offset index is held in memory.

**Observed convergence (live nodes found, cumulative):**

| Pass | Cumulative Live Nodes |
| :---: | ---: |
| 6 | 64,346 |
| 9 | 8,422,926 |
| 11 | 39,348,095 |
| 13 | 60,461,410 |
| 16 | 66,218,429 |
| 20 | 66,292,704 |
| 21 | 0 new → **Fixpoint reached** |

---

## 🛡️ Safety

`nc-snapshot` never modifies the live blockchain database. All processing runs against RocksDB checkpoints created outside `9c-blockchain/`.

Every generated snapshot is:

1. Validated using native Rust RocksDB open
2. Restored into a clean test node
3. Synchronized against the live Odin network

Only then is the snapshot considered valid.

---

## 🧪 Testing

See **[TESTING.md](./TESTING.md)** for the full test strategy.

- **99 unit + integration tests**, zero external dependencies
- Tests cover parsers, Bencodex decoding, epoch filtering, metadata, live-keys
- The chain reader is tested against a synthetic RocksDB fixture (3 blocks)
- No test ever touches `~/9c-blockchain`
- `cargo test` completes in under a second

---

## 🚦 Current Status

| Component | Status |
| :--- | :--- |
| Snapshot Engine | ✅ Rust |
| Export | ✅ Rust |
| Fixpoint BFS | ✅ Rust |
| State GC / Prune | ✅ Rust |
| Archive Creation | ✅ Rust |
| Verification | ✅ Rust |
| Live Checkpoints | ✅ Rust 🦀 (Secondary Mode) |
| Metadata | ✅ Rust 🦀 (Bencodex) |
| State-root Read | ✅ Rust 🦀 (Chain Reader) |
| Validation | ✅ Rust 🦀 |
| Chain Reader | ✅ Rust 🦀 |

---

## 🗺️ Roadmap

**Completed:** Live snapshots · Rust snapshot engine · State Trie GC · Fixpoint mmap traversal · BLAKE3 verification · Multi-threaded compression · RocksDB secondary mode checkpoints · Rust Chain Reader (native Bencodex) · Rust metadata generation · Rust validation layer.

**100% Rust 🦀** — Zero C#/.NET runtime dependencies. Everything runs in a single native binary.

---

## 🎯 Design Goals

- Keep production nodes online
- Never touch the live database
- Produce deterministic snapshots
- Reduce storage usage
- Improve snapshot generation performance
- Maintain compatibility with Nine Chronicles

---

## 🤝 Acknowledgments

Inspired by the original [NineChronicles.Snapshot](https://github.com/planetarium/NineChronicles.Snapshot) by Planetarium. The State Trie Garbage Collection pipeline in `nc-snapshot` is an original engineering extension designed to improve snapshot performance while preserving compatibility with the official ecosystem.

---

## 📄 License

MIT
