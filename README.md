# ⚡ nc-snapshot

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![XERJ Corpus Hub contributor](https://hub.xerj.org/contributors/trincaxt/badge.svg)](https://hub.xerj.org/contributors/trincaxt)

A high-performance snapshot engine for the **Nine Chronicles** blockchain. `nc-snapshot` generates consistent live snapshots from running nodes using a Rust-based State Trie Garbage Collection pipeline, multi-threaded archive generation, and official Libplanet validation.

---

## 🚀 Highlights

- **Live snapshots** without stopping the node
- **🦀 100% Native Rust engine** — zero .NET/C# runtime dependencies
- **⚡ Direct 3-Scan GC Pipeline (`--scan-gc`)** — zero intermediate export file, saving ~70–100 GB of scratch disk I/O
- **🎯 Compact 12-Byte KeyIndex** — reduces index RAM footprint by 62.5% (saving ~7 GB on large states) with mathematical collision detection ($10^{-19}$ probability)
- **🧠 In-Memory CSR Graph Traversal** — 33 levels of BFS traversing 1.12 BILLION edges in **7.4 seconds**
- **🌳 State Trie Garbage Collection** — prunes up to **80% of unreachable historical state**
- **💾 RocksDB secondary mode checkpoints** — instant, consistent, memtable-aware
- **📦 Multi-threaded** Zstandard compression & **🔐 BLAKE3** manifests
- **🔄 Production-proven** on the Nine Chronicles Odin network (up to **342M+ nodes processed in 12.6 min**)

---

## ❓ Why?

Long-running Nine Chronicles nodes accumulate millions of unreachable trie nodes inside the state database. The official snapshot workflow preserves these nodes during snapshot generation.

`nc-snapshot` introduces an additional garbage-collection stage before archive creation, producing smaller snapshots while remaining fully compatible with the official ecosystem.

---

## 📊 The Evolution: From 6 Hours (C#) to 6 Minutes (Rust 3-Scan)

The GC concept was first validated on a forked `NineChronicles.Snapshot` in C#, then rewritten in Rust across three iterative architectural generations:

| Stage | Official C# (forked) | Rust v1 (Fixpoint) | Rust v2 (CSR + Export) | Rust v3 (`--scan-gc`) | Speedup (v3 vs C#) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| Scratch Disk I/O | ~100 GB | ~85 GB | ~70 GB | **0 GB (None!)** | **Eliminated** |
| Scan / Export | 77.6 min | 10.3 min | 7.1 min | **1.8 min** (107.9s) | **~43×** |
| Graph / BFS | 195.2 min | ~26 min | 1.6 min | **2.1 min** (Scan 2 + BFS) | **~90×** |
| In-Memory BFS only | N/A | N/A | < 1 min | **3.5 seconds** | **~3,300×** |
| Prune / Write | 67.9 min | ~10 min | 8.8 min | **2.1 min** (130.4s) | **~32×** |
| **GC Total** | **340.7 min (~5.7h)** | **~46 min** | **~17.5 min** | **6.1 min** | **🔥 ~55× faster** |

> **Correctness equivalence:** All pipelines produce the exact same reachable live-node set down to the exact bit. Rust v3 (`--scan-gc`) operates directly against the RocksDB iterator with zero intermediate scratch files, turning a 6-hour maintenance nightmare into a 6-minute background job.

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


### Live partition/base snapshot (with Direct 3-Scan GC)

```bash
./target/release/nc-snapshot create \
  --live --prune --scan-gc \
  --mode partition \
  --apv "<APV>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots \
  --level 3 \
  --epoch-validate <N> \
  --epoch-limit <N>
```

### Benchmark RocksDB scan throughput without modifying anything

```bash
./target/release/examples/scan_bench ~/9c-blockchain/states
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
| `--scan-gc` | **Direct 3-scan GC on RocksDB** — zero scratch files, runs in ~6 min (~10 GB RAM) |
| `--live` | Snapshot without stopping the node (uses secondary checkpoints) |
| `--prune` | Run State Trie GC before archiving (removes historical unreferenced state) |
| `--export-dir <DIR>` | Custom disk directory for temporary GC scratch files (when running without `--scan-gc`) |
| `--checkpoint-dir <DIR>`| Custom directory for RocksDB live checkpoints (defaults to same filesystem) |
| `--mode` | `state` \| `partition` \| `full` |
| `--level` | Zstd level 1–19 (3 recommended for optimal speed vs ratio) |
| `--epoch-validate <N>` | Only link and checkpoint epochs ≥ N |
| `--epoch-limit <N>` | Only archive epochs ≥ N (incremental partition) |
| `--block-before <N>` | Tip offset for live consistency (default: 1) |

---

## 🔄 Pipelines

`nc-snapshot` supports two state pruning engines depending on available hardware:

### 1. Direct 3-Scan Pipeline (`--scan-gc`) — *Recommended*
Operates directly against the RocksDB secondary checkpoint without generating any intermediate export file (~100 GB disk I/O eliminated):

```
Live RocksDB Checkpoint (Source)
      │
      ├──► Scan 1: Sequential Keys (1.4M keys/s)  ──► KeyIndex in RAM (32 B/key)
      │
      ├──► Scan 2: Parallel Values (Rayon)        ──► CSR Graph in RAM (4 B/edge)
      │                                                     │
      │                                              In-Memory BFS (3.5s)
      │                                                     │
      │                                                     ▼
      └──► Scan 3: Filtered Bulk Write ◄──────────── LiveSet (1 bit/node)
                  │
                  ▼
          states_gc/ (Compacted RocksDB, Libplanet format_version 5)
```

### 2. Export-based Pipeline (Low RAM)
For memory-constrained environments (<8 GB RAM), streams keys/values into `states_export.bin` on disk (optionally redirected via `--export-dir` to an external NVMe or HDD), builds an indexed CSR graph, and copies live nodes.

---

## ⚡ Performance

### 🏆 Benchmark 1: 342.4 Million Nodes & 1.12 Billion Edges in 12.6 Minutes (80% Pruned)

Executed live on a production validator node on the Nine Chronicles Odin network with the **Compact 12-Byte KeyIndex**:

**Hardware:** AMD Ryzen 7 5700G (8c/16t) · 32 GB RAM · Kingston NVMe Gen4 · Live node running  
**Run Date:** November 2026  
**Mode:** `--live --prune --scan-gc --mode partition --epoch-limit 20772`

| Pipeline Phase | Throughput | Time | Impact |
| :--- | :---: | :---: | :--- |
| **Live Secondary Checkpoint** | Memtable catchup + hardlinks | **1.4s** | Zero consensus downtime |
| **Scan 1: Compact KeyIndex** | **1,531,000 keys/sec** | **223.6s** (~3.7 min) | **4.18 GB RAM** (saved ~7 GB!) |
| **Scan 2: CSR Graph Construction** | **1,120,795,084 edges mapped** | **265.1s** (~4.4 min) | 7.22 GB RAM for 1.12B edges |
| **In-Memory BFS (33 Levels)** | **46,200,000 nodes/sec** | **🔥 7.4s** | 68.9M live nodes identified |
| **Scan 3: Prune & Bulk Load** | 68,909,780 live nodes written | **256.8s** (~4.2 min) | Bulk load, auto-compaction off |
| **Phase 4: RocksDB Validation** | Checkpoint integrity check | **0.0s** | Instant verification |
| **Total State GC Wall Time** | **342.4M → 68.9M nodes** | **⏱️ 12.6 min** | **🔥 79.9% (273.5M dead nodes deleted)** |
| **Partition Snapshot (`.tar.zst`)** | 278 files (2.13 GiB → 1.19 GiB) | **30.1s** (72 MB/s) | Blake3 manifest verified |
| **State Snapshot (`.tar.zst`)** | 1,094 files (72.69 GiB → 60.24 GiB) | **9.2 min** (134 MB/s) | Tip block #20409105 |

---

### 🥈 Benchmark 2: 152.4 Million Nodes in 6.1 Minutes (55% Pruned)

| Metric | Value |
| :--- | :--- |
| **Total Nodes Scanned** | 152,443,110 trie nodes |
| **Live Nodes Kept** | 68,626,304 nodes |
| **Garbage Deleted** | 83,816,806 nodes (55.0% pruned) |
| **Scan 1 (Keys)** | 107.9s (1.41M keys/sec) |
| **Scan 2 (CSR Graph)** | 126.9s (393M edges) |
| **In-Memory BFS (33 Levels)** | **3.5 seconds** |
| **Scan 3 (Prune)** | 130.4s |
| **Total GC Time** | **6.1 minutes** |

---

## 🔬 Deep Dive: Compact 12-Byte KeyIndex (Memory Innovation)

In state tries with 340M+ nodes, storing standard 32-byte SHA-256 keys in memory requires **~11 GB of RAM** just for the index.

`nc-snapshot` introduces a **Compact 12-Byte KeyIndex** with mathematical safety guarantees:
- **Prefix Table ($2^{24}$ buckets):** The first 3 bytes (24 bits) determine the prefix bucket in a 64 MB table.
- **Suffix Storage (12 bytes):** Only bytes 3..15 are stored in RAM.
- **Total Key Entropy:** 15 bytes = **120 bits of entropy**.
- **Collision Probability:** For $N = 342 \times 10^6$ nodes, the probability of two keys sharing 15 bytes is $P \approx \frac{N^2}{2 \times 2^{120}} \approx 4.4 \times 10^{-20}$ (astronomically negligible).
- **Safety Abort Guard:** If two distinct keys ever collide on the first 15 bytes, the index detects it during Scan 1 and safely aborts before any state modification.
- **Result:** Slashes index memory consumption by **62.5% (saving ~7 GB RAM)**, enabling live GC on commodity 16 GB–32 GB systems.

---

## 🌐 Beyond Nine Chronicles: Universal Blockchain State Pruning

> ### 💡 Web3 Infrastructure & DoraHacks Vision
> 
> **The State Bloat Crisis:** Virtually all high-throughput blockchains relying on Merkle Tries, Merkle-Patricia Trees (EVM/Geth/Besu), or IAVL trees (Cosmos SDK) backed by LSM-tree stores (RocksDB / Pebble) face compounding state bloat. As historical modifications accumulate, state databases balloon to terabytes, slowing down validators, making node synchronization painful, and centralizing networks around specialized RPC providers.
> 
> **How nc-snapshot solves this:**
> 1. **Zero-Downtime Secondary Catch-Up:** Leveraging RocksDB's secondary read-only instance mode catches up with primary memtables in milliseconds without holding write locks.
> 2. **Cache-Oblivious Key Bucketing:** 3-byte prefix bucketing ($2^{24}$ buckets) enables binary search in small L3 cache windows, achieving **1.4+ million key lookups/sec** in RAM.
> 3. **CSR (Compressed Sparse Row) Trie Representation:** Flattens 400M+ trie references into linear 32-bit arrays, eliminating pointers and memory fragmentation.
> 4. **Hardware Democratization:** Pruning 152 million blockchain state nodes in **6 minutes on a consumer Ryzen APU** demonstrates that state garbage collection does not require multi-thousand dollar cloud instances.
> 
> This architecture can serve as an open-source blueprint for state pruning engines across any RocksDB-backed decentralized ledger.

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

| Component | Status | Description |
| :--- | :---: | :--- |
| **Direct 3-Scan GC** | ✅ Rust | Direct RocksDB traversal without export files (`--scan-gc`) |
| **CSR Graph BFS** | ✅ Rust | Sub-5s in-memory BFS over 390M+ edges |
| **Live Checkpoints** | ✅ Rust | RocksDB secondary mode with memtable sync |
| **Snapshot Engine** | ✅ Rust | Multi-threaded state & partition generation |
| **Integrity & Verification**| ✅ Rust | BLAKE3 manifests & RocksDB integrity checks |
| **Metadata & Tip Reader** | ✅ Rust | Native Bencodex reader & chain tip discovery |

---

## 🗺️ Roadmap

**Completed:** Live snapshots · 100% Rust snapshot engine · Direct 3-Scan GC Pipeline (`--scan-gc`) · In-Memory CSR Graph BFS · Multi-tier epoch limits & checkpoint filtering · BLAKE3 verification · Multi-threaded compression · RocksDB secondary mode checkpoints · Native Bencodex chain reader · Zero-scratch I/O architecture.

**100% Rust 🦀** — Zero C#/.NET runtime dependencies. Everything runs in a single, high-performance binary.

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
