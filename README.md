# ⚡ nc-snapshot

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![XERJ Corpus Hub contributor](https://hub.xerj.org/contributors/trincaxt/badge.svg)](https://hub.xerj.org/contributors/trincaxt)

A high-performance snapshot engine for the **Nine Chronicles** blockchain. `nc-snapshot` generates consistent live snapshots from running nodes using a Rust-based State Trie Garbage Collection pipeline, multi-threaded archive generation, and official Libplanet validation.

---

## 🚀 Highlights

- **Live snapshots** without stopping the node
- **🦀 100% Native Rust engine** — zero .NET/C# runtime dependencies
- **⚡ Direct 3-Scan GC Pipeline (`--scan-gc`)** — zero intermediate export file, saving ~70–100 GB of scratch I/O
- **🧠 In-Memory CSR Graph Traversal** — 33 levels of BFS traversing 68M+ nodes in **3.5 seconds**
- **🌳 State Trie Garbage Collection** — prune up to 55%+ of unreachable historical state
- **💾 RocksDB secondary mode checkpoints** — instant, consistent, memtable-aware
- **📦 Multi-threaded** Zstandard compression & **🔐 BLAKE3** manifests
- **🔄 Production-proven** on the Nine Chronicles Odin network (152M+ nodes processed in 6.1 min)

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

### 🏆 Record Production Benchmark: 152.4 Million Trie Nodes in 6.1 Minutes

Executed live on a production validator node on the Nine Chronicles Odin network:

**Hardware:** AMD Ryzen 7 5700G (8c/16t) · 32 GB RAM · Kingston NVMe Gen4 · Live node running  
**Run Date:** November 2026  
**Mode:** `--live --prune --scan-gc --mode partition --epoch-limit 20772`

| Pipeline Phase | Throughput | Time |
| :--- | :---: | :---: |
| **Live Secondary Checkpoint** | Memtable catchup + hardlinks | **< 1.0s** |
| **Scan 1: Key Indexing** | **1,412,000 keys/sec** | **107.9s** (~1.8 min) |
| **Scan 2: CSR Graph Construction** | 393,370,211 edges mapped | **126.9s** (~2.1 min) |
| **In-Memory BFS (33 Levels)** | **19,600,000 nodes/sec** | **🔥 3.5s** |
| **Scan 3: Prune & Bulk Load** | 68,626,304 live nodes written | **130.4s** (~2.1 min) |
| **Phase 4: RocksDB Validation** | Checkpoint integrity check | **0.0s** (Instant) |
| **Total State GC Wall Time** | **152.4M → 68.6M nodes** | **⏱️ 6.1 min** |
| **Partition Snapshot (`.tar.zst`)** | 205 files (1.20 GiB → 639.8 MiB) | **9.5s** (130 MB/s) |
| **State Snapshot (`.tar.zst`)** | 1,089 files (72.07 GiB → 59.67 GiB) | **10.5 min** (117 MB/s) |

### 📈 Production GC Results

| Metric | Value | Impact |
| :--- | :---: | :--- |
| **Total Nodes Scanned** | **152,443,110** | Complete state trie history |
| **Live Nodes Preserved** | **68,626,304** | 100% verified reachable tip state |
| **Garbage Nodes Deleted** | **83,816,806** | **55.0% state reduction** |
| **Export File Scratch Space** | **0 Bytes** | No disk wear, zero temporary space |
| **RAM Footprint (Peak)** | **~7.7 GB** | KeyIndex (4.95 GB) + CSR Graph (2.79 GB) |
| **Node Resync After Prune** | **Instant** | 100% compatible with Libplanet C# |

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
