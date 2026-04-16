# nc-snapshot — Development Log & Technical Design

> **Native Rust snapshot tool for Nine Chronicles blockchain.**  
> Production-tested on `odin5.9capi.com`.

---

## 1. Project Overview

`nc-snapshot` is a high-performance, read-only snapshot orchestrator written in Rust. It creates `.tar.zst` archives of a running (or stopped) Nine Chronicles node without modifying the sacred `9c-blockchain/` directory.

### Why Rust?
- **I/O-bound workloads** (archive creation, compression) are faster and more memory-efficient in Rust.
- **zstd multithreading** achieves ~150 MB/s sustained throughput.
- The original C# tool (`NineChronicles.Snapshot`) is single-threaded on compression and blocks the node during state-copy (`CopyStates`).

---

## 2. Timeline of Development

### Phase 1 — Initial Port (GitHub baseline)
- Ported the directory-list logic from C# `NineChronicles.Snapshot.Program.cs` to Rust.
- Implemented `tar.zst` streaming with `zstd::Encoder::multithread()`.
- Added BLAKE3 manifest generation for integrity verification.
- **Problem:** Staging was built with **symlinks**, and `WalkDir` does **not** follow symlinks by default. Archives were effectively empty or missing all directory contents.

### Phase 2 — Symlink Bug Fix
- **Root cause:** `snapshot.rs::collect_files()` uses `WalkDir::new(dir)` with default `follow_symlinks(false)`. Symlinks to both files and directories were skipped.
- **Fix:** Replaced symlink-based staging with recursive **hard-link population** (`create_hardlink_checkpoint`). Hard-links are real directory entries, so `WalkDir` traverses them normally.
- **Result:** `--prune` mode (node stopped) began producing valid archives with real data.

### Phase 3 — Live Snapshot Race Condition
- **Goal:** Enable `--live` snapshots without stopping the node.
- **Problem:** `create_hardlink_checkpoint` used a single `WalkDir` pass. While the node is running, RocksDB can create new `.sst` files and update `MANIFEST-*` **between** our walk and the archive step. The snapshot ended up with a `MANIFEST` referencing `.sst` files that were never hardlinked, causing:
  ```
  Corruption: IO error: No such file or directory: .../states/012361.sst
  ```
- **Fix:** Converted `create_hardlink_checkpoint` to a **two-pass convergent checkpoint**:
  1. **Pass 1:** Hard-link all data files (`.sst`, `.log`, `IDENTITY`, `LOCK`, etc.) but **skip** metadata files.
  2. **Pass 2:** Hard-link metadata files (`MANIFEST-*`, `CURRENT`, `OPTIONS-*`) **last**.
- **Result:** The checkpoint is internally consistent — the `MANIFEST` only references `.sst` files that already exist in the checkpoint.

### Phase 4 — Production Validation
- Ran `./nc-snapshot create --live --mode state` against the live `odin5.9capi.com` node.
- Extracted the snapshot and booted the C# `NineChronicles.Headless` node against it.
- Node started, preloaded, and synchronized correctly.
- Snapshot metrics:
  - **Original:** 68.77 GiB
  - **Compressed:** 59.67 GiB
  - **Time:** 7m 47s
  - **Throughput:** 150 MB/s

---

## 3. Critical Design Decisions

### 3.1 Hard-links over Symlinks
Symlinks are fast to create but invisible to `WalkDir` in default mode. Hard-links:
- Are instant (no data copy, same filesystem).
- Are treated as real files/directories by any walker.
- Do **not** consume extra disk space for the data itself.
- Are safe because `9c-blockchain/` is never modified.

### 3.2 Two-Pass Checkpoint for Live Mode
RocksDB is **not quiescent** while the node runs. Compaction creates and deletes `.sst` files continuously. A single-pass copy is vulnerable to a race between data files and the `MANIFEST`. The two-pass approach guarantees that the `MANIFEST` in the checkpoint is always consistent with the data files present.

### 3.3 Skip C# Bridge in Live Mode
The C# bridge (`NineChronicles.Snapshot.Bridge`) opens the RocksDB with a write lock. In `--live` mode, the node already holds that lock. Attempting to run the bridge would fail with a store-lock error. We skip the bridge entirely and derive metadata from the Rust side (or omit it, since the node ignores `metadata.json` on restore).

### 3.4 RocksDB Crate Version Lock
The node uses **RocksDB 8.5.3** (via Libplanet 5.5.3 / RocksDbSharp).

The Rust pruner is pinned to:
```toml
rocksdb = "0.22"  # -> librocksdb-sys 0.16.0+8.10.0
```

**Constraint:** `librocksdb-sys` major version must stay in the **8.x** family. Versions `0.23+` ship RocksDB 9.x/10.x, which generate SST files that the C# node cannot open. **Do NOT upgrade this crate without Planetarium also upgrading their RocksDB version.**

---

## 4. Analysis of Official Planetarium Snapshots

### Observation 1 — `txexec` Shrunk Drastically
- Previously: `txexec` in official snapshots was **~58 GB**.
- Currently: `txexec` in official snapshots is **~40–50 MB**.
- **Hypothesis:** The node that generates official snapshots was restarted or compacted, causing natural cleanup of the `txexec` RocksDB column family. This is **not** snapshot-level pruning.

### Observation 2 — Official Snapshot Size Cycle
- Before txexec cleanup: official snapshots oscillated between **110–118 GB**, then dropped back to ~110 GB periodically.
- After txexec cleanup: started at **~58 GB**, then **~59 GB** the next day.
- **Hypothesis:** The C# snapshot tool (`NineChronicles.Snapshot`) now runs with **`bypassCopyStates = true`** most of the time. This skips the `CopyStates` prune step entirely. The size oscillation previously seen (110 → 118 → 110) was likely the `CopyStates` prune running occasionally. Now, with `bypassCopyStates`, the snapshot simply copies the live store.

### Implication for `nc-snapshot`
- **`--live` (no prune)** is the **primary, validated mode** and matches the current official snapshot behavior.
- **`--prune` (state trie prune via `nc-pruner`)** is **optional**. It can produce smaller archives when the node is stopped, but it is no longer a critical requirement for parity with official snapshots.

---

## 5. Current State of the Codebase

### `nc-snapshot` (orchestrator)
- `--live` ✅ Validated in production.
- `--prune` ⚠️ Code is fixed (symlink bug resolved), but **not validated** against the latest node state. Needs a maintenance window to test.
- `STATE_LINK_DIRS` is aligned with the official C# tool.

### `nc-snapshot-rs` (`nc-pruner`)
- Implements **Selective Streaming Copy** (DFS traversal of Bencodex trie + bloom filter visited set).
- Reads `chain/` to auto-detect state roots.
- Opens `states/` in **ReadOnly** mode.
- Writes pruned DB to a separate directory.
- `depth` parameter controls how many recent state roots to preserve (default: 3).

---

## 6. Roadmap

### Immediate (Pre-Release)
1. **Add `--prune-depth` argument to `nc-snapshot`**
   - Currently hardcoded to `3` when invoking `nc-pruner`.
   - Must be configurable so users can align `prune-depth >= block-before`.

2. **Validate `--prune` end-to-end**
   - Stop `odin5` during a low-traffic maintenance window.
   - Run `--prune --mode state` and boot the node against the extracted archive.
   - Confirm synchronization works.

3. **Write release notes and README**
   - Installation from source (`cargo build --release`).
   - Usage examples for `--live` and `--prune`.
   - Compatibility notes (RocksDB version constraint).

### Short-Term
4. **Investigate `txexec` behavior**
   - Determine if `txexec` size fluctuations are purely node-side compaction or if there is a new cleanup mechanism in recent Libplanet versions.
   - Decide if `txexec` should remain in `STATE_LINK_DIRS` permanently.

5. **Optimize archive creation for partition mode**
   - The C# tool recently optimized partition snapshots by archiving directly from store without intermediate copies.
   - Evaluate if `nc-snapshot` can skip staging entirely for partition mode by archiving from hard-link checkpoints directly.

### Long-Term
6. **Consider making `--prune` a separate workflow**
   - Since official snapshots appear to have moved away from `CopyStates`, `nc-snapshot` could position `--live` as the default/recommended mode and `--prune` as an advanced optimization for operators with scheduled downtime.

7. **Offer upstream contribution to Planetarium**
   - Once fully documented and tested, propose `nc-snapshot` as a reference implementation or alternative to the C# tool for operators who prefer a fast, CLI-native snapshotter.

---

## 7. Quick Reference: Safe Commands

### Live snapshot (recommended, node stays up)
```bash
./target/release/nc-snapshot create --live \
  --mode state \
  --apv "<APV_STRING>" \
  --source ~/9c-blockchain \
  --output-dir ~/snapshots/state/ \
  --level 1
```

### Pruned snapshot (node must be stopped)
```bash
# Stop node first
./target/release/nc-snapshot create --prune \
  --mode state \
  --apv "<APV_STRING>" \
  --source ~/9c-blockchain \
  --output-dir ~/snapshots/state/ \
  --level 1
```

### Manual pruner (for testing)
```bash
./nc-snapshot-rs/target/release/nc-pruner prune \
  --store-path ~/9c-blockchain \
  --target-path ~/tmp/pruned-states-test \
  --depth 10
```

---

## 8. Credits & Context

- **Operator:** `odin5.9capi.com` — official Nine Chronicles community node.
- **Tool author:** trincaxt
- **Stack:** Rust, zstd, tar, walkdir, BLAKE3, rocksdb 0.22
- **Compatibility target:** Planetarium `NineChronicles.Headless` (Libplanet 5.5.3, RocksDB 8.5.3)

---

*Last updated: 2025-04-15*
