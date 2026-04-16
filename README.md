[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux--x64-lightgrey?logo=linux)](https://github.com/trincaxt/nc-snapshot/releases)
[![Nine Chronicles](https://img.shields.io/badge/Nine%20Chronicles-compatible-purple)](https://nine-chronicles.com)

# ⚡ NC Snapshot — Nine Chronicles Blockchain Snapshot Tool

Fast, production-grade snapshot tool for Nine Chronicles blockchain nodes, built in Rust.

Replaces the [NineChronicles.Snapshot](https://github.com/planetarium/NineChronicles.Snapshot) C# tool with **~7x faster** performance.

## Recent Changes (2026-04-11)

### Live Snapshot Support (`--live`)
- **Take snapshots without stopping the node** using RocksDB hard-link checkpoints
- Hard-links create instant point-in-time copies (no data copy, no downtime)
- `--live` works with all modes: state, partition, full
- `--live --prune` uses nc-pruner's built-in checkpoint mechanism
- Checkpoints are automatically cleaned up after archiving

### Prune Support (`--prune`)
- **50% smaller state snapshots** by pruning unreachable trie nodes
- Uses nc-pruner (Selective Streaming Copy) — no tombstones, no compaction
- Reduces states/ from ~36 GiB to ~19 GiB (~15-25 min)
- 9c-blockchain/ is NEVER modified (opens ReadOnly, writes to temp dir)
- Requires nc-pruner binary (included in `nc-snapshot-rs/`)

### Bridge Update
- **Complete Metadata Format**: Bridge now generates full BlockHeader JSON (same as C# original)
- **Self-Contained**: Bridge is now included within the project at `bridge-bin/`

## Quick Start

```bash
# State snapshot (daily)
./nc-snapshot create --mode state --apv "<APV>" -s ~/9c-blockchain --output-dir ~/snapshots/state

# Live state snapshot (node stays running!)
./nc-snapshot create --live --mode state --apv "<APV>" -s ~/9c-blockchain --output-dir ~/snapshots/state

# Live state snapshot with prune (smaller, node stays running)
./nc-snapshot create --live --prune --mode state --apv "<APV>" -s ~/9c-blockchain --output-dir ~/snapshots/state

# Base/partition snapshot (weekly/monthly)
./nc-snapshot create --mode partition --apv "<APV>" -s ~/9c-blockchain --output-dir ~/snapshots/base

# Live partition snapshot
./nc-snapshot create --live --mode partition --apv "<APV>" -s ~/9c-blockchain --output-dir ~/snapshots/base

# Verify integrity
./nc-snapshot verify ~/snapshots/state/state_latest.tar.zst
```

## Performance

| Metric | NC Snapshot (Rust) | NineChronicles.Snapshot (C#) |
|--------|-------------------|------------------------------|
| State snapshot (~127 GiB) | **11 min** | ~7 hours |
| State + prune (~114 GiB) | **~25 min** | ~3 hours |
| Compression | zstd multi-threaded | zip single-threaded |
| Integrity | BLAKE3 checksums | None |
| Throughput | ~175-679 MB/s | ~5-10 MB/s |

## Features

- **🔥 Fast** — zstd multi-threaded compression (`-T0` equivalent)
- **🔒 Integrity** — BLAKE3 checksums for every file (manifest embedded in archive)
- **🛡️ Safe** — Detects if node is running via RocksDB lock files
- **📸 Live** — Take snapshots without stopping the node (`--live`)
- **✂️ Prune** — 50% smaller state snapshots (`--prune`)
- **⚡ Atomic** — Writes to temp file, renames on success (no partial outputs)
- **📊 Progress** — Real-time progress bar with ETA and throughput
- **📁 Incremental** — Skip unchanged files using mtime+size fingerprint
- **🏷️ Auto-naming** — `--output-dir` names the file automatically by epoch
- **📦 Portable** — Self-contained folder, copy anywhere and it works
- **🤖 Scriptable** — JSON output mode for automation

## How Live Snapshots Work

The `--live` flag uses RocksDB hard-link checkpoints for consistency:

```
Without --live (offline):
  1. Check node is stopped (abort if running)
  2. Archive directly from source

With --live (online):
  1. Skip node detection (node can be running)
  2. Create hard-link checkpoint of states/ (instant, no data copy)
  3. Archive from checkpoint (consistent point-in-time view)
  4. Clean up checkpoint

With --live --prune (online + smaller):
  1. Skip node detection
  2. nc-pruner creates its own checkpoint + pruned copy
  3. Archive from pruned copy
  4. Clean up staging dirs
```

### Safety Guarantees
- **9c-blockchain/ is NEVER modified** — all checkpoints are created outside it
- **Atomic Consistency:** Uses a unique two-pass checkpoint strategy. Data files (`.sst`) are linked first, and metadata (`MANIFEST`) is linked last. This prevents corruption caused by RocksDB's background compaction during the snapshot process.
- Hard-links are instant and don't affect original files
- If checkpoint fails, falls back to archiving live source (with warning)
- `--block-before` (default=1) handles tip offset for live snapshots

## How Pruning Works

The `--prune` flag uses nc-pruner's Selective Streaming Copy:

```
1. nc-pruner opens states/ in ReadOnly mode (never modifies source)
2. Creates hard-link checkpoint outside 9c-blockchain/
3. Reads last 3 state roots from chain/
4. DFS traversal of reachable trie nodes from those roots
5. Writes only reachable nodes to clean target DB
6. Creates staging with pruned states + symlinks to original static dirs
7. Archives from staging, then cleans up
```

**Result:** states/ goes from ~36 GiB to ~19 GiB (47% reduction)

## Installation

### 1. Publish the C# Bridge

Run from the original [NineChronicles.Snapshot](https://github.com/planetarium/NineChronicles.Snapshot) repository:

```bash
cd /path/to/NineChronicles.Snapshot

dotnet publish NineChronicles.Snapshot.Bridge \
  --configuration Release \
  --runtime linux-x64 \
  --self-contained false \
  -o ./newsnapshot/bridge-bin/
```

### 2. Build the Rust binary

```bash
cd newsnapshot
cargo build --release
# Binary: ./target/release/nc-snapshot
```

### 3. Build nc-pruner (for `--prune` support)

```bash
cd nc-snapshot-rs
cargo build --release
# Binary: ./target/release/nc-pruner
```

### Project Structure

```
newsnapshot/
├── bridge-bin/                          ← published C# bridge (portable, no external deps)
│   └── NineChronicles.Snapshot.Bridge
├── nc-snapshot-rs/                      ← nc-pruner for --prune support
│   └── target/release/nc-pruner
├── src/
│   ├── main.rs                          ← CLI entry point
│   ├── snapshot.rs                      ← archive creation
│   ├── node_detect.rs                   ← lock file detection
│   ├── types.rs                         ← config and result types
│   ├── verify.rs                        ← archive verification
│   └── errors.rs                        ← error types
├── Cargo.toml
└── target/release/nc-snapshot           ← final binary
```

> 💡 After building, the entire `newsnapshot/` folder is self-contained and can be copied to any Linux x64 server.

## Snapshot Modes

### `state` — State Snapshot
Same as `state_latest.zip` from the NC tool. Contains:
- `block/blockindex`, `tx/txindex` — indexes only
- `states` — full state trie
- `txbindex`, `txexec`, `chain`, `blockcommit`

**Size:** ~127-130 GiB → ~114 GiB compressed | ~64 GiB with `--prune`

### `partition` — Base/Partition Snapshot
Same as `snapshot-XXXXX-XXXXX.zip` from the NC tool. Contains:
- `block/*` epochs (excluding blockindex)
- `tx/*` epochs (excluding txindex)

**Size:** ~274 GiB (all epochs) → varies with `--epoch-limit`

### `full` — Full Snapshot
Everything in the store directory. Combines state + partition + small dirs.

**Size:** ~402 GiB

## Usage

> 💡 Use `--output-dir` to auto-name the file by epoch (e.g. `snapshot-20536-20536.tar.zst`).
> Use `-o` only when you need a specific fixed filename.

### State Snapshot (daily)

```bash
./target/release/nc-snapshot create \
  --mode state \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/state
```
→ Creates `~/snapshots/state/state_latest.tar.zst` ✅

### Live State Snapshot (node stays running)

```bash
./target/release/nc-snapshot create \
  --live \
  --mode state \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/state
```
→ Creates consistent snapshot without stopping node ✅

### Live State Snapshot + Prune (smallest, node stays running)

```bash
./target/release/nc-snapshot create \
  --live \
  --prune \
  --mode state \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/state
```
→ Creates pruned snapshot (~64 GiB) without stopping node ✅

### Base Snapshot (weekly/monthly)

```bash
./target/release/nc-snapshot create \
  --mode partition \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/base
```
→ Creates `~/snapshots/base/snapshot-XXXXX-XXXXX.tar.zst` auto-named by epoch ✅
→ Saves metadata to `~/snapshots/base/metadata/snapshot-XXXXX-XXXXX.json` ✅

### Live Base Snapshot

```bash
./target/release/nc-snapshot create \
  --live \
  --mode partition \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/base
```
→ Creates partition snapshot with hard-link checkpoint of block/ and tx/ ✅

### Incremental Snapshot (following week)

```bash
# Get the final epoch from the previous base snapshot metadata.json
./target/release/nc-snapshot create \
  --mode partition \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/base \
  --epoch-limit 20536
```
→ Creates `~/snapshots/base/snapshot-20536-XXXXX.tar.zst` with only new epochs ✅

> 📂 The correct epoch limit is found in `~/snapshots/base/metadata/snapshot-XXXXX-XXXXX.json` after each base snapshot run.

### Verify Integrity

```bash
./target/release/nc-snapshot verify ~/snapshots/base/snapshot-20536-20536.tar.zst
```

### Restore Snapshot

```bash
# Partition first, then state on top
tar -I zstd -xf ~/snapshots/base/snapshot-20536-20536.tar.zst -C ~/9c-blockchain/
tar -I zstd -xf ~/snapshots/state/state_latest.tar.zst -C ~/9c-blockchain/
```

### Dry-run (preview without creating archive)

```bash
./target/release/nc-snapshot create \
  --mode partition \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/base \
  --dry-run
```

## CLI Reference

```
nc-snapshot create [OPTIONS]

Options:
  -s, --source <PATH>         Blockchain directory [default: ~/9c-blockchain]
  -o, --output <PATH>         Output archive path (.tar.zst) — fixed name, overrides auto-naming
      --output-dir <PATH>     Output directory — file auto-named by epoch
  -m, --mode <MODE>           state | partition | full [default: state]
  -l, --level <1-19>          Zstd compression level [default: 1]
  -t, --threads <N>           Compression threads, 0=all CPUs [default: 0]
  -e, --exclude <DIR>         Directories to exclude (repeatable)
  -i, --include <DIR>         Override default dirs (repeatable)
      --epoch-limit <N>       Partition mode: skip epochs below N
      --apv <APV>             APV string for metadata generation
      --block-before <N>      Block before current tip [default: 1]
      --live                  Take snapshot without stopping node (uses checkpoints)
      --prune                 Prune states before archiving (~50% smaller)
      --pruner-path <PATH>    Path to nc-pruner binary
      --force                 Proceed even if node is running
      --json                  JSON structured output
      --dry-run               Scan only, don't create archive
      --incremental           Skip unchanged files (mtime+size)

nc-snapshot verify [OPTIONS] <ARCHIVE>
      --json                  JSON structured output
```

### Output Naming Rules

| Flag | Result |
|---|---|
| `--output-dir ~/snapshots/base` | `~/snapshots/base/snapshot-20536-20536.tar.zst` (auto) |
| `-o ~/snapshots/base/my-name.tar.zst` | `~/snapshots/base/my-name.tar.zst` (fixed) |
| neither | `snapshot-20536-20536.tar.zst` in current directory |

### Live + Prune Matrix

| Flags | Behavior |
|---|---|
| (none) | Offline snapshot, aborts if node running |
| `--force` | Offline snapshot, ignores node detection |
| `--live` | Online snapshot with hard-link checkpoint |
| `--prune` | Offline snapshot with pruned states |
| `--live --prune` | Online snapshot with nc-pruner checkpoint + prune |

## Compression Levels

| Level | Speed | State Size (~128 GiB) | Time Est. |
|-------|-------|-----------------------|-----------|
| 1 | ~175 MB/s | ~114 GiB | ~14 min |
| 2 | ~160 MB/s | ~110 GiB | ~16 min |
| 3 | ~130 MB/s | ~108 GiB | ~19 min |
| 9 | ~40 MB/s | ~105 GiB | ~55 min |

Level 1 is recommended — RocksDB SST files are already internally compressed.

## Real-World Test Results (2026-03-25)

### Base Snapshot (273 GiB)
```
📂 Scanning files... 47993 files | 273.53 GiB
████████████████████████████████████████ 273.53 GiB (174.94 MiB/s)
  Metadata   : ~/snapshots/base/metadata/snapshot-20536-20536.json
✅ Snapshot criado: snapshot-20536-20536.tar.zst
   Original   : 273.53 GiB
   Comprimido : 198.51 GiB
   Redução    : 27.4%
   Tempo      : 1601.1s (26 min 41s)
   Throughput : 175 MB/s
```

### State Snapshot (128 GiB)
```
📂 Scanning files... 2231 files | 128.35 GiB
████████████████████████████████████████ 128.35 GiB (154.34 MiB/s)
✅ Snapshot criado: state_latest.tar.zst
   Original   : 128.35 GiB
   Comprimido : 114.22 GiB
   Redução    : 11.0%
   Tempo      : 851.6s (14 min 11s)
   Throughput : 154 MB/s
```

### Live State Snapshot (2026-04-15 — validated on odin5)
```
./target/release/nc-snapshot create --live \
  --mode state \
  --apv "200420/AB2da648b9154F2cCcAFBD85e0Bc3d51f97330Fc/MEUCIQDWWr4Fk3XUo3RHwe5IyFIq9OSplBw5M9u69AhBFi78UwIgK96aJ+09EpYBTxVDkzlTMDefRBjcf5104TGmj3ad+mg=/ZHU5OnRpbWVzdGFtcHUxMDoyMDI2LTAzLTI2ZQ==" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/state/ \
  --level 1

🟢 Live mode: skipping bridge (reading metadata via Rust)
╔══════════════════════════════════════════╗
║   ⚡ NC Blockchain Snapshot Tool         ║
╚══════════════════════════════════════════╝
  Source  : /home/vrunnx/9c-blockchain
  Output  : /home/vrunnx/snapshots/state/state_snapshot.tar.zst
  Mode    : state
  Level   : zstd-1
  Threads : 8
  Live    : ON (node may be running, using checkpoints)

🟢 Live mode: skipping node detection (using checkpoints for consistency)
   ⚠  Snapshot may be slightly behind the chain tip
📸 Creating live checkpoint (hard-links)...
   ✓ Checkpointed states/
✅ Live checkpoint created
   Checkpoint: /home/vrunnx/snapshots/state/.nc-snapshot-live-checkpoint
   Archiving from staging (9c-blockchain untouched)
📂 Scanning files... 1221 files | 68.77 GiB
  [00:07:47] [████████████████████████████████████████] 68.77 GiB/68.77 GiB (150.49 MiB/s) ETA 0s
✅ Snapshot criado: /home/vrunnx/snapshots/state/state_snapshot.tar.zst
   Modo       : state
   Original   : 68.77 GiB
   Comprimido : 59.67 GiB
   Redução    : 13.2%
   Tempo      : 467.9s
   Throughput : 150 MB/s
   Manifest   : /home/vrunnx/snapshots/state/state_snapshot.tar.blake3
🧹 Cleaning up live checkpoint...
```

### Verification
```
🔍 Verifying: snapshot-20536-20536.tar.zst
📋 Manifest: 47993 entries, Archive: 47993 files
✅ All 47993 files verified OK

🔍 Verifying: state_latest.tar.zst
📋 Manifest: 2231 entries, Archive: 2231 files
✅ All 2231 files verified OK
```

### Full Restore Test
```
tar -I zstd -xf snapshot-20536-20536.tar.zst -C ~/9c-blockchain/  # 43 min
tar -I zstd -xf state_latest.tar.zst -C ~/9c-blockchain/          # 23 min
✅ Node started and synced successfully after restore
```

## Contributing

Contributions are welcome! Feel free to open issues or pull requests.

This project is specifically designed for [Nine Chronicles](https://nine-chronicles.com) node operators and uses the [NineChronicles.Snapshot.Bridge](https://github.com/planetarium/NineChronicles.Snapshot) from [Planetarium](https://github.com/planetarium) to fetch blockchain metadata.

## Acknowledgements

- **[Planetarium](https://github.com/planetarium)** — for the original [NineChronicles.Snapshot](https://github.com/planetarium/NineChronicles.Snapshot) C# tool and the Bridge interface this project builds upon.
- **[Perplexity AI](https://www.perplexity.ai)** — AI assistant that paired on the entire architecture: `--output-dir` auto-naming, portable `bridge-bin/` design, `fetch_metadata` refactor from `dotnet run` to published binary, and all the debugging sessions across 10+ open terminals. 🦀

## License

MIT — see [LICENSE](LICENSE) for details.
