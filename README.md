# ⚡ NC Snapshot — Nine Chronicles Blockchain Snapshot Tool

Fast, production-grade snapshot tool for Nine Chronicles blockchain nodes, built in Rust.

Replaces the [NineChronicles.Snapshot](https://github.com/planetarium/NineChronicles.Snapshot) C# tool with **~40x faster** performance.

## Performance

| Metric | NC Snapshot (Rust) | NineChronicles.Snapshot (C#) |
|--------|-------------------|------------------------------|
| State snapshot (~127 GiB) | **11 min** | ~7 hours |
| Compression | zstd multi-threaded | zip single-threaded |
| Integrity | BLAKE3 checksums | None |
| Throughput | ~190-500 MB/s | ~5-10 MB/s |

## Installation

### 1. Publish the C# Bridge (run from the original NineChronicles.Snapshot repo)

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

### Project Structure (portable)

```
newsnapshot/
├── bridge-bin/                          ← published C# bridge (no external deps)
│   └── NineChronicles.Snapshot.Bridge
├── src/
│   └── main.rs
├── Cargo.toml
└── target/release/nc-snapshot          ← final binary
```

> 💡 After building, the entire `newsnapshot/` folder is self-contained and can be copied to any Linux x64 server.

## Snapshot Modes

### `state` (default) — State Snapshot
Same as `state_latest.zip` from the NC tool. Contains:
- `block/blockindex` — block index only (not all epochs)
- `tx/txindex` — transaction index only (not all epochs)
- `states` — full state trie
- `txbindex` — transaction block index
- `txexec` — transaction execution results
- `chain` — chain metadata
- `blockcommit` — block commit data

**Size:** ~127-130 GiB → ~113 GiB compressed

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

### Grandão — Partition Snapshot (once a week/month)

```bash
./target/release/nc-snapshot create \
  --mode partition \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/base
```
→ Creates `~/snapshots/base/snapshot-XXXXX-XXXXX.tar.zst` **auto-named by epoch** ✅  
→ Saves metadata to `~/snapshots/base/metadata/snapshot-XXXXX-XXXXX.json` automatically ✅

### State Snapshot (daily)

```bash
./target/release/nc-snapshot create \
  --mode state \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/state
```
→ Creates `~/snapshots/state/state_latest.tar.zst` ✅

### Incremental / Complemento (following week)

```bash
# Get the final epoch from the previous grandão metadata.json and pass it to --epoch-limit
./target/release/nc-snapshot create \
  --mode partition \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/base \
  --epoch-limit 20536
```
→ Creates `~/snapshots/base/snapshot-20536-XXXXX.tar.zst` with **only new epochs** ✅

> 📂 The correct epoch limit is found in `~/snapshots/base/metadata/snapshot-XXXXX-XXXXX.json` after each grandão run.

### Full Snapshot

```bash
./target/release/nc-snapshot create \
  --mode full \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/full
```

### Verify Integrity

```bash
./target/release/nc-snapshot verify ~/snapshots/base/snapshot-20536-20536.tar.zst
```

### Restore Snapshot

```bash
# State snapshot → restore into blockchain dir
tar -I zstd -xf ~/snapshots/state/state_latest.tar.zst -C ~/9c-blockchain/

# Partition snapshot → restore epochs
tar -I zstd -xf ~/snapshots/base/snapshot-20536-20536.tar.zst -C ~/9c-blockchain/

# Full restore to new directory
mkdir -p ~/9c-restored
tar -I zstd -xf ~/snapshots/full/full-snapshot.tar.zst -C ~/9c-restored/
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

## Compression Levels

Level 1 is recommended for RocksDB data (already internally compressed):

| Level | Speed | State Size (~128 GiB) | Time Est. |
|-------|-------|-----------------------|-----------|
| 1 | ~190 MB/s | ~113 GiB | ~11 min |
| 2 | ~160 MB/s | ~110 GiB | ~13 min |
| 3 | ~130 MB/s | ~108 GiB | ~16 min |
| 9 | ~40 MB/s | ~105 GiB | ~50 min |

Higher levels give diminishing returns on pre-compressed SST files.

## Features

- **🔥 Fast** — zstd multi-threaded compression (`-T0` equivalent)
- **🔒 Integrity** — BLAKE3 checksums for every file (manifest embedded in archive)
- **🛡️ Safe** — Detects if node is running via RocksDB lock files
- **⚡ Atomic** — Writes to temp file, renames on success (no partial outputs)
- **📊 Progress** — Real-time progress bar with ETA and throughput
- **📁 Incremental** — Skip unchanged files using mtime+size fingerprint
- **🏷️ Auto-naming** — `--output-dir` names the file automatically by epoch
- **📦 Portable** — Self-contained folder, copy anywhere and it works
- **🤖 Scriptable** — JSON output mode for automation
- **📦 Single binary** — No runtime dependencies

## Test Results

### State Snapshot (2026-03-24)
```
📂 Scanning files... 2155 files | 126.74 GiB
████████████████████████████████████████ 126.74 GiB (190.38 MiB/s)
✅ Snapshot criado: state_latest.tar.zst
   Original   : 126.74 GiB
   Comprimido : 112.85 GiB
   Redução    : 11.0%
   Tempo      : 681.7s (11 min 22s)
   Throughput : 190 MB/s
```

### Partition Snapshot / Incremental (2026-03-25)
```
📂 Scanning files... 243 files | 0.36 GiB
████████████████████████████████████████ 364.85 MiB (679.58 MiB/s)
  Metadata   : ~/snapshots/base/metadata/snapshot-20536-20536.json
✅ Snapshot criado: snapshot-20536-20536.tar.zst
   Modo       : partition
   Original   : 364.9 MiB
   Comprimido : 191.9 MiB
   Redução    : 47.4%
   Tempo      : 0.5s
   Throughput : 679 MB/s
```

### Verification
```
🔍 Verifying: state_latest.tar.zst
📋 Manifest: 2155 entries, Archive: 2155 files
✅ All 2155 files verified OK
```

### Restore Test
```
tar -I zstd -xf state_latest.tar.zst -C ~/9c-blockchain/
✅ Node started successfully after restore
```

## Acknowledgements

- **[Planetarium](https://github.com/planetarium)** — for the original [NineChronicles.Snapshot](https://github.com/planetarium/NineChronicles.Snapshot) C# tool and the Bridge interface this project builds upon.

