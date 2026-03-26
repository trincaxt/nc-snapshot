[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux--x64-lightgrey?logo=linux)](https://github.com/trincaxt/nc-snapshot/releases)
[![Nine Chronicles](https://img.shields.io/badge/Nine%20Chronicles-compatible-purple)](https://nine-chronicles.com)

# ⚡ NC Snapshot — Nine Chronicles Blockchain Snapshot Tool

Fast, production-grade snapshot tool for Nine Chronicles blockchain nodes, built in Rust.

Replaces the [NineChronicles.Snapshot](https://github.com/planetarium/NineChronicles.Snapshot) C# tool with **~40x faster** performance.

## Quick Start

```bash
# Base snapshot — auto-named by epoch
./nc-snapshot create --mode partition --apv "<APV>" -s ~/9c-blockchain --output-dir ~/snapshots/base

# State snapshot — daily
./nc-snapshot create --mode state --apv "<APV>" -s ~/9c-blockchain --output-dir ~/snapshots/state

# Verify integrity
./nc-snapshot verify ~/snapshots/base/snapshot-20536-20536.tar.zst
```

## Performance

| Metric | NC Snapshot (Rust) | NineChronicles.Snapshot (C#) |
|--------|-------------------|------------------------------|
| State snapshot (~127 GiB) | **11 min** | ~7 hours |
| Compression | zstd multi-threaded | zip single-threaded |
| Integrity | BLAKE3 checksums | None |
| Throughput | ~175-679 MB/s | ~5-10 MB/s |

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

### Project Structure

```
newsnapshot/
├── bridge-bin/                          ← published C# bridge (portable, no external deps)
│   └── NineChronicles.Snapshot.Bridge
├── src/
│   └── main.rs
├── Cargo.toml
└── target/release/nc-snapshot          ← final binary
```

> 💡 After building, the entire `newsnapshot/` folder is self-contained and can be copied to any Linux x64 server.

## Snapshot Modes

### `state` — State Snapshot
Same as `state_latest.zip` from the NC tool. Contains:
- `block/blockindex`, `tx/txindex` — indexes only
- `states` — full state trie
- `txbindex`, `txexec`, `chain`, `blockcommit`

**Size:** ~127-130 GiB → ~114 GiB compressed

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

### State Snapshot (daily)

```bash
./target/release/nc-snapshot create \
  --mode state \
  --apv "<YOUR_APV_HERE>" \
  -s ~/9c-blockchain \
  --output-dir ~/snapshots/state
```
→ Creates `~/snapshots/state/state_latest.tar.zst` ✅

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
