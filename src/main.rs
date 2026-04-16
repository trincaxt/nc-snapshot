mod errors;
mod node_detect;
mod snapshot;
mod types;
mod verify;

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use types::{BridgeResult, SnapshotConfig, SnapshotMode};

#[derive(Parser, Debug)]
#[command(
    name = "nc-snapshot",
    about = "⚡ Nine Chronicles blockchain snapshot tool",
    version,
    long_about = "Fast, production-grade snapshot tool for Nine Chronicles blockchain.\n\
Creates tar.zst archives with BLAKE3 integrity verification.\n\n\
Modes:\n\
- state (default)  State snapshot: indexes + state data (~127 GiB)\n\
- partition        Base/partition snapshot: block + tx epochs (~230+ GiB)\n\
- full             Full snapshot: everything in the store directory"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create a new snapshot archive
    Create {
        /// Source blockchain directory (NEVER modified)
        #[arg(short, long, default_value = "~/9c-blockchain")]
        source: String,

        /// Output archive path (.tar.zst) — overrides --output-dir and auto-naming
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Output directory for auto-named snapshot (e.g. ~/snapshots/base)
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Snapshot mode: state (default), partition (base), full
        #[arg(short, long, default_value = "state")]
        mode: String,

        /// Zstd compression level 1-19 (1=fastest, default=1)
        #[arg(short, long, default_value = "1")]
        level: i32,

        /// Number of compression threads (0=all CPUs)
        #[arg(short, long, default_value = "0")]
        threads: usize,

        /// Directories to EXCLUDE from snapshot
        #[arg(short, long)]
        exclude: Vec<String>,

        /// Directories to INCLUDE (overrides mode defaults)
        #[arg(short, long)]
        include: Vec<String>,

        /// Epoch limit for partition mode (skip epochs below this number)
        #[arg(long)]
        epoch_limit: Option<u64>,

        /// APV for metadata generation
        #[arg(long)]
        apv: Option<String>,

        /// Block before current tip
        #[arg(long, default_value_t = 1)]
        block_before: i32,

        /// Proceed even if node is detected running
        #[arg(long)]
        force: bool,

        /// Output results as JSON
        #[arg(long)]
        json: bool,

        /// Scan only, don't create archive
        #[arg(long)]
        dry_run: bool,

        /// Skip unchanged files since last snapshot
        #[arg(long)]
        incremental: bool,

        /// Prune states before archiving (reduces state snapshot ~50%).
        /// Requires nc-pruner binary. The 9c-blockchain/ directory is NEVER modified.
        #[arg(long)]
        prune: bool,

        /// Path to nc-pruner binary (default: looks in PATH and ../nc-snapshot-rs/target/release/)
        #[arg(long)]
        pruner_path: Option<PathBuf>,

        /// Number of recent state roots to preserve when pruning (default: 3).
        /// Must be >= --block-before to avoid pruning away the snapshot tip state.
        #[arg(long, default_value = "3")]
        prune_depth: usize,

        /// Take a live snapshot without stopping the node.
        /// Uses RocksDB hard-link checkpoints for consistency.
        /// The snapshot may be slightly behind the chain tip (use --block-before to control).
        #[arg(long)]
        live: bool,
    },

    /// Verify an existing archive's integrity
    Verify {
        /// Path to the .tar.zst archive to verify
        archive: PathBuf,

        /// Output results as JSON
        #[arg(long)]
        json: bool,
    },
}

fn expand_tilde(path: &str) -> PathBuf {
    if path.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(&path[2..]);
        }
    }
    PathBuf::from(path)
}

fn fetch_metadata(
    source: &Path,
    apv: &str,
    block_before: i32,
    mode: &str,
) -> anyhow::Result<BridgeResult> {
    let prepare_args = serde_json::json!({
        "Apv": apv,
        "OutputDirectory": ".",
        "StorePath": source.to_string_lossy(),
        "BlockBefore": block_before,
        "BypassCopyStates": true,
        "SnapshotType": mode
    });

    let bridge_bin = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("bridge-bin")
        .join("NineChronicles.Snapshot.Bridge");

    let output = Command::new(&bridge_bin)
        .arg(prepare_args.to_string())
        .output()?;

    if !output.status.success() {
        anyhow::bail!(
            "C# Bridge failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let last_line = stdout
        .lines()
        .last()
        .ok_or_else(|| anyhow::anyhow!("Empty bridge output"))?;
    let res: BridgeResult = serde_json::from_str(last_line)?;
    Ok(res)
}

/// Find the nc-pruner binary in common locations.
fn find_pruner_binary(explicit_path: &Option<PathBuf>) -> Option<PathBuf> {
    if let Some(p) = explicit_path {
        if p.exists() {
            return Some(p.clone());
        }
    }

    if let Ok(output) = Command::new("which").arg("nc-pruner").output() {
        if output.status.success() {
            let path_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path_str.is_empty() {
                return Some(PathBuf::from(path_str));
            }
        }
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        manifest_dir.join("../nc-snapshot-rs/target/release/nc-pruner"),
        manifest_dir.join("../nc-snapshot-rs/target/debug/nc-pruner"),
        manifest_dir.join("nc-snapshot-rs/target/release/nc-pruner"),
    ];
    for candidate in &candidates {
        if candidate.exists() {
            return Some(candidate.clone());
        }
    }

    None
}

/// Check whether a file name is a RocksDB metadata file that must be copied last.
fn is_rocksdb_metadata(name: &str) -> bool {
    name.starts_with("MANIFEST-") || name == "CURRENT" || name.starts_with("OPTIONS-")
}

/// Create a hard-link checkpoint of a directory for consistent live snapshots.
/// Hard-links are instant (no data copy) and create a point-in-time view.
/// The source directory is NEVER modified.
///
/// For live checkpoints we do two passes:
///   1. Hard-link data files (.sst, .log, etc.)
///   2. Hard-link metadata files (MANIFEST-*, CURRENT, OPTIONS-*) last
/// This avoids the race where MANIFEST references a newly-created .sst that
/// our first WalkDir pass missed.
fn create_hardlink_checkpoint(src_dir: &Path, dst_dir: &Path) -> anyhow::Result<()> {
    use anyhow::Context;

    std::fs::create_dir_all(dst_dir)
        .with_context(|| format!("Creating checkpoint dir: {}", dst_dir.display()))?;

    // ═══════════════════════════════════════════════════════════════════════
    // PASS 1: directories + data files
    // ═══════════════════════════════════════════════════════════════════════
    for entry in walkdir::WalkDir::new(src_dir).follow_links(false) {
        let entry = entry.map_err(|e| anyhow::anyhow!("WalkDir error: {}", e))?;
        let rel = entry.path().strip_prefix(src_dir).unwrap_or(entry.path());
        let dst_path = dst_dir.join(rel);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path).with_context(|| {
                format!("Creating checkpoint subdirectory: {}", dst_path.display())
            })?;
        } else if entry.file_type().is_file() {
            let file_name = entry.file_name().to_string_lossy();
            if is_rocksdb_metadata(&file_name) {
                continue; // Copy metadata in pass 2
            }
            if let Some(parent) = dst_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::hard_link(entry.path(), &dst_path).with_context(|| {
                format!(
                    "Hard-linking {} -> {}",
                    entry.path().display(),
                    dst_path.display()
                )
            })?;
        }
        // Skip symlinks
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PASS 2: metadata files (MANIFEST, CURRENT, OPTIONS)
    // ═══════════════════════════════════════════════════════════════════════
    for entry in walkdir::WalkDir::new(src_dir).follow_links(false) {
        let entry = entry.map_err(|e| anyhow::anyhow!("WalkDir error: {}", e))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy();
        if !is_rocksdb_metadata(&file_name) {
            continue;
        }
        let rel = entry.path().strip_prefix(src_dir).unwrap_or(entry.path());
        let dst_path = dst_dir.join(rel);
        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::hard_link(entry.path(), &dst_path).with_context(|| {
            format!(
                "Hard-linking metadata {} -> {}",
                entry.path().display(),
                dst_path.display()
            )
        })?;
    }

    Ok(())
}

/// State dirs needed for state snapshot mode.
/// These are the directories (besides states/) that a state snapshot includes.
const STATE_LINK_DIRS: &[&str] = &[
    "block",      // contains block/blockindex
    "tx",         // contains tx/txindex
    "txbindex",
    "chain",
    "blockcommit",
    "txexec",
];

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Create {
            source,
            output,
            output_dir,
            mode,
            level,
            threads,
            exclude,
            include,
            mut epoch_limit,
            apv,
            block_before,
            force,
            json,
            dry_run,
            incremental,
            prune,
            pruner_path,
            prune_depth,
            live,
        } => {
            let source_path = expand_tilde(&source);
            let threads = if threads == 0 {
                num_cpus::get()
            } else {
                threads
            };
            let mode_enum: SnapshotMode = mode.parse().unwrap_or(SnapshotMode::State);

            let mut bridge_res = None;
            if let Some(ref apv_val) = apv {
                if live {
                    // In live mode, the C# bridge cannot open the RocksDB (lock held by node).
                    // We skip the bridge entirely and read metadata from Rust side instead.
                    if !json {
                        eprintln!("🟢 Live mode: skipping bridge (reading metadata via Rust)");
                    }
                } else {
                    if !json {
                        eprintln!("🚀 Fetching blockchain metadata...");
                    }
                    match fetch_metadata(&source_path, apv_val, block_before, &mode) {
                        Ok(res) => {
                            if !res.success {
                                eprintln!("❌ Bridge error: {:?}", res.error);
                                process::exit(1);
                            }
                            if mode_enum == SnapshotMode::Partition && epoch_limit.is_none() {
                                epoch_limit = Some(res.current_metadata_block_epoch as u64);
                            }
                            bridge_res = Some(res);
                        }
                        Err(e) => {
                            eprintln!("⚠️ Failed to fetch metadata: {}", e);
                        }
                    }
                }
            }

            let final_output = if let Some(p) = output {
                p
            } else {
                let auto_name = if let Some(ref res) = bridge_res {
                    if mode_enum == SnapshotMode::Partition {
                        format!("{}.tar.zst", res.partition_base_filename)
                    } else {
                        "state_latest.tar.zst".to_string()
                    }
                } else {
                    format!("{}_snapshot.tar.zst", mode)
                };

                match output_dir {
                    Some(ref dir) => {
                        if !dir.exists() {
                            if let Err(e) = std::fs::create_dir_all(dir) {
                                eprintln!("❌ Failed to create output-dir: {}", e);
                                process::exit(1);
                            }
                        }
                        dir.join(auto_name)
                    }
                    None => PathBuf::from(auto_name),
                }
            };

            if !json {
                eprintln!("╔══════════════════════════════════════════╗");
                eprintln!("║   ⚡ NC Blockchain Snapshot Tool         ║");
                eprintln!("╚══════════════════════════════════════════╝");
                eprintln!("  Source  : {}", source_path.display());
                eprintln!("  Output  : {}", final_output.display());
                eprintln!("  Mode    : {}", mode);
                eprintln!("  Level   : zstd-{}", level);
                eprintln!("  Threads : {}", threads);
                if prune {
                    eprintln!("  Prune   : ON (states will be pruned)");
                }
                if live {
                    eprintln!("  Live    : ON (node may be running, using checkpoints)");
                }
                if let Some(el) = epoch_limit {
                    eprintln!("  Epoch≥  : {}", el);
                }
                eprintln!();
            }

            if live {
                if !json {
                    eprintln!("🟢 Live mode: skipping node detection (using checkpoints for consistency)");
                    eprintln!("   ⚠️  Snapshot may be slightly behind the chain tip");
                }
            } else if !force {
                let locked = node_detect::check_node_running(&source_path);
                if !locked.is_empty() {
                    eprintln!("⚠️  Node appears to be running!");
                    process::exit(1);
                }
            }

            // ══════════════════════════════════════════════════════════
            // PRUNE STEP (state mode only)
            //
            // CRITICAL: 9c-blockchain/ is NEVER modified.
            // nc-pruner opens states/ ReadOnly, writes pruned copy to temp dir.
            // We create a STAGING directory with symlinks to original + pruned.
            // Archive is created from staging. Staging is deleted after.
            // ══════════════════════════════════════════════════════════
            let mut staging_dir: Option<PathBuf> = None;
            let mut prune_work_dir: Option<PathBuf> = None;

            if prune && mode_enum == SnapshotMode::State {
                if live {
                    // In live mode, prune is NOT safe — the node's compaction can
                    // delete .sst files during checkpoint creation, causing missing
                    // file errors. The hard-link checkpoint is not atomic.
                    // Skip prune in live mode; archive live states as-is.
                    if !json {
                        eprintln!("⚠️  --prune skipped in --live mode (not safe with running node)");
                        eprintln!("   Archive will include full unpruned states.");
                        eprintln!("   For pruned snapshots, stop the node first.");
                    }
                } else {
                    if !json {
                        eprintln!("🔧 Pruning states before archiving...");
                    }

                    let pruner_bin = find_pruner_binary(&pruner_path);
                    match pruner_bin {
                        Some(ref bin) => {
                            if !json {
                                eprintln!("   Pruner: {}", bin.display());
                            }
                            let output_parent = final_output.parent().unwrap_or(Path::new("."));
                            let work_dir = output_parent.join(".nc-snapshot-prune-work");
                            let _ = std::fs::create_dir_all(&work_dir);
                            let pruned_states = work_dir.join("states_pruned");

                            let mut cmd = Command::new(bin);
                            cmd.arg("prune")
                                .arg("--store-path").arg(&source_path)
                                .arg("--target-path").arg(&pruned_states)
                                .arg("--depth").arg(prune_depth.to_string());

                            match cmd.output() {
                                Ok(out) => {
                                    let stderr = String::from_utf8_lossy(&out.stderr);
                                    if out.status.success() && pruned_states.exists() {
                                        let staging = work_dir.join("staging");
                                        let _ = std::fs::remove_dir_all(&staging);
                                        std::fs::create_dir_all(&staging).expect("Failed staging");

                                        let mut staging_ok = true;
                                        if let Err(e) = create_hardlink_checkpoint(&pruned_states, &staging.join("states")) {
                                            eprintln!("⚠️ Failed to hard-link pruned states into staging: {}", e);
                                            staging_ok = false;
                                        }

                                        if staging_ok {
                                            for dir in STATE_LINK_DIRS {
                                                let src = source_path.join(dir);
                                                if src.exists() {
                                                    if let Err(e) = create_hardlink_checkpoint(&src, &staging.join(dir)) {
                                                        eprintln!("⚠️ Failed to hard-link {} into staging: {}", dir, e);
                                                        staging_ok = false;
                                                        break;
                                                    }
                                                }
                                            }
                                        }

                                        if staging_ok {
                                            staging_dir = Some(staging.clone());
                                            prune_work_dir = Some(work_dir);
                                            if !json {
                                                eprintln!("✅ Prune complete: {}", staging.display());
                                            }
                                        } else {
                                            let _ = std::fs::remove_dir_all(&work_dir);
                                        }
                                    } else {
                                        eprintln!("⚠️ nc-pruner failed: {}", stderr);
                                        let _ = std::fs::remove_dir_all(&work_dir);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("⚠️ Failed to run nc-pruner: {}", e);
                                    let _ = std::fs::remove_dir_all(&work_dir);
                                }
                            }
                        }
                        None => {
                            eprintln!("⚠️ nc-pruner not found. Use --pruner-path.");
                        }
                    }
                } // end else (not live)
            } // end if prune

            // ══════════════════════════════════════════════════════════
            // LIVE CHECKPOINT (when --live without --prune)
            //
            // Create hard-link checkpoint of states/ (and block/ tx/ for partition)
            // to get a consistent point-in-time view while the node is running.
            // Hard-links are instant (no data copy) and don't affect the original files.
            // When --live WITH --prune, nc-pruner already creates its own checkpoint.
            // ══════════════════════════════════════════════════════════
            let mut live_checkpoint_dir: Option<PathBuf> = None;

            if live && staging_dir.is_none() {
                if !json {
                    eprintln!("📸 Creating live checkpoint (hard-links)...");
                }

                let output_parent = final_output.parent().unwrap_or(Path::new("."));
                let checkpoint_base = output_parent.join(".nc-snapshot-live-checkpoint");
                let _ = std::fs::remove_dir_all(&checkpoint_base);
                std::fs::create_dir_all(&checkpoint_base)
                    .expect("Failed to create live checkpoint directory");

                // Determine which dirs need hard-link checkpoints
                // states/ always needs a checkpoint for consistency
                // block/ and tx/ need checkpoints in partition mode (they're actively written)
                let dirs_to_checkpoint: Vec<&str> = match mode_enum {
                    SnapshotMode::State => vec!["states"],
                    SnapshotMode::Partition => vec!["block", "tx"],
                    SnapshotMode::Full => vec!["states", "block", "tx"],
                };

                let mut checkpoint_ok = true;
                for dir_name in &dirs_to_checkpoint {
                    let src_dir = source_path.join(dir_name);
                    if !src_dir.exists() {
                        if !json {
                            eprintln!("   ⚠️ Skipping {} (not found)", dir_name);
                        }
                        continue;
                    }

                    let dst_dir = checkpoint_base.join(dir_name);
                    if let Err(e) = create_hardlink_checkpoint(&src_dir, &dst_dir) {
                        eprintln!("⚠️ Failed to checkpoint {}: {}", dir_name, e);
                        checkpoint_ok = false;
                        break;
                    }
                    if !json {
                        eprintln!("   ✓ Checkpointed {}/", dir_name);
                    }
                }

                if checkpoint_ok {
                    // Create staging dir that uses hard-links for ALL dirs.
                    // Symlinks are NOT used because WalkDir does not follow them,
                    // which would result in empty / broken archives.
                    let staging = checkpoint_base.join("staging");
                    std::fs::create_dir_all(&staging).expect("Failed to create staging dir");

                    // Hard-link checkpointed dirs into staging
                    let mut staging_ok = true;
                    for dir_name in &dirs_to_checkpoint {
                        let ckpt = checkpoint_base.join(dir_name);
                        if ckpt.exists() {
                            if let Err(e) = create_hardlink_checkpoint(&ckpt, &staging.join(dir_name)) {
                                eprintln!("⚠️ Failed to hard-link checkpoint {} into staging: {}", dir_name, e);
                                staging_ok = false;
                                break;
                            }
                        }
                    }

                    // Hard-link static dirs from original source
                    if staging_ok && (mode_enum == SnapshotMode::State || mode_enum == SnapshotMode::Full) {
                        for dir in STATE_LINK_DIRS {
                            if dirs_to_checkpoint.contains(dir) {
                                continue; // Already checkpointed
                            }
                            let src = source_path.join(dir);
                            if src.exists() {
                                if let Err(e) = create_hardlink_checkpoint(&src, &staging.join(dir)) {
                                    eprintln!("⚠️ Failed to hard-link {} into staging: {}", dir, e);
                                    staging_ok = false;
                                    break;
                                }
                            }
                        }
                    }

                    if staging_ok {
                        staging_dir = Some(staging.clone());
                        live_checkpoint_dir = Some(checkpoint_base.clone());

                        if !json {
                            eprintln!("✅ Live checkpoint created");
                            eprintln!("   Checkpoint: {}", checkpoint_base.display());
                        }
                    } else {
                        eprintln!("⚠️ Live checkpoint staging failed, archiving from live source (may be inconsistent)");
                        let _ = std::fs::remove_dir_all(&checkpoint_base);
                    }
                } else {
                    eprintln!("⚠️ Live checkpoint failed, archiving from live source (may be inconsistent)");
                    let _ = std::fs::remove_dir_all(&checkpoint_base);
                }
            }

            // Use staging dir if prune succeeded, otherwise original store
            let effective_source = staging_dir
                .as_ref()
                .unwrap_or(&source_path)
                .clone();

            if staging_dir.is_some() && !json {
                eprintln!("   Archiving from staging (9c-blockchain untouched)");
            }

            let config = SnapshotConfig {
                source: effective_source,
                output: final_output,
                level,
                threads,
                exclude,
                include,
                mode: mode_enum,
                epoch_limit,
                force,
                json,
                dry_run,
                incremental,
                apv,
                block_before,
            };

            // ── Execute snapshot creation ──
            let snapshot_result = snapshot::create_snapshot(&config, bridge_res);

            // ── Cleanup: remove staging and work dir ──
            // 9c-blockchain/ is NEVER touched — only our temp dirs are cleaned
            if let Some(ref work) = prune_work_dir {
                if !json {
                    eprintln!("🧹 Cleaning up staging directory...");
                }
                let _ = std::fs::remove_dir_all(work);
            }

            // Clean up live checkpoint if we created one
            if let Some(ref ckpt) = live_checkpoint_dir {
                if !json {
                    eprintln!("🧹 Cleaning up live checkpoint...");
                }
                let _ = std::fs::remove_dir_all(ckpt);
            }

            match snapshot_result {
                Ok(result) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&result).unwrap());
                    }
                }
                Err(e) => {
                    eprintln!("❌ Snapshot failed: {:#}", e);
                    process::exit(1);
                }
            }
        }

        Commands::Verify { archive, json } => match verify::verify_archive(&archive, json) {
            Ok(result) => {
                if json {
                    println!("{}", serde_json::to_string_pretty(&result).unwrap());
                }
            }
            Err(e) => {
                eprintln!("❌ Verification failed: {:#}", e);
                process::exit(1);
            }
        },
    }
}
