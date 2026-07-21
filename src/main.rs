mod errors;
mod node_detect;
mod snapshot;
mod types;
mod verify;
mod gc_filter;
mod pruner;
mod exporter;
mod chain_reader;
mod checkpoint_secondary;
mod io_util;
mod metadata;
mod pipeline;

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process;
use types::{SnapshotConfig, SnapshotMode};

#[derive(Parser, Debug)]
#[command(
name = "nc-snapshot",
about = "⚡ Nine Chronicles blockchain snapshot tool",
version,
long_about = "Fast, production-grade snapshot tool for Nine Chronicles blockchain.\n\
Creates tar.zst archives with BLAKE3 integrity verification.\n\n\
Modes:\n\
- state (default)   State snapshot: indexes + state data (~127 GiB)\n\
- partition         Base/partition snapshot: block + tx epochs (~230+ GiB)\n\
- full              Full snapshot: everything in the store directory"
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

        /// Epoch limit for partition mode (skip epochs below this number) - ARQUIVA apenas
        #[arg(long)]
        epoch_limit: Option<u64>,

        /// Epoch validation limit - VALIDA apenas epochs >= this number (saves time)
        #[arg(long)]
        epoch_validate: Option<u64>,

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

/// Creates a consistent RocksDB checkpoint using secondary mode (100% Rust 🦀).
fn checkpoint_db(rel_path: &str, src: &Path, dst: &Path, json: bool) -> anyhow::Result<PathBuf> {
    if !json {
        eprintln!("    🦀 {} via secondary mode 🦀...", rel_path);
    }
    
    checkpoint_secondary::create_checkpoint_secondary(src, dst)?;
    Ok(dst.to_path_buf())
}

/// Lê o StateRootHash do checkpoint — 100% Rust puro.
fn get_state_root_from_checkpoint_hybrid(
    checkpoint_base: &Path,
    block_before: i32,
    json: bool,
) -> anyhow::Result<(String, i64)> {
    if !json {
        eprintln!("  Getting state root from checkpoint...");
    }

    let tip = chain_reader::read_state_root_from_checkpoint(
        checkpoint_base,
        block_before as u64,
    )?;

    let srh = hex::encode(tip.state_root_hash);
    if !json {
        eprintln!("  StateRoot: {}...", &srh[..16]);
        eprintln!("  Block #{}", tip.block_index);
    }

    Ok((srh, tip.block_index))
}

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
            epoch_validate,
            apv,
            block_before,
            force,
                json,
                dry_run,
                incremental,
                prune,
                pruner_path: _,
                prune_depth: _,
                live,
        } => {
            let source_path = expand_tilde(&source);
            let threads = if threads == 0 {
                num_cpus::get()
            } else {
                threads
            };
            let mode_enum: SnapshotMode = mode.parse().unwrap_or(SnapshotMode::State);

            // Pega o valor da flag epoch_validate
            let validate_epoch = epoch_validate.unwrap_or(0);

            // ── OFFLINE: fetch_metadata ANTES do checkpoint (source sem LOCK) ──
            // ── LIVE: fetch_metadata DEPOIS do checkpoint (no checkpoint, sem LOCK) ──
            let mut bridge_res = None;
            if !live && mode_enum == SnapshotMode::Partition {
                if let Some(ref apv_val) = apv {
                    if !json {
                        eprintln!("🚀 Fetching blockchain metadata...");
                    }
                    let metadata_output_dir = output_dir.as_ref().map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));
                    match metadata::fetch_metadata_hybrid(&source_path, apv_val, block_before, &mode, metadata_output_dir, json) {
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

            // Output path inicial (sem bridge para live, com bridge para offline)
            let mut final_output = if let Some(p) = output {
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
                eprintln!("║   ⚡ NC Blockchain Snapshot Tool ⚡       ║");
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
                if validate_epoch > 0 {
                    eprintln!("  Validate≥: {}", validate_epoch);
                }
                eprintln!();
            }

            if live {
                if !json {
                    eprintln!("🟢 Live mode: skipping node detection (using checkpoints for consistency)");
                    eprintln!("   ⚠️ Snapshot may be slightly behind the chain tip");
                }
            } else if !force {
                let locked = node_detect::check_node_running(&source_path);
                if !locked.is_empty() {
                    eprintln!("⚠️ Node appears to be running!");
                    process::exit(1);
                }
            }

            // LIVE CHECKPOINT
            let mut staging_dir: Option<PathBuf> = None;
            let mut live_checkpoint_dir: Option<PathBuf> = None;

            if live && staging_dir.is_none() {
                let output_parent = final_output.parent().unwrap_or(Path::new("."));
                let checkpoint_base = output_parent.join(".nc-snapshot-live-checkpoint");

                let _ = std::fs::remove_dir_all(&checkpoint_base);
                let _ = std::fs::create_dir_all(&checkpoint_base);

                if !json {
                    eprintln!("📸 Creating live checkpoint via secondary mode 🦀...");
                    eprintln!("   Using RocksDB secondary mode for consistent checkpoints");
                    eprintln!("   Expected time: ~seconds");
                }

                let mut checkpoint_ok = true;

                match mode_enum {
                    SnapshotMode::State => {
                        if !json {
                            eprintln!("  State mode: linking state DBs + indexes");
                        }

                        // ── MESMOS DBs QUE O FULL (menos blockpercept, txpercept, etc) ──
                        let state_dbs = [
                            ("states", "states"),
                            ("chain", "chain"),
                            ("blockcommit", "blockcommit"),
                            ("txexec", "txexec"),
                            ("txbindex", "txbindex"),
                            ("block/blockindex", "block/blockindex"),
                            ("tx/txindex", "tx/txindex"),
                            // ⚠️ NÃO INCLUIR blockpercept, txpercept, nextstateroothash, evidencec, evidencep
                        ];

                        for (rel_path, display_name) in &state_dbs {
                            let src = source_path.join(rel_path);
                            if src.exists() {
                                let dst = checkpoint_base.join(rel_path);
                                if !json {
                                    eprintln!("    Linking checkpoint for {}...", display_name);
                                }
                                match checkpoint_db(rel_path, &src, &dst, json) {
                                    Ok(_) => {
                                        if !json {
                                            eprintln!("    ✓ {} linked", display_name);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Failed to checkpoint {}: {}", display_name, e);
                                        checkpoint_ok = false;
                                        break;
                                    }
                                }
                            }
                        }

                        // ⚠️ NÃO processa block/ epochs ou tx/ epochs (só no partition/full)
                    }

                    SnapshotMode::Partition => {
                        // For partition mode: block/ and tx/ contain THOUSANDS of epoch subdirs
                        // Each epoch is a separate RocksDB — hard-linked individually

                        if !json {
                            eprintln!("  Partition mode: linking states + all DBs + epochs >= {}", validate_epoch);
                        }

                        // Primeiro os DBs únicos (como Full mode) — necessários para
                        // o bridge (chain/, states/) e para o state archive
                        let single_dbs = [
                            ("states", "states"),
                            ("chain", "chain"),
                            ("blockcommit", "blockcommit"),
                            ("txexec", "txexec"),
                            ("txbindex", "txbindex"),
                            ("block/blockindex", "block/blockindex"),
                            ("tx/txindex", "tx/txindex"),
                            ("blockpercept", "blockpercept"),
                            ("txpercept", "txpercept"),
                            ("nextstateroothash", "nextstateroothash"),
                            ("evidencec", "evidencec"),
                            ("evidencep", "evidencep"),
                            ("stagedtx", "stagedtx"),
                        ];

                        for (rel_path, display_name) in &single_dbs {
                            let src = source_path.join(rel_path);
                            if src.exists() {
                                let dst = checkpoint_base.join(rel_path);
                                if !json {
                                    eprintln!("    Linking checkpoint for {}/...", display_name);
                                }
                                match checkpoint_db(rel_path, &src, &dst, json) {
                                    Ok(_) => {
                                        if !json {
                                            eprintln!("    ✓ {} linked", display_name);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Failed to checkpoint {}: {}", display_name, e);
                                        checkpoint_ok = false;
                                        break;
                                    }
                                }
                            }
                        }

                        // Agora as epochs (block/ + tx/) — já incluímos block/blockindex e tx/txindex no single_dbs acima
                        if checkpoint_ok {
                            let block_root = source_path.join("block");
                            if block_root.exists() {
                                let block_dst_root = checkpoint_base.join("block");
                                std::fs::create_dir_all(&block_dst_root).ok();

                                if !json {
                                    eprintln!("  📦 Processing block epochs (>= {})...", validate_epoch);
                                }

                                // 🦀 100% RUST: Using secondary mode for all epochs
                                match checkpoint_secondary::checkpoint_batch_epochs(
                                    "block",
                                    &source_path,
                                    &checkpoint_base,
                                    validate_epoch,
                                ) {
                                    Ok(_) => {
                                        if !json {
                                            eprintln!("  ✅ block/ epochs linked");
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Failed to checkpoint block/ epochs: {}", e);
                                        checkpoint_ok = false;
                                    }
                                }
                            }
                        }

                        if checkpoint_ok {
                            let tx_root = source_path.join("tx");
                            if tx_root.exists() {
                                let tx_dst_root = checkpoint_base.join("tx");
                                std::fs::create_dir_all(&tx_dst_root).ok();

                                if !json {
                                    eprintln!("  📦 Processing tx epochs (>= {})...", validate_epoch);
                                }

                                // 🦀 100% RUST: Using secondary mode for tx epochs
                                match checkpoint_secondary::checkpoint_batch_epochs(
                                    "tx",
                                    &source_path,
                                    &checkpoint_base,
                                    validate_epoch,
                                ) {
                                    Ok(_) => {
                                        if !json {
                                            eprintln!("  ✅ tx/ epochs linked");
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Failed to checkpoint tx/ epochs: {}", e);
                                        checkpoint_ok = false;
                                    }
                                }
                            }
                        }
                    }

                    SnapshotMode::Full => {
                        if !json {
                            eprintln!("  Full mode: linking states + all DBs + epochs >= {}", validate_epoch);
                        }

                        let single_dbs = [
                            ("states", "states"),
                            ("chain", "chain"),
                            ("blockcommit", "blockcommit"),
                            ("txexec", "txexec"),
                            ("txbindex", "txbindex"),
                            ("block/blockindex", "block/blockindex"),
                            ("tx/txindex", "tx/txindex"),
                            ("blockpercept", "blockpercept"),
                            ("txpercept", "txpercept"),
                            ("nextstateroothash", "nextstateroothash"),
                            ("stagedtx", "stagedtx"),
                            ("evidencec", "evidencec"),
                            ("evidencep", "evidencep"),
                        ];

                        for (rel_path, display_name) in &single_dbs {
                            let src = source_path.join(rel_path);
                            if src.exists() {
                                let dst = checkpoint_base.join(rel_path);
                                if !json {
                                    eprintln!("    Linking checkpoint for {}/...", display_name);
                                }
                                match checkpoint_db(rel_path, &src, &dst, json) {
                                    Ok(_) => {
                                        if !json {
                                            eprintln!("    ✓ {} linked", display_name);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Failed to checkpoint {}: {}", display_name, e);
                                        checkpoint_ok = false;
                                        break;
                                    }
                                }
                            }
                        }

                        if checkpoint_ok {
                            let block_root = source_path.join("block");
                            if block_root.exists() {
                                let block_dst_root = checkpoint_base.join("block");
                                std::fs::create_dir_all(&block_dst_root).ok();

                                if !json {
                                    eprintln!("  📦 Processing block epochs (>= {})...", validate_epoch);
                                }

                                // 🦀 100% RUST: Using secondary mode for all epochs
                                match checkpoint_secondary::checkpoint_batch_epochs(
                                    "block",
                                    &source_path,
                                    &checkpoint_base,
                                    validate_epoch,
                                ) {
                                    Ok(_) => {
                                        if !json {
                                            eprintln!("  ✅ block/ epochs linked");
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Failed to checkpoint block/ epochs: {}", e);
                                        checkpoint_ok = false;
                                    }
                                }
                            }
                        }

                        if checkpoint_ok {
                            let tx_root = source_path.join("tx");
                            if tx_root.exists() {
                                let tx_dst_root = checkpoint_base.join("tx");
                                std::fs::create_dir_all(&tx_dst_root).ok();

                                if !json {
                                    eprintln!("  📦 Processing tx epochs (>= {})...", validate_epoch);
                                }

                                // 🦀 100% RUST: Using secondary mode for tx epochs
                                match checkpoint_secondary::checkpoint_batch_epochs(
                                    "tx",
                                    &source_path,
                                    &checkpoint_base,
                                    validate_epoch,
                                ) {
                                    Ok(_) => {
                                        if !json {
                                            eprintln!("  ✅ tx/ epochs linked");
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Failed to checkpoint tx/ epochs: {}", e);
                                        checkpoint_ok = false;
                                    }
                                }
                            }
                        }
                    }
                }

                if checkpoint_ok {
                    // ── LIVE: fetch_metadata só no Partition mode ──
                    if mode_enum == SnapshotMode::Partition {
                    if let Some(ref apv_val) = apv {
                        if bridge_res.is_none() {
                            let metadata_output_dir = output_dir.as_ref().map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));
                            match metadata::fetch_metadata_hybrid(&checkpoint_base, apv_val, block_before, &mode, metadata_output_dir, json) {
                                Ok(res) => {
                                    if !res.success {
                                        eprintln!("❌ Bridge error: {:?}", res.error);
                                    } else {
                                        if epoch_limit.is_none() {
                                            epoch_limit = Some(res.current_metadata_block_epoch as u64);
                                        }
                                        let parent = final_output.parent().unwrap_or(Path::new(".")).to_path_buf();
                                        final_output = parent.join(format!("{}.tar.zst", res.partition_base_filename));
                                        bridge_res = Some(res);
                                        if !json {
                                            eprintln!("  ✅ Metadata fetched, output: {}", final_output.display());
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("⚠️ Failed to fetch metadata from checkpoint: {}", e);
                                }
                            }
                        }
                    }
                    }

                    if prune && (mode_enum == SnapshotMode::State || mode_enum == SnapshotMode::Full || mode_enum == SnapshotMode::Partition) {
                        if !json {
                            eprintln!("🧹 Running GC Pipeline on checkpoint states/...");
                        }

                        let state_root_hex = match get_state_root_from_checkpoint_hybrid(
                            &checkpoint_base,
                            block_before,
                            json,
                        ) {
                            Ok((srh, _block_index)) => srh,
                            Err(e) => {
                                eprintln!("⚠ Could not read state root: {} — skipping prune", e);
                                String::new()
                            }
                        };

                        if !state_root_hex.is_empty() {
                            let states_src = checkpoint_base.join("states");
                            let states_gc = checkpoint_base.join("states_gc");

                            match pipeline::run_gc_pipeline(
                                &states_src,
                                &states_gc,
                                &state_root_hex,
                                json,
                            ) {
                                Ok(_) => {
                                    if let Err(e) = std::fs::remove_dir_all(&states_src) {
                                        eprintln!("⚠ Failed to remove old states/: {} — keeping original", e);
                                    } else if let Err(e) = std::fs::rename(&states_gc, &states_src) {
                                        eprintln!("⚠ Failed to swap states_gc → states: {} — keeping original", e);
                                    } else if !json {
                                        eprintln!("✅ states/ pruned and swapped");
                                    }
                                }
                                Err(e) => {
                                    eprintln!("⚠ GC Pipeline failed: {} — using unpruned states/", e);
                                    let _ = std::fs::remove_dir_all(&states_gc);
                                }
                            }
                        }
                    }

                    if !json {
                        eprintln!("✅ Live checkpoint created");
                        eprintln!("   Checkpoint: {}", checkpoint_base.display());
                        eprintln!("   Archiving from staging (9c-blockchain untouched)");
                    }
                    staging_dir = Some(checkpoint_base.clone());
                    live_checkpoint_dir = Some(checkpoint_base);
                } else {
                    eprintln!("❌ Live checkpoint failed");
                    eprintln!("   Consider stopping the node and using offline snapshot");
                    let _ = std::fs::remove_dir_all(&checkpoint_base);
                    process::exit(1);
                }
            }

            let effective_source = staging_dir
            .as_ref()
            .unwrap_or(&source_path)
            .clone();

            if staging_dir.is_some() && !json {
                eprintln!("   Archiving from staging (9c-blockchain untouched)");
            }

            let mut final_exclude = exclude.clone();

            if mode_enum == SnapshotMode::Full {
                let compat_dirs = vec![
                    "blockpercept".to_string(),
                    "txpercept".to_string(),
                    "nextstateroothash".to_string(),
                    "evidencec".to_string(),
                    "evidencep".to_string(),
                    "stagedtx".to_string(),
                ];
                final_exclude.extend(compat_dirs);
            }

            let config = SnapshotConfig {
                source: effective_source,
                output: final_output,
                level,
                threads,
                exclude: final_exclude,
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

            let snapshot_result = snapshot::create_snapshot(&config, bridge_res);

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
