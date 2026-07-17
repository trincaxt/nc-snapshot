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

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process;
use std::time::Instant;
use types::{BridgeResult, SnapshotConfig, SnapshotMode, BlockMetadata};
use std::fs;


const EPOCH_UNIT_SECONDS: i64 = 86400; // 24 horas


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

/// Gera metadata 100% Rust puro (sem C# bridge).
fn fetch_metadata_hybrid(
    source: &Path,
    apv: &str,
    block_before: i32,
    _mode: &str,
    output_dir: &Path,
    json_output: bool,
) -> anyhow::Result<BridgeResult> {
    if !json_output {
        eprintln!("🟢 Fetching metadata from checkpoint (no LOCK conflicts)...");
    }

    let (metadata_json, partition_filename, latest_epoch) = 
        generate_metadata_rust(source, apv, block_before, output_dir, json_output)?;

    let current_metadata_block_epoch = get_metadata_epoch(&output_dir.join("metadata"), "BlockEpoch");
    let previous_metadata_block_epoch = get_metadata_epoch(&output_dir.join("metadata"), "PreviousBlockEpoch");

    Ok(BridgeResult {
        success: true,
        error: None,
        partition_base_filename: partition_filename,
        state_base_filename: "state_latest".to_string(),
        latest_epoch,
        current_metadata_block_epoch,
        previous_metadata_block_epoch,
        stringfy_metadata: metadata_json,
    })
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

/// Lê o epoch do metadata anterior (metadata/*.json mais recente).
/// Retorna 0 se não houver metadata anterior.
fn get_metadata_epoch(metadata_dir: &Path, epoch_type: &str) -> i32 {
    if !metadata_dir.exists() {
        return 0;
    }

    match fs::read_dir(metadata_dir) {
        Ok(entries) => {
            let mut json_files: Vec<_> = entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == "json")
                        .unwrap_or(false)
                })
                .collect();

            if json_files.is_empty() {
                return 0;
            }

            // Ordena por mtime (mais recente primeiro)
            json_files.sort_by(|a, b| {
                let a_meta = a.metadata().ok();
                let b_meta = b.metadata().ok();
                let a_time = a_meta.and_then(|m| m.modified().ok());
                let b_time = b_meta.and_then(|m| m.modified().ok());
                b_time.cmp(&a_time)
            });

            // Lê o primeiro (mais recente)
            if let Some(file) = json_files.first() {
                if let Ok(content) = fs::read_to_string(file.path()) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                        if let Some(epoch) = json.get(epoch_type).and_then(|v| v.as_i64()) {
                            return epoch as i32;
                        }
                    }
                }
            }

            0
        }
        Err(_) => 0,
    }
}

/// Calcula o nome base do arquivo partition.
fn get_partition_base_filename(
    current_metadata_block_epoch: i32,
    current_metadata_tx_epoch: i32,
    latest_epoch: i32,
) -> String {
    if current_metadata_block_epoch == 0 && current_metadata_tx_epoch == 0 {
        format!("snapshot-{}-{}", latest_epoch - 1, latest_epoch - 1)
    } else {
        format!("snapshot-{}-{}", latest_epoch, latest_epoch)
    }
}

/// Gera o metadata JSON em Rust puro (byte-idêntico ao C# bridge).
fn generate_metadata_rust(
    checkpoint_base: &Path,
    apv: &str,
    block_before: i32,
    output_dir: &Path,
    json_output: bool,
) -> anyhow::Result<(String, String, i32)> {
    use anyhow::Context;

    // 1. Lê informações completas do header
    let header = chain_reader::read_block_header_from_checkpoint(checkpoint_base, block_before as u64)?;

    // 2. Calcula latest epoch (do timestamp do bloco)
    let timestamp_parsed = chrono::DateTime::parse_from_rfc3339(&header.timestamp)
        .context("Failed to parse timestamp")?;
    let latest_epoch = (timestamp_parsed.timestamp() / EPOCH_UNIT_SECONDS) as i32;

    // 3. Lê epochs do metadata anterior
    let metadata_dir = output_dir.join("metadata");
    let current_metadata_block_epoch = get_metadata_epoch(&metadata_dir, "BlockEpoch");
    let current_metadata_tx_epoch = get_metadata_epoch(&metadata_dir, "TxEpoch");
    let previous_metadata_block_epoch = get_metadata_epoch(&metadata_dir, "PreviousBlockEpoch");

    // 4. Calcula previous epochs
    let (previous_block_epoch, previous_tx_epoch) = if current_metadata_block_epoch == latest_epoch {
        (previous_metadata_block_epoch, previous_metadata_block_epoch)
    } else {
        (current_metadata_block_epoch, current_metadata_tx_epoch)
    };

    // 5. Calcula block/tx epochs
    let (block_epoch, tx_epoch) = if current_metadata_block_epoch == 0 && current_metadata_tx_epoch == 0 {
        (latest_epoch - 1, latest_epoch - 1)
    } else {
        (latest_epoch, latest_epoch)
    };

    // 6. Monta o metadata
    let metadata = BlockMetadata {
        index: header.index,
        timestamp: header.timestamp.clone(),
        state_root_hash: hex::encode(header.state_root_hash),
        previous_hash: hex::encode(header.previous_hash),
        tx_hash: header.tx_hash.map(|h| hex::encode(h)),
        apv: apv.to_string(),
        block_epoch,
        tx_epoch,
        previous_block_epoch,
        previous_tx_epoch,
    };

    // 7. Serializa para JSON (sem formatação, igual ao C#)
    let metadata_json = serde_json::to_string(&metadata)
        .context("Failed to serialize metadata")?;

    // 8. Nome do arquivo partition
    let partition_filename = get_partition_base_filename(
        current_metadata_block_epoch,
        current_metadata_tx_epoch,
        latest_epoch,
    );

    Ok((metadata_json, partition_filename, latest_epoch))
}

/// Executa o GC Pipeline completo 🦀.
fn run_gc_pipeline(
    source_states: &Path,
    dest_states: &Path,
    root_hash_hex: &str,
    json: bool,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let pipeline_start = Instant::now();

    let checkpoint_dir = source_states.parent().unwrap_or(Path::new("."));
    let export_file = checkpoint_dir.join("states_export.bin");
    let live_keys_file = checkpoint_dir.join("live_keys.bin");

    if !json {
        eprintln!("  Source: {}", source_states.display());
        eprintln!("  Dest : {}", dest_states.display());
        eprintln!("  Root : {}...", &root_hash_hex[..16]);
        eprintln!("  ⏳ Running GC Pipeline (Export + BFS + Prune + Validate )...");
        eprintln!();
    }

    // ── Phase 1: Export (RUST) ──────────────────────────────────────
    let phase1_start = Instant::now();
    if !json {
        eprintln!("📤 Phase 1: Exporting states/ 🦀...");
    }

    let export_result = exporter::export_states(source_states, &export_file)?;
    let phase1_elapsed = phase1_start.elapsed().as_secs_f64();

    if !json {
        eprintln!("  ✅ Exported {:.0} entries in {:.1}s  |  {:.1} min",
                  export_result.total_entries,
                  export_result.elapsed_secs,
                  phase1_elapsed / 60.0);
    }

    // ── Phase 2: BFS (RUST) ──────────────────────────────────────
    let phase2_start = Instant::now();
    if !json {
        eprintln!("🌳 Phase 2: BFS ( 🦀 - SCAN SEQUENCIAL - 🦀 )...");
    }

    let root_bytes = hex_to_hash32(root_hash_hex)?;
    let roots = vec![root_bytes];

    gc_filter::run_gc_filter(
        &export_file,
        roots,
        &live_keys_file,
    )?;
    let phase2_elapsed = phase2_start.elapsed().as_secs_f64();

    // ── Phase 3: Prune (RUST) ──────────────────────────────────────
    let phase3_start = Instant::now();
    if !json {
        eprintln!("🗑️ Phase 3: Prune  🦀 ...");
    }

    let result = pruner::prune_states(
        source_states,
        dest_states,
        &live_keys_file,
        json,
    )?;
    let phase3_elapsed = phase3_start.elapsed().as_secs_f64();

    if !json {
        eprintln!("  ✅ Prune: {} kept, {} deleted  |  {:.1} min",
                  result.nodes_copied,
                  result.nodes_deleted,
                  phase3_elapsed / 60.0);
    }

    // ── Phase 4: Validate (RUST 🦀) ─────────────────────────────────
    let phase4_start = Instant::now();
    if !json {
        eprintln!("🔍 Phase 4: Validating pruned states/ 🦀...");
    }

    chain_reader::validate_states(dest_states)
        .context("Failed to validate pruned states/")?;
    let phase4_elapsed = phase4_start.elapsed().as_secs_f64();

    if !json {
        eprintln!("  ✅ Validation passed!  |  {:.1}s", phase4_elapsed);
    }

    // ── Cleanup temporary files ────────────────────────────────
    let _ = std::fs::remove_file(&export_file);
    let _ = std::fs::remove_file(&live_keys_file);

    if !json {
        let total_elapsed = pipeline_start.elapsed().as_secs_f64();
        let total_nodes = result.nodes_copied + result.nodes_deleted;
        let pct_removed = if total_nodes > 0 {
            result.nodes_deleted as f64 / total_nodes as f64 * 100.0
        } else {
            0.0
        };

        eprintln!("✅ GC Pipeline complete! 🦀");
        eprintln!("  📊 Phase 1 (Export): {:.1} min", phase1_elapsed / 60.0);
        eprintln!("  📊 Phase 2 (BFS): {:.1} min", phase2_elapsed / 60.0);
        eprintln!("  📊 Phase 3 (Prune): {:.1} min", phase3_elapsed / 60.0);
        eprintln!("  📊 Phase 4 (Validate): {:.1} min", phase4_elapsed / 60.0);
        eprintln!("  💾 Nodes: {} → {} ({} deleted, {:.1}% removed)",
                  total_nodes,
                  result.nodes_copied,
                  result.nodes_deleted,
                  pct_removed);
    }

    Ok(())
}

/// Converte hex string para [u8; 32]
fn hex_to_hash32(hex: &str) -> anyhow::Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    if hex.len() != 64 {
        anyhow::bail!("Invalid hex length: expected 64, got {}", hex.len());
    }
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i*2..i*2+2], 16)?;
    }
    Ok(bytes)
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
                    match fetch_metadata_hybrid(&source_path, apv_val, block_before, &mode, metadata_output_dir, json) {
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
                            match fetch_metadata_hybrid(&checkpoint_base, apv_val, block_before, &mode, metadata_output_dir, json) {
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

                            match run_gc_pipeline(
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
