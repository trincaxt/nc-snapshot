mod errors;
mod node_detect;
mod snapshot;
mod types;
mod verify;
mod gc_filter;
mod pruner;
mod exporter;

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

    // Bridge sempre escreve o resultado JSON no stdout (mesmo em erro)
    let stdout = String::from_utf8_lossy(&output.stdout);
    let last_line = stdout
    .lines()
    .last()
    .ok_or_else(|| anyhow::anyhow!("Empty bridge output"))?;
    let res: BridgeResult = serde_json::from_str(last_line)?;

    if !output.status.success() || !res.success {
        let err_msg = res.error.unwrap_or_else(|| "Unknown error".to_string());
        let stderr_msg = String::from_utf8_lossy(&output.stderr);
        let details = if stderr_msg.is_empty() { err_msg } else { format!("{} | stderr: {}", err_msg, stderr_msg) };
        anyhow::bail!("C# Bridge failed: {}", details);
    }

    Ok(res)
}

/// Hard-link ou copia (fallback cross-device) um único arquivo.
fn link_or_copy(src: &Path, dst: &Path) -> anyhow::Result<()> {
    match std::fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(_) => {
            std::fs::copy(src, dst)?;
            Ok(())
        }
    }
}

/// Checkpoint consistente via CheckpointBridge (secondary + catch_up + flush).
/// Usado SÓ pros DBs de índice pequenos que o metadata/state-root leem cross-DB,
/// onde o hardlink cru perde a última memtable e quebra o lookup de tip.
fn create_validated_checkpoint_via_bridge(
    src_dir: &Path,
    dst_dir: &Path,
) -> anyhow::Result<PathBuf> {
    use anyhow::Context;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct CheckpointResult {
        #[serde(rename = "Success")]
        success: bool,
        #[serde(rename = "ValidatedPath")]
        validated_path: Option<String>,
        #[serde(rename = "Error")]
        error: Option<String>,
    }

    let bridge_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("bridge-bin")
        .join("checkpoint")
        .join("CheckpointBridge");

    if !bridge_path.exists() {
        anyhow::bail!("CheckpointBridge not found: {}", bridge_path.display());
    }

    let output = Command::new(&bridge_path)
        .arg(src_dir.to_string_lossy().as_ref())
        .arg(dst_dir.to_string_lossy().as_ref())
        .output()
        .with_context(|| format!("Failed to execute CheckpointBridge: {}", bridge_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        anyhow::bail!("CheckpointBridge failed: {}\nStdout: {}\nStderr: {}", output.status, stdout, stderr);
    }

    let stdout = String::from_utf8(output.stdout)?;
    let result: CheckpointResult = serde_json::from_str(&stdout)
        .with_context(|| format!("Failed to parse bridge output: {}", stdout))?;

    if result.success {
        result.validated_path
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("Bridge returned success but no path"))
    } else {
        anyhow::bail!("CheckpointBridge failed: {}", result.error.unwrap_or_else(|| "Unknown error".to_string()));
    }
}

/// DBs de índice que precisam de checkpoint consistente (tip completo cross-DB).
/// Todo o resto (states/ + epochs de block/tx) vai de hardlink Rust — rápido.
const CONSISTENT_INDEX_DIRS: &[&str] = &[
    "states",
    "chain",
    "block/blockindex",
    "tx/txindex",
    "nextstateroothash",
];

/// Escolhe a estratégia por DB: consistente (C#) pros índices, hardlink (Rust) pro resto.
fn checkpoint_db(rel_path: &str, src: &Path, dst: &Path, json: bool) -> anyhow::Result<PathBuf> {
    if CONSISTENT_INDEX_DIRS.contains(&rel_path) {
        if !json {
            eprintln!("    🔒 {} via checkpoint consistente (C#)...", rel_path);
        }
        create_validated_checkpoint_via_bridge(src, dst)
    } else {
        if !json {
            eprintln!("    🦀 {} via hard-link (Rust)...", rel_path);
        }
        create_checkpoint_hardlink(src, dst, json)
    }
}

/// Checkpoint via hard-link em duas passadas (Rust 🦀, ~instantâneo).
/// SSTs (imutáveis) primeiro, MANIFEST/CURRENT/OPTIONS por último — assim o
/// MANIFEST só referencia SSTs que já existem no destino. Formato IDÊNTICO ao
/// que o Libplanet escreveu (sem re-serialização, sem mismatch de librocksdb).
/// Substitui create_validated_checkpoint_via_bridge (C#, 5-30 min).
fn create_checkpoint_hardlink(src_dir: &Path, dst_dir: &Path, _json: bool) -> anyhow::Result<PathBuf> {
    use anyhow::Context;
    use std::fs;

    if dst_dir.exists() {
        fs::remove_dir_all(dst_dir).with_context(|| format!("rm dst: {}", dst_dir.display()))?;
    }
    fs::create_dir_all(dst_dir).with_context(|| format!("mkdir dst: {}", dst_dir.display()))?;

    let scan_ssts = |acc: &mut Vec<(PathBuf, String)>| -> anyhow::Result<()> {
        for e in fs::read_dir(src_dir)? {
            let e = e?;
            if !e.file_type()?.is_file() { continue; }
            let name = e.file_name().to_string_lossy().to_string();
            if name.ends_with(".sst") { acc.push((e.path(), name)); }
        }
        Ok(())
    };

    // Passo 1: SSTs (dados imutáveis)
    let mut ssts = Vec::new();
    scan_ssts(&mut ssts)?;
    for (src, name) in &ssts {
        link_or_copy(src, &dst_dir.join(name)).with_context(|| format!("SST {name}"))?;
    }

    // Passo 2: metadata por último (MANIFEST/CURRENT/OPTIONS referenciam os SSTs já linkados)
    for e in fs::read_dir(src_dir)? {
        let e = e?;
        if !e.file_type()?.is_file() { continue; }
        let name = e.file_name().to_string_lossy().to_string();
        if name == "LOCK" || name.ends_with(".sst") { continue; } // nunca linka LOCK; SSTs já foram
        link_or_copy(&e.path(), &dst_dir.join(&name)).with_context(|| format!("meta {name}"))?;
    }

    // Passo 3: re-link de SSTs criados por compaction durante a cópia (guarda anti-race no live)
    let mut ssts2 = Vec::new();
    scan_ssts(&mut ssts2)?;
    for (src, name) in &ssts2 {
        let dst = dst_dir.join(name);
        if !dst.exists() { link_or_copy(src, &dst)?; }
    }

    Ok(dst_dir.to_path_buf())
}

/// Batch hard-link para epoch dirs >= epoch_validate (Rust 🦀).
/// Substitui create_validated_checkpoint_batch (C#). Mesma assinatura/semântica.
fn create_checkpoint_batch_hardlink(
    src_root: &Path,
    dst_root: &Path,
    epoch_validate: u64,
    json: bool,
) -> anyhow::Result<PathBuf> {
    use anyhow::Context;
    use std::fs;

    let mut epochs: Vec<(u64, PathBuf)> = Vec::new();
    for e in fs::read_dir(src_root).with_context(|| format!("read {}", src_root.display()))? {
        let e = e?;
        let p = e.path();
        if !p.is_dir() { continue; }
        let name = p.file_name().unwrap_or_default().to_string_lossy().to_string();
        if let Some(rest) = name.strip_prefix("epoch") {
            if let Ok(n) = rest.parse::<u64>() {
                if n >= epoch_validate { epochs.push((n, p)); }
            }
        }
    }
    epochs.sort_by_key(|(n, _)| *n);

    if !json {
        eprintln!("      Found {} epochs to link (>= {})", epochs.len(), epoch_validate);
    }
    fs::create_dir_all(dst_root)?;

    for (i, (n, src)) in epochs.iter().enumerate() {
        let dst = dst_root.join(format!("epoch{}", n));
        if !json {
            eprintln!("      [{}/{}] epoch{}", i + 1, epochs.len(), n);
        }
        create_checkpoint_hardlink(src, &dst, json)?;
    }
    Ok(dst_root.to_path_buf())
}

/// State dirs needed for state snapshot mode.
const STATE_LINK_DIRS: &[&str] = &[
    "block/blockindex",
"tx/txindex",
"txbindex",
"chain",
"blockcommit",
"txexec",
];

/// Chama CheckpointBridge --get-state-root no checkpoint criado.
fn get_state_root_from_checkpoint(
    bridge_path: &Path,
    checkpoint_base: &Path,
    block_before: i32,
    json: bool,
) -> anyhow::Result<String> {
    use anyhow::Context;

    let store_path = checkpoint_base;

    if !json {
        eprintln!("  Getting state root from checkpoint...");
    }

    let output = std::process::Command::new(bridge_path)
    .arg("--get-state-root")
    .arg(store_path.as_os_str())
    .arg("--block-before")
    .arg(block_before.to_string())
    .output()
    .context("Failed to run CheckpointBridge --get-state-root")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: serde_json::Value = serde_json::from_str(stdout.trim())
    .context("Invalid JSON from --get-state-root")?;

    if !result["Success"].as_bool().unwrap_or(false) {
        anyhow::bail!("{}", result["Error"].as_str().unwrap_or("unknown error"));
    }

    let srh = result["StateRootHash"]
    .as_str()
    .ok_or_else(|| anyhow::anyhow!("Missing StateRootHash in response"))?
    .to_string();

    if !json {
        eprintln!("  StateRoot: {}...", &srh[..16]);
        eprintln!("  Block #{}",
                  result["BlockIndex"].as_i64().unwrap_or(-1));
    }
    Ok(srh)
}

/// Chama CheckpointBridge --gc-pipeline e parseia o resultado.
fn run_gc_pipeline(
    bridge_path: &Path,
    source_states: &Path,
    dest_states: &Path,
    root_hash_hex: &str,
    json: bool,
) -> anyhow::Result<()> {
    use anyhow::Context;
    use std::process::Stdio;

    let checkpoint_dir = source_states.parent().unwrap_or(Path::new("."));
    let export_file = checkpoint_dir.join("states_export.bin");
    let live_keys_file = checkpoint_dir.join("live_keys.bin");

    if !json {
        eprintln!("  Source: {}", source_states.display());
        eprintln!("  Dest : {}", dest_states.display());
        eprintln!("  Root : {}...", &root_hash_hex[..16]);
        eprintln!("  ⏳ Running GC Pipeline (Export Rust + BFS Rust + Prune Rust + Validate C#)...");
        eprintln!("  🚀 TUDO EM RUST MENOS VALIDAÇÃO!");
        eprintln!();
    }

    // ── Phase 1: Export (RUST) ──────────────────────────────────────
    if !json {
        eprintln!("📤 Phase 1: Exporting states/ (Rust 🦀)...");
    }

    let export_result = exporter::export_states(source_states, &export_file)?;

    if !json {
        eprintln!("  ✅ Exported {:.0} entries in {:.1}s",
                  export_result.total_entries, export_result.elapsed_secs);
    }

    // ── Phase 2: BFS (RUST) ──────────────────────────────────────
    if !json {
        eprintln!("🌳 Phase 2: BFS (Rust 🦀 - SCAN SEQUENCIAL, ~500 MB RAM)...");
    }

    let root_bytes = hex_to_hash32(root_hash_hex)?;
    let roots = vec![root_bytes];

    gc_filter::run_gc_filter(
        &export_file,
        roots,
        &live_keys_file,
    )?;

    // ── Phase 3: Prune (RUST) ──────────────────────────────────────
    if !json {
        eprintln!("🗑️ Phase 3: Prune (Rust 🦀 - RÁPIDO!)...");
    }

    // Executar prune em Rust (passa o arquivo de live keys)
    let result = pruner::prune_states(
        source_states,
        dest_states,
        &live_keys_file, // ← PASSA O ARQUIVO!
        json,
    )?;

    if !json {
        eprintln!("  ✅ Prune: {} kept, {} deleted",
                  result.nodes_copied, result.nodes_deleted);
    }

    // ── Phase 4: Validate (C#) ──────────────────────────────────────
    if !json {
        eprintln!("🔍 Phase 4: Validating pruned states/ (C#)...");
    }

    let validate_output = std::process::Command::new(bridge_path)
    .arg("--gc-validate")
    .arg(dest_states.as_os_str())
    .stderr(Stdio::inherit())
    .output()
    .context("Failed to run CheckpointBridge --gc-validate")?;

    if !validate_output.status.success() {
        anyhow::bail!("Validation failed");
    }

    // ── Limpar arquivos temporários ─────────────────────────────
    let _ = std::fs::remove_file(&export_file);
    let _ = std::fs::remove_file(&live_keys_file);

    if !json {
        eprintln!("✅ GC Pipeline complete! (Prune em Rust 🦀)");
        eprintln!("  📊 Phase 1 (Export): Rust 🦀");
        eprintln!("  📊 Phase 2 (BFS): Rust 🦀");
        eprintln!("  📊 Phase 3 (Prune): Rust 🦀");
        eprintln!("  📊 Phase 4 (Validate): C# 9C");
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
            if !live {
                if let Some(ref apv_val) = apv {
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

                let bridge_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("bridge-bin")
                .join("checkpoint")
                .join("CheckpointBridge");

                if !bridge_path.exists() {
                    eprintln!("❌ CheckpointBridge not found: {}", bridge_path.display());
                    process::exit(1);
                }

                if !json {
                    eprintln!("📸 Creating live checkpoint via hard-links (Rust 🦀)...");
                    eprintln!("   Two-pass: SSTs first, MANIFEST last (formato idêntico ao Libplanet)");
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

                                match create_checkpoint_batch_hardlink(
                                    &block_root,
                                    &block_dst_root,
                                    validate_epoch,
                                    json,
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

                                match create_checkpoint_batch_hardlink(
                                    &tx_root,
                                    &tx_dst_root,
                                    validate_epoch,
                                    json,
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

                                match create_checkpoint_batch_hardlink(
                                    &block_root,
                                    &block_dst_root,
                                    validate_epoch,
                                    json,
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

                                match create_checkpoint_batch_hardlink(
                                    &tx_root,
                                    &tx_dst_root,
                                    validate_epoch,
                                    json,
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
                    // ── LIVE: fetch_metadata DEPOIS do checkpoint (no checkpoint, sem LOCK!) ──
                    if let Some(ref apv_val) = apv {
                        if bridge_res.is_none() {
                            if !json {
                                eprintln!("🟢 Fetching metadata from checkpoint (no LOCK conflicts)...");
                            }
                            match fetch_metadata(&checkpoint_base, apv_val, block_before, &mode) {
                                Ok(res) => {
                                    if !res.success {
                                        eprintln!("❌ Bridge error: {:?}", res.error);
                                    } else {
                                        if mode_enum == SnapshotMode::Partition && epoch_limit.is_none() {
                                            epoch_limit = Some(res.current_metadata_block_epoch as u64);
                                        }
                                        // Atualiza final_output com o nome correto do bridge
                                        if mode_enum == SnapshotMode::Partition {
                                            let parent = final_output.parent().unwrap_or(Path::new(".")).to_path_buf();
                                            final_output = parent.join(format!("{}.tar.zst", res.partition_base_filename));
                                        } else {
                                            let parent = final_output.parent().unwrap_or(Path::new(".")).to_path_buf();
                                            final_output = parent.join("state_latest.tar.zst");
                                        }
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

                    if prune && (mode_enum == SnapshotMode::State || mode_enum == SnapshotMode::Full || mode_enum == SnapshotMode::Partition) {
                        if !json {
                            eprintln!("🧹 Running GC Pipeline on checkpoint states/...");
                        }

                        let state_root_hex = match get_state_root_from_checkpoint(
                            &bridge_path,
                            &checkpoint_base,
                            block_before,
                            json,
                        ) {
                            Ok(srh) => srh,
                            Err(e) => {
                                eprintln!("⚠ Could not read state root: {} — skipping prune", e);
                                String::new()
                            }
                        };

                        if !state_root_hex.is_empty() {
                            let states_src = checkpoint_base.join("states");
                            let states_gc = checkpoint_base.join("states_gc");

                            match run_gc_pipeline(
                                &bridge_path,
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
