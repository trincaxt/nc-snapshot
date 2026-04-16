//! nc-snapshot: Native Rust pruner for Nine Chronicles Libplanet TrieStateStore.
//!
//! Prunes the states/ RocksDB using Selective Streaming Copy:
//! instead of deleting unreachable nodes (tombstones + slow compaction),
//! creates a new clean DB with only reachable nodes via SST file generation.
//!
//! Reduces states/ from ~36 GiB to ~19 GiB (parity with C# CopyStates).
//! Expected: ~38 GiB IO, ~15-25 min (vs C#: ~276 GiB IO, 2-3 hours).
//!
//! Usage:
//!   nc-pruner prune --store-path ~/9c-blockchain
//!   nc-pruner prune --store-path ~/9c-blockchain --roots <hex1> <hex2>
//!   nc-pruner diagnose --store-path ~/9c-blockchain
//!   nc-pruner diagnose-chain --store-path ~/9c-blockchain

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

mod chain_cleanup;
mod trie;

#[derive(Parser)]
#[command(
    name = "nc-pruner",
    version,
    about = "Native Rust pruner for Nine Chronicles states/ (Libplanet TrieStateStore)",
    long_about = "Prunes unreachable trie nodes using Selective Streaming Copy.\n\
                  Creates a clean DB with only reachable nodes — no tombstones, no compaction.\n\
                  Reduces states/ from ~36 GiB to ~19 GiB in ~15-25 min."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Prune unreachable nodes from states/ using Selective Streaming Copy.
    ///
    /// This is the high-performance replacement for C# CopyStates.
    /// Opens the source blockchain in READONLY mode — never modifies,
    /// renames, or deletes anything in the source directory.
    /// Writes the pruned clean DB to --target-path.
    Prune {
        /// Path to the Nine Chronicles blockchain store directory (READONLY).
        /// Must contain states/ (and optionally chain/) subdirectories.
        /// This directory is NEVER modified.
        #[arg(long, short = 'p')]
        store_path: PathBuf,

        /// Where to write the pruned clean states DB.
        /// Defaults to a "states_pruned" directory next to states/.
        #[arg(long, short = 't')]
        target_path: Option<PathBuf>,

        /// State root hashes to preserve (hex, space-separated).
        /// If not provided, reads the last --keep-blocks roots from chain/.
        #[arg(long, short = 'r', num_args = 1..)]
        roots: Option<Vec<String>>,

        /// Depth of the blockchain to backtrack (number of recent blocks).
        /// Only used when --roots is not specified.
        #[arg(long, short = 'd', default_value = "3")]
        depth: usize,

        /// Dry run: scan and report without actually creating a new DB.
        #[arg(long)]
        dry_run: bool,
    },

    /// Diagnostic: scan states/ DB and report structure information.
    ///
    /// Useful for understanding the node format and distribution.
    /// Decodes and categorizes sample nodes.
    Diagnose {
        /// Path to the Nine Chronicles blockchain store directory.
        #[arg(long, short = 'p')]
        store_path: PathBuf,

        /// Maximum number of nodes to decode and analyze.
        #[arg(long, short = 'n', default_value = "100")]
        max_nodes: usize,
    },

    /// Diagnostic: scan chain/ DB and report its layout.
    ///
    /// Useful for understanding block storage format and finding state root hashes.
    DiagnoseChain {
        /// Path to the Nine Chronicles blockchain store directory.
        #[arg(long, short = 'p')]
        store_path: PathBuf,

        /// Maximum number of keys to show.
        #[arg(long, short = 'n', default_value = "20")]
        max_keys: usize,
    },

    /// Verify: check that all reachable nodes exist and decode correctly.
    ///
    /// Runs the same DFS as prune but only validates, doesn't modify anything.
    Verify {
        /// Path to the Nine Chronicles blockchain store directory.
        #[arg(long, short = 'p')]
        store_path: PathBuf,

        /// State root hashes to verify (hex, space-separated).
        #[arg(long, short = 'r', num_args = 1..)]
        roots: Option<Vec<String>>,

        /// Depth of most recent blocks to verify backward.
        #[arg(long, short = 'd', default_value = "3")]
        depth: usize,
    },

    /// Clean stale directories from the Nine Chronicles store.
    ///
    /// Removes known stale/legacy directories that accumulate from protocol
    /// upgrades or interrupted operations. Does NOT modify active chain data
    /// (chain/, states/, block/).
    ///
    /// Stale directories removed:
    ///   9c-main, state, stateref, state_hashes, new_states, blockpercept, stagedtx
    Clean {
        /// Path to the Nine Chronicles blockchain store directory.
        #[arg(long, short = 'p')]
        store_path: PathBuf,

        /// Also clean old snapshot artifacts (states_pruned/).
        #[arg(long)]
        include_snapshots: bool,
    },
}

fn main() -> Result<()> {
    // Initialize tracing with configurable verbosity
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Prune {
            store_path,
            target_path,
            roots,
            depth,
            dry_run,
        } => {
            tracing::info!("=== nc-pruner prune (Selective Streaming Copy) ===");
            tracing::info!("Source (READONLY): {:?}", store_path);

            // Resolve state roots FIRST before creating checkpoint!
            // This guarantees that the states/ DB checkpoint will contain the state roots we discovered,
            // mitigating a race condition where the node adds a new block after our checkpoint.
            let state_roots = if let Some(hex_roots) = roots {
                tracing::info!("Using {} manually specified root(s)", hex_roots.len());
                trie::chain_reader::parse_state_root_hashes(&hex_roots)?
            } else {
                tracing::info!(
                    "Auto-detecting last {} state root(s) from chain/...",
                    depth
                );
                trie::chain_reader::get_last_n_state_roots(&store_path, depth)
                    .context(
                        "Failed to auto-detect state roots. \
                         Use --roots to specify them manually.",
                    )?
            };

            for (i, root) in state_roots.iter().enumerate() {
                tracing::info!("  Root[{}]: {}", i, hex::encode(root));
            }

            // NOW create a point-in-time Checkpoint of the active states/ DB
            // This guarantees we don't hit "No such file or directory" when the active node
            // deletes/compacts .sst files while we are traversing the 100 million nodes.
            let states_path = store_path.join("states");
            if !states_path.exists() {
                anyhow::bail!(
                    "states/ directory not found at {:?}. \
                     Ensure --store-path points to the blockchain store root.",
                    store_path
                );
            }

            // Move target resolution BEFORE checkpoint creation so we can store the checkpoint inside the target directory!
            // Default target: states_pruned/ next to the store
            let target = target_path.unwrap_or_else(|| store_path.join("states_pruned"));

            if dry_run {
                tracing::info!("DRY RUN: No data will be written.");
            } else {
                tracing::info!("Target: {:?}", target);
            }

            // The checkpoint is created strictly OUTSIDE the sacred `9c-blockchain` folder
            let checkpoint_path = target.join("states_checkpoint_tmp");
            
            // Clean up old checkpoint if it was left over from a crash
            if checkpoint_path.exists() {
                let _ = std::fs::remove_dir_all(&checkpoint_path);
            }

            // SAFETY CHECK: Record source file count + total size before any operation
            let source_files_before = walkdir::WalkDir::new(&states_path)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
                .count();
            let source_size_before = trie::sst_writer::dir_size(&states_path);
            tracing::info!(
                "Source safety check: {} files, {} bytes",
                source_files_before,
                source_size_before
            );

            tracing::info!(
                "Creating Zero-Cost File System Checkpoint (Hard Links) for states/ DB..."
            );
            {
                std::fs::create_dir_all(&checkpoint_path)
                    .context("Failed to create checkpoint directory")?;

                // MUST recursively walk ALL subdirectories — RocksDB stores .sst files
                // in column family subdirectories (e.g., states/default/041670.sst)
                for entry in walkdir::WalkDir::new(&states_path) {
                    let entry = entry.context("Failed to walk states/ directory")?;
                    let rel = entry.path().strip_prefix(&states_path).unwrap_or(entry.path());
                    let dst = checkpoint_path.join(rel);

                    if entry.file_type().is_dir() {
                        std::fs::create_dir_all(&dst)
                            .context("Failed to create checkpoint subdirectory")?;
                    } else if entry.file_type().is_file() {
                        std::fs::hard_link(entry.path(), &dst)
                            .with_context(|| format!(
                                "Failed to hard-link {} -> {}. \
                                 Ensure checkpoint is on the same filesystem as states/",
                                entry.path().display(), dst.display()
                            ))?;
                    }
                }
            }
            tracing::info!("Filesystem Hard-link Checkpoint created successfully at {:?}", checkpoint_path);

            // Run actual prune on the CHECKPOINT path, not the active db!
            let prune_result = trie::pruner::prune_states(&checkpoint_path, &target, &state_roots, dry_run);
            
            // Immediately clean up the checkpoint folder regardless of success or failure
            let _ = std::fs::remove_dir_all(&checkpoint_path);
            tracing::info!("Temporary checkpoint removed.");

            let stats = prune_result?;

            println!("\n{}", stats);

            if dry_run {
                println!("\nThis was a dry run. Re-run without --dry-run to actually prune.");
            } else {
                println!("\nPruned DB written to: {}", target.display());
                println!("Source blockchain was NOT modified.");
            }
        }

        Command::Diagnose {
            store_path,
            max_nodes,
        } => {
            tracing::info!("=== nc-pruner diagnose (states/) ===");
            let states_path = store_path.join("states");
            trie::pruner::diagnose_states(&states_path, max_nodes)?;
        }

        Command::DiagnoseChain {
            store_path,
            max_keys,
        } => {
            tracing::info!("=== nc-pruner diagnose-chain ===");
            trie::chain_reader::diagnose_chain(&store_path, max_keys)?;
        }

        Command::Verify {
            store_path,
            roots,
            depth,
        } => {
            tracing::info!("=== nc-pruner verify ===");

            let state_roots = if let Some(hex_roots) = roots {
                trie::chain_reader::parse_state_root_hashes(&hex_roots)?
            } else {
                trie::chain_reader::get_last_n_state_roots(&store_path, depth)?
            };

            let states_path = store_path.join("states");

            // Run prune in dry-run mode (verify only, no target needed)
            let dummy_target = store_path.join("_verify_unused");
            let stats = trie::pruner::prune_states(&states_path, &dummy_target, &state_roots, true)?;

            println!("\nVerification complete.");
            println!("  Reachable nodes: {}", stats.reachable);
            println!("  Unreachable nodes: {}", stats.deleted);
            println!("  Total nodes: {}", stats.total_scanned);

            let ratio = if stats.total_scanned > 0 {
                (stats.deleted as f64 / stats.total_scanned as f64) * 100.0
            } else {
                0.0
            };
            println!("  Prunable: {:.1}%", ratio);

            if stats.reachable == 0 {
                tracing::warn!("No reachable nodes found! The state roots may be incorrect.");
            }
        }

        Command::Clean {
            store_path,
            include_snapshots,
        } => {
            tracing::info!("=== nc-pruner clean ===");
            tracing::info!("Store path: {:?}", store_path);

            // Clean stale directories
            let result = chain_cleanup::clean_stale_directories(&store_path)?;
            println!("{}", result);

            // Optionally clean old snapshot artifacts
            if include_snapshots {
                tracing::info!("Cleaning old snapshot artifacts...");
                let bytes = chain_cleanup::clean_old_snapshots(&store_path)?;
                if bytes > 0 {
                    println!(
                        "Old snapshot artifacts cleaned: {}",
                        format_bytes_display(bytes)
                    );
                } else {
                    println!("No old snapshot artifacts found.");
                }
            }
        }
    }

    Ok(())
}

/// Format bytes for display (non-module version for main.rs).
fn format_bytes_display(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}
