//! GC Pipeline orchestration — Export + BFS + Prune + Validate.
//!
//! Orquestra as 4 fases do State Trie Garbage Collection:
//!   1. Export states/ → arquivo binário sequencial
//!   2. Fixpoint BFS pra achar todos os nós vivos
//!   3. Prune: copia só nós vivos pra um novo DB
//!   4. Validate: abre o DB podado pra ver se é válido

use anyhow::Context;
use std::path::Path;
use std::time::Instant;
use crate::io_util::hex_to_hash32;
use crate::{chain_reader, exporter, gc_filter, pruner};

/// Executa o GC Pipeline completo.
pub fn run_gc_pipeline(
    source_states: &Path,
    dest_states: &Path,
    root_hash_hex: &str,
    json: bool,
) -> anyhow::Result<()> {
    let pipeline_start = Instant::now();

    let checkpoint_dir = source_states.parent().unwrap_or(Path::new("."));
    let export_file = checkpoint_dir.join("states_export.bin");
    let live_keys_file = checkpoint_dir.join("live_keys.bin");

    if !json {
        eprintln!("  Source: {}", source_states.display());
        eprintln!("  Dest : {}", dest_states.display());
        eprintln!("  Root : {}...", &root_hash_hex[..16]);
        eprintln!("  ⏳ Running GC Pipeline (Export + BFS + Prune + Validate)...");
        eprintln!();
    }

    // ── Phase 1: Export ──────────────────────────────────────────
    let phase1_start = Instant::now();
    if !json {
        eprintln!("📤 Phase 1: Exporting states/ 🦀...");
    }

    let export_result = exporter::export_states(source_states, &export_file)?;
    let phase1_elapsed = phase1_start.elapsed().as_secs_f64();

    if !json {
        eprintln!(
            "  ✅ Exported {:.0} entries in {:.1}s  |  {:.1} min",
            export_result.total_entries,
            export_result.elapsed_secs,
            phase1_elapsed / 60.0
        );
    }

    // ── Phase 2: BFS ────────────────────────────────────────────
    let phase2_start = Instant::now();
    if !json {
        eprintln!("🌳 Phase 2: BFS ( 🦀 - SCAN SEQUENCIAL - 🦀 )...");
    }

    let root_bytes = hex_to_hash32(root_hash_hex)?;
    let roots = vec![root_bytes];

    gc_filter::run_gc_filter(&export_file, roots, &live_keys_file)?;
    let phase2_elapsed = phase2_start.elapsed().as_secs_f64();

    // ── Phase 3: Prune ──────────────────────────────────────────
    let phase3_start = Instant::now();
    if !json {
        eprintln!("🗑️ Phase 3: Prune  🦀 ...");
    }

    let result = pruner::prune_states(&export_file, dest_states, &live_keys_file, json)?;
    let phase3_elapsed = phase3_start.elapsed().as_secs_f64();

    if !json {
        eprintln!(
            "  ✅ Prune: {} kept, {} deleted  |  {:.1} min",
            result.nodes_copied,
            result.nodes_deleted,
            phase3_elapsed / 60.0
        );
    }

    // ── Phase 4: Validate ───────────────────────────────────────
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

    // ── Cleanup ─────────────────────────────────────────────────
    let _ = std::fs::remove_file(&export_file);
    let _ = std::fs::remove_file(&live_keys_file);

    if !json {
        let _total_elapsed = pipeline_start.elapsed().as_secs_f64();
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
        eprintln!(
            "  💾 Nodes: {} → {} ({} deleted, {:.1}% removed)",
            total_nodes,
            result.nodes_copied,
            result.nodes_deleted,
            pct_removed
        );
    }

    Ok(())
}
