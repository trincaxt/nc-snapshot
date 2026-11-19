//! GC Pipeline orchestration — Export + BFS + Prune + Validate.
//!
//! Orquestra as 4 fases do State Trie Garbage Collection:
//!   1. Export states/ → arquivo binário sequencial (+ índice hash→offset)
//!   2. BFS indexado por nível pra achar todos os nós vivos (bitset)
//!   3. Prune: copia só nós vivos pra um novo DB
//!   4. Validate: abre o DB podado pra ver se é válido

use anyhow::Context;
use std::path::Path;
use std::time::Instant;
use crate::io_util::hex_to_hash32;
use crate::{chain_reader, exporter, gc_index, pruner};

/// Executa o GC Pipeline completo.
pub fn run_gc_pipeline(
    source_states: &Path,
    dest_states: &Path,
    root_hash_hex: &str,
    temp_dir: Option<&Path>,
    json: bool,
) -> anyhow::Result<()> {
    let pipeline_start = Instant::now();

    let default_parent = source_states.parent().unwrap_or(Path::new("."));
    let scratch_dir = temp_dir.unwrap_or(default_parent);
    std::fs::create_dir_all(scratch_dir)?;

    let export_file = scratch_dir.join("states_export.bin");
    let index_file = gc_index::index_path_for(&export_file);
    let live_bits_file = scratch_dir.join("live_bits.bin");

    if !json {
        eprintln!("  Source: {}", source_states.display());
        eprintln!("  Dest : {}", dest_states.display());
        if temp_dir.is_some() {
            eprintln!("  Scratch: {}", scratch_dir.display());
        }
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
        eprintln!("🌳 Phase 2: BFS ( 🦀 - BFS INDEXADO POR NÍVEL - 🦀 )...");
    }

    let root_bytes = hex_to_hash32(root_hash_hex)?;
    let roots = vec![root_bytes];

    gc_index::run_gc_filter_graph(&export_file, &index_file, roots, &live_bits_file)?;
    let phase2_elapsed = phase2_start.elapsed().as_secs_f64();

    // ── Phase 3: Prune ──────────────────────────────────────────
    let phase3_start = Instant::now();
    if !json {
        eprintln!("🗑️ Phase 3: Prune  🦀 ...");
    }

    let result = pruner::prune_states(&export_file, dest_states, &live_bits_file, json)?;
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
    let _ = std::fs::remove_file(&index_file);
    let _ = std::fs::remove_file(&live_bits_file);

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

/// Executa o GC Pipeline por Varredura Direta (3-scan, sem arquivo de export).
pub fn run_scan_gc_pipeline(
    source_states: &Path,
    dest_states: &Path,
    root_hash_hex: &str,
    json: bool,
) -> anyhow::Result<()> {
    let pipeline_start = Instant::now();

    if !json {
        eprintln!("  Source: {}", source_states.display());
        eprintln!("  Dest : {}", dest_states.display());
        eprintln!("  Root : {}...", &root_hash_hex[..16]);
        eprintln!("  ⏳ Running 3-Scan GC Pipeline directly on RocksDB (no export file)...");
        eprintln!();
    }

    let root_bytes = hex_to_hash32(root_hash_hex)?;
    let roots = vec![root_bytes];

    let src = crate::gc_scan_rocks::RocksSource::open(source_states)?;
    let mut sink = crate::gc_scan_rocks::RocksSink::create(dest_states)?;

    let stats = crate::gc_scan::run_scan_gc(&src, &roots, &mut sink)?;

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

    if !json {
        let total_elapsed = pipeline_start.elapsed().as_secs_f64();
        let pct_removed = if stats.total > 0 {
            stats.deleted as f64 / stats.total as f64 * 100.0
        } else {
            0.0
        };

        eprintln!("✅ 3-Scan GC Pipeline complete! 🦀");
        eprintln!("  📊 Scan 1 (Keys Index): {:.1}s", stats.secs_index);
        eprintln!("  📊 Scan 2 (Graph CSR):  {:.1}s", stats.secs_graph);
        eprintln!("  📊 In-Memory BFS:       {:.1}s", stats.secs_bfs);
        eprintln!("  📊 Scan 3 (Prune):      {:.1}s", stats.secs_prune);
        eprintln!("  📊 Phase 4 (Validate):  {:.1}s", phase4_elapsed);
        eprintln!("  ⏱️ Total time:           {:.1} min", total_elapsed / 60.0);
        eprintln!(
            "  💾 Nodes: {} → {} ({} deleted, {:.1}% removed)",
            stats.total,
            stats.live,
            stats.deleted,
            pct_removed
        );
    }

    Ok(())
}
