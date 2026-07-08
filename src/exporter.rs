//! Phase 1: Export all KV pairs from states/ RocksDB to binary file.
//! Format: [key:32b][val_len:4b little-endian][val:Nb] repeated
//!
//! Estratégia: Usar rocksdb::DB::iterator() para scan sequencial
//! com buffer de 128MB e escrita em chunk único.

use anyhow::{Context, Result};
use rocksdb::{DB, IteratorMode, Options};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

pub struct ExportResult {
    pub total_entries: u64,
    pub file_size_gb: f64,
    pub elapsed_secs: f64,
}

/// Exporta todas as keys/values do states/ para um arquivo binário.
///
/// Formato do arquivo:
/// [key:32b][val_len:4b little-endian][val:Nb] repeated
pub fn export_states(states_path: &Path, output_file: &Path) -> Result<ExportResult> {
    let start = Instant::now();

    eprintln!("📤 Exporting states/ (Rust 🦀) ...");
    eprintln!("   Source: {}", states_path.display());
    eprintln!("   Dest:   {}", output_file.display());

    // ── Abrir RocksDB em modo read-only ──────────────────────────────
    let mut opts = Options::default();
    opts.create_if_missing(false);

    // Usar format_version 5 para compatibilidade com Libplanet
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_format_version(5);
    opts.set_block_based_table_factory(&block_opts);

    let db = DB::open_for_read_only(&opts, states_path, false)
    .with_context(|| format!("Failed to open states/ at: {}", states_path.display()))?;

    // ── Criar arquivo de saída com buffer de 128MB ──────────────────
    let file = File::create(output_file)
    .with_context(|| format!("Failed to create export file: {}", output_file.display()))?;
    let mut writer = BufWriter::with_capacity(128 * 1024 * 1024, file);

    // ── BUFFER REUTILIZÁVEL PARA UMA ÚNICA ESCRITA ──────────────────
    let mut buf = Vec::with_capacity(32 + 4 + 1024); // 32 bytes key + 4 bytes len + 1KB value

    let mut count = 0u64;
    let mut last_log = Instant::now();

    // ── Scan sequencial com Iterator ────────────────────────────────
    let iter = db.iterator(IteratorMode::Start);

    for item in iter {
        let (key, value) = item
        .with_context(|| format!("Failed to read entry at offset {}", count))?;

        if key.len() != 32 {
            continue; // Pular keys que não são hashes (metadados)
        }

        // ── ESCREVER TUDO DE UMA VEZ (ZERO SYSCALLS DESNECESSÁRIOS) ──
        buf.clear();
        buf.extend_from_slice(&key);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&value);
        writer.write_all(&buf)
        .with_context(|| format!("Failed to write entry {}", count))?;

        count += 1;

        // Log a cada 10M entries ou a cada 5 segundos
        if count % 10_000_000 == 0 || last_log.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed().as_secs_f64();
            let rate = count as f64 / elapsed;
            eprintln!("   Exported {}M entries ({:.1}K/s)...",
                      count / 1_000_000,
                      rate / 1000.0
            );
            last_log = Instant::now();
        }
    }

    writer.flush()?;

    let elapsed = start.elapsed().as_secs_f64();
    let file_size = std::fs::metadata(output_file)
    .map(|m| m.len() as f64 / 1_000_000_000.0)
    .unwrap_or(0.0);

    eprintln!("   ✅ Export complete: {:.0} entries, {:.1} GB in {:.1}s",
              count, file_size, elapsed);

    Ok(ExportResult {
        total_entries: count,
       file_size_gb: file_size,
       elapsed_secs: elapsed,
    })
}
