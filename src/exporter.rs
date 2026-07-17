//! Phase 1: Export all KV pairs from states/ RocksDB to binary file.
//! Format: [key:32b][val_len:4b little-endian][val:Nb] repeated
//!
//! Otimizações aplicadas:
//! - raw_iterator_opt: key()/value() devolvem &[u8] emprestados (zero alloc por entry)
//! - fill_cache(false): scan one-pass, não polui block cache
//! - set_advise_random_on_open(false): reabilita readahead do SO para scan sequencial
//! - Logging com contador a cada 1M entries (evita clock_gettime por iteração)
//! - Escrita direta no BufWriter sem buffer intermediário (zero alloc extra)
//! - BufWriter 128MB para batching de escrita

use anyhow::{Context, Result};
use rocksdb::{DB, Options, ReadOptions};
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

    eprintln!("📤 Exporting states/ 🦀 ...");
    eprintln!("   Source: {}", states_path.display());
    eprintln!("   Dest:   {}", output_file.display());

    // ── Abrir RocksDB em modo read-only ──────────────────────────────
    let mut opts = Options::default();
    opts.create_if_missing(false);
    // Scan 100% sequencial: reabilita readahead do kernel
    // (default true = FADV_RANDOM, desliga readahead do SO)
    opts.set_advise_random_on_open(false);

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

    let mut count = 0u64;
    let mut last_log = Instant::now();

    // ── Raw iterator: ZERO alloc por entry ───────────────────────────
    // iterator_opt() devolve (Box<[u8]>, Box<[u8]>) = 2 allocs + 2 frees por entry.
    // raw_iterator_opt() devolve key()/value() como &[u8] emprestados do buffer interno.
    //
    // Nota: set_use_direct_reads não existe nas rust-rocksdb 0.24 bindings
    // (a C API rocksdb_readoptions_set_use_direct_reads não foi envolvida).
    let mut read_opts = ReadOptions::default();
    read_opts.set_readahead_size(32 * 1024 * 1024); // 32MB prefetch
    read_opts.set_verify_checksums(false);          // C# --gc-validate revalida depois
    read_opts.fill_cache(false);                     // One-pass: não poluir block cache

    let mut iter = db.raw_iterator_opt(read_opts);
    iter.seek_to_first();

    while iter.valid() {
        let key = iter.key().expect("valid() true implies key exists");
        let value = iter.value().expect("valid() true implies value exists");

        if key.len() == 32 {
            // ── ESCREVER DIRETO NO BUFWRITER (ZERO ALLOC EXTRA) ──
            writer.write_all(key)
            .with_context(|| format!("Failed to write entry {}", count))?;
            writer.write_all(&(value.len() as u32).to_le_bytes())?;
            writer.write_all(value)?;
            count += 1;

            // Log a cada ~1M entries com bitwise AND (mais rápido que módulo)
            // Só lê o relógio a cada 1M entries (evita clock_gettime por iteração)
            if count & 0xFFFFF == 0 {
                if last_log.elapsed().as_secs() >= 5 {
                    let elapsed = start.elapsed().as_secs_f64();
                    let rate = count as f64 / elapsed;
                    eprintln!("   Exported {}M entries ({:.1}K/s)...",
                              count / 1_000_000,
                              rate / 1000.0
                    );
                    last_log = Instant::now();
                }
            }
        }

        iter.next();
    }

    // Verificar se o iterador encontrou algum erro (checksum, I/O, etc.)
    iter.status().map_err(anyhow::Error::msg)
    .context("Iterator failed mid-scan")?;

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
