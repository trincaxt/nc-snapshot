//! GC do states/ via mark-and-sweep sobre o MerkleTrie do Libplanet.
//!
//! Estratégia: Recebe live_keys do BFS, copia apenas keys vivas para o novo DB.
//! Compatível com format_version 5 (Libplanet C#).

use anyhow::Context;
use rocksdb::{DB, Options, ReadOptions, WriteBatch, BlockBasedOptions};
use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

const HASH_LEN: usize = 32;
const BATCH_SIZE: usize = 250_000;

pub struct PruneResult {
    pub nodes_copied: u64,
    pub nodes_deleted: u64,
    pub elapsed_secs: f64,
}

/// Abre source como read-only (scan sequencial)
fn open_source(path: &Path) -> anyhow::Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    // Scan sequencial: reabilita readahead do kernel
    opts.set_advise_random_on_open(false);

    DB::open_for_read_only(&opts, path, false)
    .with_context(|| format!("Source states/ não abre: {}", path.display()))
}

/// Cria dest com format_version 5 (compatível com Libplanet)
fn open_dest(path: &Path) -> anyhow::Result<DB> {
    if path.exists() {
        std::fs::remove_dir_all(path)
        .with_context(|| format!("Falha ao limpar states_gc/: {}", path.display()))?;
    }
    std::fs::create_dir_all(path)
    .with_context(|| format!("Falha ao criar states_gc/: {}", path.display()))?;

    let mut opts = Options::default();
    opts.create_if_missing(true);

    // ── Compatibilidade com Libplanet C# ──────────────────────────
    opts.set_soft_pending_compaction_bytes_limit(1_000_000_000_000);
    opts.set_hard_pending_compaction_bytes_limit(1_038_176_821_042);

    // ── FORMAT_VERSION 5 (CRÍTICO!) ──────────────────────────────
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_format_version(5);  // ← MUDADO PARA 5!
    opts.set_block_based_table_factory(&block_opts);

    // Bulk write tuning
    opts.set_write_buffer_size(256 * 1024 * 1024);
    opts.set_max_write_buffer_number(6);
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_level_zero_file_num_compaction_trigger(8);
    opts.set_level_zero_slowdown_writes_trigger(20);
    opts.set_level_zero_stop_writes_trigger(36);
    opts.set_target_file_size_base(256 * 1024 * 1024);
    opts.set_max_open_files(-1);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_max_background_jobs(8);

    DB::open(&opts, path)
    .with_context(|| format!("Falha ao criar states_gc/: {}", path.display()))
}

/// Carrega live keys do arquivo binário
fn load_live_keys(live_keys_file: &Path) -> anyhow::Result<HashSet<[u8; HASH_LEN]>> {
    use std::io::Read;

    let mut file = std::fs::File::open(live_keys_file)
    .with_context(|| format!("Failed to open live_keys file: {}", live_keys_file.display()))?;

    let file_size = file.metadata()?.len();
    let num_keys = file_size / 32;

    eprintln!("   📂 Loading {} live keys from {}", num_keys, live_keys_file.display());

    let mut keys = HashSet::with_capacity(num_keys as usize);
    let mut buf = [0u8; HASH_LEN];
    let mut count = 0;

    while let Ok(()) = file.read_exact(&mut buf) {
        keys.insert(buf);
        count += 1;
        if count & 0x7FFFFF == 0 {
            eprintln!("   📂 Loaded {}M live keys...", count >> 20);
        }
    }

    eprintln!("   📂 Loaded {} live keys", keys.len());
    Ok(keys)
}

/// Executa o PRUNE: copia apenas keys vivas para o novo DB
pub fn prune_states(
    source_path: &Path,
    dest_path: &Path,
    live_keys_file: &Path,
    json: bool,
) -> anyhow::Result<PruneResult> {
    let start = Instant::now();

    if !json {
        eprintln!("🧹 Prune states/ iniciado (🦀)");
        eprintln!("   Source  : {}", source_path.display());
        eprintln!("   Dest    : {}", dest_path.display());
        eprintln!("   Live keys: {}", live_keys_file.display());
    }

    // ── Carregar live keys ──────────────────────────────────────────
    let live_keys = load_live_keys(live_keys_file)?;

    if !json {
        eprintln!("   ✅ Live keys loaded: {}", live_keys.len());
    }

    // ── Abrir source e dest ─────────────────────────────────────────
    let source = open_source(source_path)?;
    let dest = open_dest(dest_path)?;

    // ── Iterar source (raw_iterator: zero alloc por entry) ──────────
    let mut batch = WriteBatch::default();
    let mut nodes_copied: u64 = 0;
    let mut nodes_deleted: u64 = 0;
    let mut total_scanned: u64 = 0;

    let mut read_opts = ReadOptions::default();
    read_opts.fill_cache(false); // One-pass: não poluir block cache

    let mut iter = source.raw_iterator_opt(read_opts);
    iter.seek_to_first();

    while iter.valid() {
        let key = iter.key().expect("valid() true implies key exists");
        let value = iter.value().expect("valid() true implies value exists");
        total_scanned += 1;

        if key.len() == HASH_LEN {
            let mut key_arr = [0u8; HASH_LEN];
            key_arr.copy_from_slice(key);

            if live_keys.contains(&key_arr) {
                batch.put(key, value);
                nodes_copied += 1;
            } else {
                nodes_deleted += 1;
            }
        } else {
            // Keys menores que 32 bytes (metadados do RocksDB) - copiar sempre
            batch.put(key, value);
        }

        if batch.len() >= BATCH_SIZE {
            dest.write(batch).context("Writing batch")?;
            batch = WriteBatch::default();

            if !json && nodes_copied & 0x7FFFF == 0 {
                eprintln!(
                    "   📊 Copied {}M nodes, deleted {}M garbage...",
                    nodes_copied >> 20,
                    nodes_deleted >> 20
                );
            }
        }

        iter.next();
    }

    // Verificar se o iterador encontrou erros
    iter.status().map_err(anyhow::Error::msg)
    .context("Prune iterator failed")?;

    if !batch.is_empty() {
        dest.write(batch).context("Flush final")?;
    }

    let elapsed = start.elapsed().as_secs_f64();

    if !json {
        eprintln!(
            "✅ Prune complete: {} kept, {} deleted, {} scanned | {:.1}s ({:.1} min)",
                  nodes_copied,
                  nodes_deleted,
                  total_scanned,
                  elapsed,
                  elapsed / 60.0
        );
    }

    Ok(PruneResult {
        nodes_copied,
       nodes_deleted,
       elapsed_secs: elapsed,
    })
}
