//! GC do states/ via mark-and-sweep sobre o MerkleTrie do Libplanet.
//!
//! Estratégia: Recebe live_keys do BFS, copia apenas keys vivas para o novo DB.
//! Compatível com format_version 5 (Libplanet C#).

use anyhow::Context;
use rocksdb::{DB, Options, WriteBatch, BlockBasedOptions};
use rustc_hash::FxHashSet;
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

const HASH_LEN: usize = 32;
const BATCH_SIZE: usize = 250_000;

/// Lê um registro em `offset`: retorna (key, value, next_offset).
/// Formato: key(32) + value_len(4 LE) + value(value_len).
/// Retorna None se o record não couber completamente no arquivo.
#[inline]
fn read_record(data: &[u8], offset: usize) -> Option<([u8; HASH_LEN], &[u8], usize)> {
    if offset + 36 > data.len() {
        return None;
    }
    
    let key: [u8; HASH_LEN] = data[offset..offset + HASH_LEN].try_into().unwrap();
    let lo = offset + HASH_LEN;
    let vlen = u32::from_le_bytes([data[lo], data[lo + 1], data[lo + 2], data[lo + 3]]) as usize;
    let vo = lo + 4;
    
    if vo + vlen > data.len() {
        return None;
    }
    
    Some((key, &data[vo..vo + vlen], vo + vlen))
}

pub struct PruneResult {
    pub nodes_copied: u64,
    pub nodes_deleted: u64,
    #[allow(dead_code)]
    pub elapsed_secs: f64,
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

    // ── BULK LOAD MODE: manual (compatível com Libplanet) ───────
    // Desliga auto-compaction durante write, compacta no final
    opts.set_disable_auto_compactions(true);

    DB::open(&opts, path)
    .with_context(|| format!("Falha ao criar states_gc/: {}", path.display()))
}

/// Carrega live keys do arquivo binário
fn load_live_keys(live_keys_file: &Path) -> anyhow::Result<FxHashSet<[u8; HASH_LEN]>> {
    use std::io::Read;

    let mut file = std::fs::File::open(live_keys_file)
    .with_context(|| format!("Failed to open live_keys file: {}", live_keys_file.display()))?;

    let file_size = file.metadata()?.len();
    let num_keys = file_size / 32;

    eprintln!("   📂 Loading {} live keys from {}", num_keys, live_keys_file.display());

    let mut keys = FxHashSet::with_capacity_and_hasher(num_keys as usize, Default::default());
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
    export_file: &Path,
    dest_path: &Path,
    live_keys_file: &Path,
    json: bool,
) -> anyhow::Result<PruneResult> {
    let start = Instant::now();

    if !json {
        eprintln!("🧹 Prune states/ iniciado (🦀)");
        eprintln!("   Export  : {}", export_file.display());
        eprintln!("   Dest    : {}", dest_path.display());
        eprintln!("   Live keys: {}", live_keys_file.display());
    }

    // ── Carregar live keys ──────────────────────────────────────────
    let live_keys = load_live_keys(live_keys_file)?;

    if !json {
        eprintln!("   ✅ Live keys loaded: {}", live_keys.len());
    }

    // ── Abrir export file (mmap) e dest DB ──────────────────────────
    if !json {
        eprintln!("   📂 Opening export file...");
    }
    let file = File::open(export_file)
        .with_context(|| format!("Failed to open export file: {}", export_file.display()))?;
    let mmap = unsafe { Mmap::map(&file) }
        .with_context(|| format!("Failed to mmap export file: {}", export_file.display()))?;
    let data: &[u8] = &mmap[..];
    let data_len = data.len();

    let dest = open_dest(dest_path)?;

    // ── Iterar export.bin (scan sequencial mmap) ─────────────────────
    let mut batch = WriteBatch::default();
    let mut nodes_copied: u64 = 0;
    let mut nodes_deleted: u64 = 0;
    let mut total_scanned: u64 = 0;

    let mut offset = 0;

    while offset < data_len {
        let Some((key, value, next)) = read_record(data, offset) else {
            break;
        };
        offset = next;
        total_scanned += 1;

        if live_keys.contains(&key) {
            batch.put(&key, value);
            nodes_copied += 1;
        } else {
            nodes_deleted += 1;
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
    }

    if !batch.is_empty() {
        dest.write(batch).context("Flush final")?;
    }

    // ── BULK LOAD MODE: compactar tudo uma vez no final ─────────
    if !json {
        eprintln!("   🔧 Compacting destination DB...");
    }
    let compact_start = std::time::Instant::now();
    dest.compact_range::<&[u8], &[u8]>(None, None);
    if !json {
        eprintln!("   ✅ Compaction complete: {:.1}s", compact_start.elapsed().as_secs_f64());
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
