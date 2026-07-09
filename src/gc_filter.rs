//! Filtro BFS para GC de states/ — VERSÃO FIXPOINT (multi-passe, low-RAM)
//!
//! Estratégia:
//! 1. Working-set único mutável (pending). Filhos entram no MESMO passe.
//! 2. Scan sequencial completo do mmap; filhos à frente do pai são pegos de graça.
//! 3. Repete passes até um passe não achar nada novo (fixpoint).
//!
//! RAM: só visited + pending (sem índice key→offset). Cabe folgado em 32 GB.
//! I/O: 100% sequencial (NVMe feliz), zero random-access.

use anyhow::{Context, Result};
use memmap2::Mmap;
use rustc_hash::FxHashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const HASH_LEN: usize = 32;
const BENCODEX_HASH_PREFIX: &[u8] = b"32:";

/// Extrai child hashes do value (padrão Bencodex "32:" + 32 bytes)
fn extract_child_hashes(data: &[u8], out: &mut Vec<[u8; HASH_LEN]>) {
    const PREFIX: &[u8] = BENCODEX_HASH_PREFIX;
    const STEP: usize = PREFIX.len() + HASH_LEN; // 35 bytes

    if data.len() < STEP {
        return;
    }

    let mut i = 0;
    while i + STEP <= data.len() {
        if &data[i..i + PREFIX.len()] == PREFIX {
            if let Ok(arr) = data[i + PREFIX.len()..i + STEP].try_into() {
                out.push(arr);
            }
            i += STEP;
        } else {
            i += 1;
        }
    }
}

/// Lê um registro em `offset`: retorna (key, value, next_offset).
/// Formato: key(32) + value_len(4 LE) + value(value_len).
#[inline]
fn read_record(data: &[u8], offset: usize) -> ([u8; HASH_LEN], &[u8], usize) {
    let key: [u8; HASH_LEN] = data[offset..offset + HASH_LEN].try_into().unwrap();
    let lo = offset + HASH_LEN;
    let vlen = u32::from_le_bytes([data[lo], data[lo + 1], data[lo + 2], data[lo + 3]]) as usize;
    let vo = lo + 4;
    (key, &data[vo..vo + vlen], vo + vlen)
}

/// BFS por fixpoint: working-set mutável, filhos entram no mesmo passe.
/// Repete scans sequenciais completos até um passe não descobrir nada novo.
fn bfs_fixpoint(
    export_path: &Path,
    roots: &[[u8; HASH_LEN]],
) -> Result<FxHashSet<[u8; HASH_LEN]>> {
    eprintln!("🌳 BFS fixpoint (multi-passe, FxHashSet, low-RAM)...");

    let file = File::open(export_path)
    .with_context(|| format!("Failed to open export file: {}", export_path.display()))?;
    let file_size = file.metadata()?.len();
    eprintln!("   File size: {:.2} GB", file_size as f64 / 1024.0 / 1024.0 / 1024.0);

    let mmap = unsafe { Mmap::map(&file) }
    .with_context(|| format!("Failed to mmap file: {}", export_path.display()))?;
    let data: &[u8] = &mmap[..];
    let data_len = data.len();

    // visited = já processado (filhos extraídos).
    // pending = descoberto, ainda não localizado no arquivo.
    let mut visited: FxHashSet<[u8; HASH_LEN]> = FxHashSet::default();
    let mut pending: FxHashSet<[u8; HASH_LEN]> = roots.iter().copied().collect();

    let mut pass = 0;
    let mut buf: Vec<[u8; HASH_LEN]> = Vec::with_capacity(32);

    while !pending.is_empty() {
        let start = std::time::Instant::now();
        let pending_at_start = pending.len();
        let mut found_this_pass = 0usize;

        let mut offset = 0;
        let mut scanned = 0u64;

        while offset + 36 <= data_len {
            let (key, value, next) = read_record(data, offset);
            offset = next;
            scanned += 1;

            // Se está pendente, processa AGORA. Filhos entram no pending e,
            // se estiverem à frente neste arquivo, são pegos neste mesmo passe.
            if pending.remove(&key) {
                visited.insert(key);
                found_this_pass += 1;

                buf.clear();
                extract_child_hashes(value, &mut buf);
                for &child in &buf {
                    if !visited.contains(&child) {
                        pending.insert(child);
                    }
                }
            }

            if scanned % 20_000_000 == 0 {
                eprintln!("   ...scanned {}M | pending {} | visited {}",
                          scanned / 1_000_000, pending.len(), visited.len());
            }
        }

        pass += 1;
        eprintln!("✓ Pass {}: found {} (pending era {}) | resta {} pending | {:.1}s | {} visited",
                  pass, found_this_pass, pending_at_start, pending.len(),
                  start.elapsed().as_secs_f64(), visited.len());

        // Fixpoint: se nada foi achado, o que sobrou em pending não existe no arquivo.
        if found_this_pass == 0 {
            if !pending.is_empty() {
                eprintln!("   ℹ️ {} pending não existem no export (dangling refs), ignorando.",
                          pending.len());
            }
            break;
        }
    }

    eprintln!("✅ BFS fixpoint: {} passes, {} live nodes", pass, visited.len());
    Ok(visited)
}

/// Pipeline completo: lê export file, faz BFS fixpoint, escreve keys vivas.
pub fn run_gc_filter(
    export_path: &Path,
    roots: Vec<[u8; HASH_LEN]>,
    output_path: &Path,
) -> Result<()> {
    eprintln!("🧹 GC Filter Pipeline (BFS Fixpoint 🚀)");
    eprintln!("   Export file: {}", export_path.display());
    eprintln!("   Output file: {}", output_path.display());
    eprintln!("   Roots: {}", roots.len());

    let live_keys = bfs_fixpoint(export_path, &roots)?;

    eprintln!("📤 Writing {} live keys to {}...", live_keys.len(), output_path.display());

    let output_file = File::create(output_path)
    .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, output_file);

    for key in &live_keys {
        writer.write_all(key)?;
    }

    writer.flush()?;
    eprintln!("✅ GC filter complete: {} keys written", live_keys.len());

    Ok(())
}
