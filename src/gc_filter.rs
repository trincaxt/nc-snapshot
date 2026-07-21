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

use crate::io_util::{read_record, HASH_LEN};
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

    const MAX_PASSES: usize = 18; // Early termination: evita passes 19-20 ineficientes

    while !pending.is_empty() {
        let start = std::time::Instant::now();
        let pending_at_start = pending.len();
        let mut found_this_pass = 0usize;

        let mut offset = 0;
        let mut scanned = 0u64;

        while offset < data_len {
            let Some((key, value, next)) = read_record(data, offset) else {
                break;
            };
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

        // Early termination: últimos passes têm custo/benefício ruim
        if pass >= MAX_PASSES {
            if !pending.is_empty() {
                eprintln!("   ⏱️ Reached max passes ({}), stopping early. {} pending ignored (likely dangling).",
                          MAX_PASSES, pending.len());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_child_hashes_empty() {
        let mut out = Vec::new();
        extract_child_hashes(b"", &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn test_extract_child_hashes_no_prefix() {
        let mut out = Vec::new();
        extract_child_hashes(b"no hash markers here", &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn test_extract_child_hashes_single() {
        let mut data = b"32:".to_vec();
        let hash = [0xAB; 32];
        data.extend_from_slice(&hash);

        let mut out = Vec::new();
        extract_child_hashes(&data, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], hash);
    }

    #[test]
    fn test_extract_child_hashes_multiple() {
        let mut data = b"32:".to_vec();
        let hash1 = [0xAA; 32];
        let hash2 = [0xBB; 32];
        data.extend_from_slice(&hash1);
        data.extend_from_slice(b"32:");
        data.extend_from_slice(&hash2);

        let mut out = Vec::new();
        extract_child_hashes(&data, &mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], hash1);
        assert_eq!(out[1], hash2);
    }

    #[test]
    fn test_extract_child_hashes_adjacent() {
        // 32: + hash1 + 32: + hash2 — no space between
        let mut data = b"32:".to_vec();
        data.extend_from_slice(&[0x11; 32]);
        data.extend_from_slice(b"32:");
        data.extend_from_slice(&[0x22; 32]);
        // plus trailing non-hash data
        data.extend_from_slice(b"extra");

        let mut out = Vec::new();
        extract_child_hashes(&data, &mut out);
        assert_eq!(out.len(), 2);
    }
}
