//! Filtro BFS para GC de states/ usando pipeline C#/Rust.
//!
//! Pipeline:
//!   CheckpointBridge --gc-states → este filtro → CheckpointBridge --gc-write
//!
//! Estratégia BFS level-by-level para evitar OOM:
//!   1. Level 0: roots
//!   2. Para cada level N:
//!      - Lê arquivo INTEIRO sequencialmente
//!      - Para cada KV no arquivo: se key está em level N, extrai children → level N+1
//!      - Marca key como visitada
//!   3. Repete até não ter mais children
//!
//! Custo: depth × scan_time
//!   - Se trie tem depth ~15 e cada scan = 40min → ~10 horas total
//!   - MAS é sequencial e não precisa de RAM gigante

use anyhow::{Context, Result};
use std::collections::HashSet;
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

/// BFS level-by-level: para cada level, faz scan sequencial completo do arquivo.
///
/// Retorna: HashSet de TODAS as keys vivas (visitadas durante o BFS)
use memmap2::Mmap;

/// BFS level-by-level usando mmap para leitura ultra-rápida
fn bfs_level_by_level(
    export_path: &Path,
    roots: &[[u8; HASH_LEN]],
) -> Result<HashSet<[u8; HASH_LEN]>> {
    eprintln!("🌳 Starting level-by-level BFS (mmap)...");
    eprintln!("   Strategy: Memory-mapped file (zero-copy)");

    // ── MAPEAR O ARQUIVO INTEIRO NA MEMÓRIA ──────────────────────────
    let file = File::open(export_path)
    .with_context(|| format!("Failed to open export file: {}", export_path.display()))?;

    let file_size = file.metadata()?.len();
    eprintln!("   File size: {:.2} GB", file_size as f64 / 1024.0 / 1024.0 / 1024.0);

    // Mapear o arquivo inteiro (zero-copy, o kernel gerencia)
    let mmap = unsafe { Mmap::map(&file) }
    .with_context(|| format!("Failed to mmap file: {}", export_path.display()))?;

    let data = &mmap[..]; // Slice para os dados mapeados
    let data_len = data.len();

    let mut visited = HashSet::new();
    let mut current_level: HashSet<[u8; HASH_LEN]> = roots.iter().copied().collect();
    let mut level_num = 0;
    let mut children_buf = Vec::with_capacity(32);

    while !current_level.is_empty() {
        eprintln!("📂 Level {}: {} nodes to process", level_num, current_level.len());
        let scan_start = std::time::Instant::now();

        let mut next_level = HashSet::new();
        let mut found_in_level = 0;
        let mut offset = 0;
        let mut scanned = 0u64;

        // ── SCAN DO MMAP (ZERO-COPY!) ──────────────────────────────
        while offset + 36 <= data_len {
            // Ler key (32 bytes)
            let key = &data[offset..offset + HASH_LEN];
            let key_arr: [u8; HASH_LEN] = key.try_into().unwrap();
            offset += HASH_LEN;

            // Ler value_len (4 bytes little-endian)
            let value_len = u32::from_le_bytes([
                data[offset], data[offset+1], data[offset+2], data[offset+3]
            ]) as usize;
            offset += 4;

            scanned += 1;

            // Se esta key está no current_level, processar
            if current_level.contains(&key_arr) {
                // Ler o value (diretamente do mmap, sem cópia!)
                let value = &data[offset..offset + value_len];

                // Marcar como visitada
                visited.insert(key_arr);
                found_in_level += 1;

                // Extrair children para next_level
                children_buf.clear();
                extract_child_hashes(value, &mut children_buf);

                for &child in &children_buf {
                    if !visited.contains(&child) {
                        next_level.insert(child);
                    }
                }
            }

            offset += value_len;

            // Progress a cada 10M entries
            if scanned % 10_000_000 == 0 {
                let progress = (offset as f64 / data_len as f64) * 100.0;
                eprintln!("   Scanned {}M entries ({:.1}%)...",
                          scanned / 1_000_000, progress);
            }
        }

        eprintln!("   ✓ Level {}: Found {}/{} nodes, {} children → level {}",
                  level_num,
                  found_in_level,
                  current_level.len(),
                  next_level.len(),
                  level_num + 1);
        eprintln!("   Scan time: {:.1}s ({} total visited)",
                  scan_start.elapsed().as_secs_f64(),
                  visited.len());

        if found_in_level == 0 && !current_level.is_empty() {
            eprintln!("⚠️  Warning: Level {} had {} nodes but NONE were found!",
                      level_num, current_level.len());
            break;
        }

        current_level = next_level;
        level_num += 1;
    }

    eprintln!("✅ BFS complete: {} total levels, {} live nodes", level_num, visited.len());
    Ok(visited)
}

/// Pipeline completo: lê export file, faz BFS level-by-level, escreve keys vivas
pub fn run_gc_filter(
    export_path: &Path,
    roots: Vec<[u8; HASH_LEN]>,
    output_path: &Path,
) -> Result<()> {
    eprintln!("🧹 GC Filter Pipeline (Level-by-Level BFS)");
    eprintln!("   Export file: {}", export_path.display());
    eprintln!("   Output file: {}", output_path.display());
    eprintln!("   Roots: {}", roots.len());

    // BFS level-by-level
    let live_keys = bfs_level_by_level(export_path, &roots)?;

    // Escrever keys vivas para arquivo (32 bytes cada)
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
