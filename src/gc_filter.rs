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

    // Só uma trava de segurança contra loop infinito. NÃO é critério de parada: parar antes do fixpoint
    // descarta nós vivos (os que ainda estão em `pending` e os descendentes deles).
    const SAFETY_MAX_PASSES: usize = 10_000;

    while !pending.is_empty() {
        let start = std::time::Instant::now();
        let pending_at_start = pending.len();
        let mut found_this_pass = 0usize;

        let mut offset = 0;
        let mut scanned = 0u64;

        while offset < data_len {
            let Some((key, value, next)) = read_record(data, offset) else {
                // Antes: `break` silencioso. Um export truncado (disco cheio, cópia interrompida) resultaria
                // num conjunto de nós vivos incompleto, e o prune apagaria nós vivos.
                anyhow::bail!(
                    "Export truncado ou corrompido: registro incompleto no offset {} de {} bytes",
                    offset,
                    data_len
                );
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

        // Cada passe com found > 0 pode ter descoberto filhos novos: continuar até o fixpoint (found == 0).
        if pass >= SAFETY_MAX_PASSES {
            anyhow::bail!(
                "BFS não convergiu em {} passes ({} pending). Abortando para não podar nós vivos.",
                pass,
                pending.len()
            );
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

    // ───────────────────────────────────────────────────────────────────
    // Corretude do BFS: tem que achar EXATAMENTE os nós alcançáveis a partir da raiz,
    // em qualquer ordem de arquivo (o limite antigo de 18 passes descartava nós vivos).
    // ───────────────────────────────────────────────────────────────────
    use std::collections::{HashMap, HashSet};

    type H = [u8; 32];

    /// hash determinístico sem o byte ':' (0x3A), para não criar falsos "32:" por acaso
    fn h(n: u64) -> H {
        let mut out = [0u8; 32];
        let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
        for b in out.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let mut v = (x >> 24) as u8;
            if v == b':' {
                v = b';';
            }
            *b = v;
        }
        out
    }

    struct Node {
        key: H,
        children: Vec<H>,
    }

    fn write_export(path: &std::path::Path, nodes: &[&Node]) {
        use std::io::Write;
        let mut f = std::fs::File::create(path).unwrap();
        for n in nodes {
            let mut value = b"d1:t1:n".to_vec(); // lixo de prefixo, como num valor Bencodex
            for c in &n.children {
                value.extend_from_slice(b"32:");
                value.extend_from_slice(c);
            }
            f.write_all(&n.key).unwrap();
            f.write_all(&(value.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&value).unwrap();
        }
    }

    fn run(nodes: &[&Node], root: H) -> HashSet<H> {
        let dir = tempfile::tempdir().unwrap();
        let (exp, out) = (dir.path().join("export.bin"), dir.path().join("live.bin"));
        write_export(&exp, nodes);
        run_gc_filter(&exp, vec![root], &out).unwrap();
        let bytes = std::fs::read(&out).unwrap();
        assert_eq!(bytes.len() % 32, 0);
        bytes.chunks(32).map(|c| <H>::try_from(c).unwrap()).collect()
    }

    fn chain(n: u64) -> Vec<Node> {
        (0..n)
            .map(|i| Node { key: h(i), children: if i + 1 < n { vec![h(i + 1)] } else { vec![] } })
            .collect()
    }

    #[test]
    fn bfs_chain_in_forward_order_single_pass() {
        let nodes = chain(30);
        let refs: Vec<&Node> = nodes.iter().collect();
        assert_eq!(run(&refs, h(0)).len(), 30);
    }

    #[test]
    fn bfs_chain_deeper_than_18_in_reverse_file_order_keeps_every_live_node() {
        // pior caso: cada passe só desce UM nível (o filho aparece antes do pai no arquivo)
        for depth in [18u64, 19, 21, 30, 64] {
            let nodes = chain(depth);
            let refs: Vec<&Node> = nodes.iter().rev().collect();
            let live = run(&refs, h(0));
            assert_eq!(
                live.len() as u64,
                depth,
                "profundidade {depth}: {} nós vivos foram descartados",
                depth - live.len() as u64
            );
        }
    }

    #[test]
    fn bfs_dangling_references_are_ignored_and_garbage_is_dropped() {
        let a = Node { key: h(1), children: vec![h(2), h(999)] }; // h(999) não existe no export
        let b = Node { key: h(2), children: vec![] };
        let garbage = Node { key: h(3), children: vec![h(2)] }; // ninguém aponta para ele
        let live = run(&[&garbage, &b, &a], h(1));
        assert_eq!(live, HashSet::from([h(1), h(2)]));
    }

    #[test]
    fn bfs_truncated_export_is_an_error_not_a_silent_partial_result() {
        let nodes = chain(10);
        let refs: Vec<&Node> = nodes.iter().collect();
        let dir = tempfile::tempdir().unwrap();
        let (exp, out) = (dir.path().join("export.bin"), dir.path().join("live.bin"));
        write_export(&exp, &refs);
        let len = std::fs::metadata(&exp).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&exp).unwrap();
        f.set_len(len - 5).unwrap(); // corta o fim do último registro
        let err = run_gc_filter(&exp, vec![h(0)], &out).unwrap_err();
        assert!(err.to_string().contains("truncado"), "erro inesperado: {err}");
    }

    /// Propriedade: em árvores aleatórias, com ordem aleatória no arquivo, live == alcançáveis (DFS em memória).
    #[test]
    fn bfs_random_forests_match_reference_reachability() {
        let mut s: u64 = 0x1234_5678_9ABC_DEF1;
        let mut rnd = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        for round in 0..40u64 {
            let n = 20 + (rnd() % 400);
            // árvore "comprida": o pai de i está entre os últimos 3 índices -> profundidade grande
            let mut nodes: Vec<Node> =
                (0..n).map(|i| Node { key: h(round * 10_000 + i), children: vec![] }).collect();
            for i in 1..n {
                let p = i.saturating_sub(1 + rnd() % 3);
                let child = nodes[i as usize].key;
                nodes[p as usize].children.push(child);
            }
            let garbage: Vec<Node> =
                (0..n / 2).map(|i| Node { key: h(round * 10_000 + 5_000 + i), children: vec![] }).collect();
            let mut all: Vec<&Node> = nodes.iter().chain(garbage.iter()).collect();
            for i in (1..all.len()).rev() {
                all.swap(i, (rnd() % (i as u64 + 1)) as usize); // embaralha
            }
            let by_key: HashMap<H, &Node> = nodes.iter().map(|x| (x.key, x)).collect();
            let mut expected = HashSet::new();
            let mut stack = vec![nodes[0].key];
            while let Some(k) = stack.pop() {
                if expected.insert(k) {
                    stack.extend(by_key[&k].children.iter().copied());
                }
            }
            let live = run(&all, nodes[0].key);
            let missing = expected.difference(&live).count();
            assert_eq!(missing, 0, "rodada {round}: {missing} nós vivos descartados (de {})", expected.len());
            assert_eq!(live, expected, "rodada {round}");
        }
    }
}
