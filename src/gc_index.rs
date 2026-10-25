//! Índice ordenado hash→offset do export + BFS por nível + bitset de nós vivos.
//!
//! Por que funciona sem hashmap: o export é gerado iterando o RocksDB em ordem de
//! chave, então as chaves saem em ordem crescente. Logo:
//!   ordinal (posição no export) == posição no índice == ordem por hash == ordem por offset.
//! Isso dá três coisas de graça:
//!   1. lookup hash→ordinal por tabela de buckets + busca binária curta (sem hashmap);
//!   2. conjunto de vivos = bitset indexado pelo ordinal (N bits, ~12 MB p/ 100M nós);
//!   3. ordenar a fronteira do BFS por ordinal = ordenar por offset (leitura monotônica).
//!
//! Formato do índice: N entradas de 40 bytes = [key:32][offset:8 LE], sem header.
//! `offset` é a posição do início do registro (key32+len4+value) em states_export.bin.

use anyhow::{bail, Context, Result};
use memmap2::{Advice, Mmap};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::gc_filter::extract_child_hashes;
use crate::io_util::{read_record, HASH_LEN};

pub const ENTRY_LEN: usize = HASH_LEN + 8;
const MAX_BUCKET_BITS: u32 = 24; // 2^24 * 4 B = 64 MB de tabela no máximo
const CHUNK: usize = 4096; // nós por tarefa rayon

/// states_export.bin -> states_export.idx
pub fn index_path_for(export: &Path) -> PathBuf {
    export.with_extension("idx")
}

// ───────────────────────────── escrita ─────────────────────────────

/// Escreve o índice durante o export. Exige chaves estritamente crescentes
/// (se o RocksDB não entregar em ordem, falha alto em vez de gerar índice inválido).
/// Escreve em `<path>.partial` e só renomeia no `finish()`: índice incompleto nunca é usado.
pub struct IndexWriter {
    w: BufWriter<File>,
    partial: PathBuf,
    final_path: PathBuf,
    last: Option<[u8; HASH_LEN]>,
    count: u64,
}

impl IndexWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let mut partial = path.as_os_str().to_owned();
        partial.push(".partial");
        let partial = PathBuf::from(partial);
        let f = File::create(&partial)
            .with_context(|| format!("Falha ao criar índice: {}", partial.display()))?;
        Ok(Self {
            w: BufWriter::with_capacity(16 * 1024 * 1024, f),
            partial,
            final_path: path.to_path_buf(),
            last: None,
            count: 0,
        })
    }

    #[inline]
    pub fn push(&mut self, key: &[u8], offset: u64) -> Result<()> {
        if key.len() != HASH_LEN {
            bail!("chave com {} bytes (esperado {})", key.len(), HASH_LEN);
        }
        if let Some(last) = &self.last {
            if key <= &last[..] {
                bail!(
                    "export fora de ordem na entrada {}: o índice exige chaves estritamente crescentes",
                    self.count
                );
            }
        }
        self.w.write_all(key)?;
        self.w.write_all(&offset.to_le_bytes())?;
        let mut k = [0u8; HASH_LEN];
        k.copy_from_slice(key);
        self.last = Some(k);
        self.count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<u64> {
        self.w.flush()?;
        drop(self.w);
        std::fs::rename(&self.partial, &self.final_path)
            .with_context(|| format!("Falha ao finalizar índice: {}", self.final_path.display()))?;
        Ok(self.count)
    }
}

/// Gera o índice a partir de um export já existente (varredura sequencial).
/// Útil para testar em cima do export atual sem refazer o export.
/// O pipeline gera o índice durante o export (exporter.rs); esta função é
/// usada por examples/scale_example.rs.
#[allow(dead_code)]
pub fn build_index_from_export(export_path: &Path, index_path: &Path) -> Result<u64> {
    let file = File::open(export_path)
        .with_context(|| format!("Falha ao abrir export: {}", export_path.display()))?;
    let mmap = unsafe { Mmap::map(&file)? };
    let _ = mmap.advise(Advice::Sequential);
    let data: &[u8] = &mmap[..];

    let mut iw = IndexWriter::create(index_path)?;
    let mut offset = 0usize;
    while offset < data.len() {
        let Some((key, _value, next)) = read_record(data, offset) else {
            bail!(
                "Export truncado ou corrompido: registro incompleto no offset {} de {} bytes",
                offset,
                data.len()
            );
        };
        iw.push(&key, offset as u64)?;
        offset = next;
    }
    iw.finish()
}

// ───────────────────────────── leitura ─────────────────────────────

pub struct Index {
    map: Mmap,
    n: usize,
    bits: u32,
    starts: Vec<u32>, // starts[p] = nº de entradas com prefixo < p ; len = 2^bits + 1
}

impl Index {
    /// Abre e VALIDA o índice contra o export: tamanho múltiplo de 40, chaves e offsets
    /// estritamente crescentes, primeiro offset 0, e o último registro termina exatamente
    /// no fim do export (pega export truncado ou índice de outra geração).
    pub fn open(index_path: &Path, export: &[u8]) -> Result<Self> {
        let f = File::open(index_path)
            .with_context(|| format!("Falha ao abrir índice: {}", index_path.display()))?;
        let map = unsafe { Mmap::map(&f)? };
        if map.len() % ENTRY_LEN != 0 || map.is_empty() {
            bail!("Índice inválido: {} bytes não é múltiplo de {}", map.len(), ENTRY_LEN);
        }
        let n = map.len() / ENTRY_LEN;
        if n > u32::MAX as usize {
            bail!("Índice com {} entradas excede u32", n);
        }

        // ~4 entradas por bucket em média, entre 2^1 e 2^24 buckets
        let bits = ((n / 4).max(2) as f64).log2().ceil().clamp(1.0, MAX_BUCKET_BITS as f64) as u32;
        let mut starts = vec![0u32; (1usize << bits) + 1];

        let mut prev_key: Option<&[u8]> = None;
        let mut prev_off: u64 = 0;
        for i in 0..n {
            let e = &map[i * ENTRY_LEN..(i + 1) * ENTRY_LEN];
            let key = &e[..HASH_LEN];
            let off = u64::from_le_bytes(e[HASH_LEN..].try_into().unwrap());
            if let Some(pk) = prev_key {
                if key <= pk || off <= prev_off {
                    bail!("Índice fora de ordem na entrada {}", i);
                }
            } else if off != 0 {
                bail!("Primeiro registro do índice deveria estar no offset 0 (achei {})", off);
            }
            prev_key = Some(key);
            prev_off = off;
            starts[prefix_of(key, bits) + 1] += 1;
        }
        for p in 0..(1usize << bits) {
            starts[p + 1] += starts[p];
        }

        let idx = Self { map, n, bits, starts };

        // O último registro tem que fechar exatamente o export.
        let last = n as u32 - 1;
        match read_record(export, idx.offset(last) as usize) {
            Some((k, _, next)) if next == export.len() && k[..] == *idx.key(last) => {}
            _ => bail!(
                "Índice não corresponde ao export (último registro não fecha em {} bytes). \
                 Export truncado ou índice de outra execução.",
                export.len()
            ),
        }
        Ok(idx)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.n
    }

    #[inline]
    pub fn key(&self, ord: u32) -> &[u8] {
        let s = ord as usize * ENTRY_LEN;
        &self.map[s..s + HASH_LEN]
    }

    #[inline]
    pub fn offset(&self, ord: u32) -> u64 {
        let s = ord as usize * ENTRY_LEN + HASH_LEN;
        u64::from_le_bytes(self.map[s..s + 8].try_into().unwrap())
    }

    /// hash -> ordinal. None se o hash não existe no export (dangling ref: ignorar, como no BFS atual).
    #[inline]
    pub fn lookup(&self, key: &[u8; HASH_LEN]) -> Option<u32> {
        let p = prefix_of(key, self.bits);
        let (mut lo, mut hi) = (self.starts[p] as usize, self.starts[p + 1] as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(mid as u32).cmp(&key[..]) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid as u32),
            }
        }
        None
    }
}

#[inline]
fn prefix_of(key: &[u8], bits: u32) -> usize {
    (u32::from_be_bytes([key[0], key[1], key[2], key[3]]) >> (32 - bits)) as usize
}

// ───────────────────────────── bitset de vivos ─────────────────────────────

/// Conjunto de nós vivos: bit i = i-ésimo registro do export.
pub struct LiveSet {
    words: Vec<u64>,
    n: u64,
}

impl LiveSet {
    #[inline]
    pub fn get(&self, ord: u64) -> bool {
        ord < self.n && (self.words[(ord >> 6) as usize] >> (ord & 63)) & 1 == 1
    }
    pub fn len(&self) -> u64 {
        self.n
    }
    pub fn count_ones(&self) -> u64 {
        self.words.iter().map(|w| w.count_ones() as u64).sum()
    }

    /// Formato: [n:8 LE][words: 8 LE cada]
    pub fn save(&self, path: &Path) -> Result<()> {
        let mut w = BufWriter::new(File::create(path)?);
        w.write_all(&self.n.to_le_bytes())?;
        for word in &self.words {
            w.write_all(&word.to_le_bytes())?;
        }
        w.flush()?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("Falha ao ler bitset: {}", path.display()))?;
        if bytes.len() < 8 {
            bail!("Bitset truncado");
        }
        let n = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let nwords = ((n + 63) / 64) as usize;
        if bytes.len() != 8 + nwords * 8 {
            bail!("Bitset com tamanho inconsistente (n={}, bytes={})", n, bytes.len());
        }
        let words = bytes[8..]
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        Ok(Self { words, n })
    }
}

// ───────────────────────────── BFS por nível ─────────────────────────────

/// BFS por nível sobre o índice. Cada nível: ordena a fronteira por ordinal (= por offset),
/// lê só esses registros do export (mmap) em paralelo e marca os filhos no bitset atômico.
/// Filho que não existe no índice é ignorado (dangling ref), igual ao BFS fixpoint atual.
pub fn bfs_indexed(export: &[u8], index: &Index, roots: &[[u8; HASH_LEN]]) -> Result<LiveSet> {
    let n = index.len();
    let words: Vec<AtomicU64> = (0..(n + 63) / 64).map(|_| AtomicU64::new(0)).collect();
    // true se este chamador foi quem marcou o bit (primeira vez)
    let claim = |o: u32| -> bool {
        let m = 1u64 << (o & 63);
        words[(o >> 6) as usize].fetch_or(m, Ordering::Relaxed) & m == 0
    };

    let mut frontier: Vec<u32> = Vec::new();
    for r in roots {
        if let Some(o) = index.lookup(r) {
            if claim(o) {
                frontier.push(o);
            }
        }
    }

    let mut level = 0usize;
    let mut live = frontier.len() as u64;
    while !frontier.is_empty() {
        let t = std::time::Instant::now();
        frontier.par_sort_unstable();
        let size = frontier.len();

        let parts: Vec<Vec<u32>> = frontier
            .par_chunks(CHUNK)
            .map(|chunk| -> Result<Vec<u32>> {
                let mut out = Vec::new();
                let mut buf: Vec<[u8; HASH_LEN]> = Vec::with_capacity(32);
                for &o in chunk {
                    let off = index.offset(o) as usize;
                    let Some((key, value, _)) = read_record(export, off) else {
                        bail!("Export truncado ou corrompido no offset {} (ordinal {})", off, o);
                    };
                    if key[..] != *index.key(o) {
                        bail!("Índice não corresponde ao export no ordinal {}", o);
                    }
                    buf.clear();
                    extract_child_hashes(value, &mut buf);
                    for c in &buf {
                        if let Some(co) = index.lookup(c) {
                            if claim(co) {
                                out.push(co);
                            }
                        }
                    }
                }
                Ok(out)
            })
            .collect::<Result<Vec<_>>>()?;

        let next: Vec<u32> = parts.into_iter().flatten().collect();
        live += next.len() as u64;
        level += 1;
        eprintln!(
            "✓ Nível {}: leu {} nós | achou {} novos | {} vivos | {:.1}s",
            level,
            size,
            next.len(),
            live,
            t.elapsed().as_secs_f64()
        );
        frontier = next;
    }

    let words: Vec<u64> = words.into_iter().map(|w| w.into_inner()).collect();
    Ok(LiveSet { words, n: n as u64 })
}

/// Pipeline: export + índice -> bitset de vivos gravado em `live_bits_path`.
pub fn run_gc_filter_indexed(
    export_path: &Path,
    index_path: &Path,
    roots: Vec<[u8; HASH_LEN]>,
    live_bits_path: &Path,
) -> Result<LiveSet> {
    eprintln!("🧹 GC Filter (BFS indexado por nível)");
    let file = File::open(export_path)
        .with_context(|| format!("Falha ao abrir export: {}", export_path.display()))?;
    let mmap = unsafe { Mmap::map(&file)? };
    // acesso aleatório: desliga o readahead do kernel (senão cada leitura puxa ~128 KB à toa)
    let _ = mmap.advise(Advice::Random);
    let data: &[u8] = &mmap[..];

    let t = std::time::Instant::now();
    let index = Index::open(index_path, data)?;
    eprintln!("   Índice: {} entradas validadas em {:.1}s", index.len(), t.elapsed().as_secs_f64());

    let live = bfs_indexed(data, &index, &roots)?;
    live.save(live_bits_path)?;
    eprintln!("✅ {} nós vivos de {} ({} bytes de bitset)", live.count_ones(), live.len(), live.words.len() * 8);
    Ok(live)
}

/// Comparação exata com um live_keys.bin gerado pelo BFS fixpoint antigo (validação em dados reais):
/// toda chave do arquivo tem que estar viva no bitset e as contagens têm que bater.
#[allow(dead_code)]
pub fn compare_with_keys_file(index: &Index, live: &LiveSet, live_keys_file: &Path) -> Result<()> {
    use std::io::Read;
    let mut f = std::io::BufReader::with_capacity(16 * 1024 * 1024, File::open(live_keys_file)?);
    let mut buf = [0u8; HASH_LEN];
    let mut n_file = 0u64;
    while f.read_exact(&mut buf).is_ok() {
        n_file += 1;
        match index.lookup(&buf) {
            Some(o) if live.get(o as u64) => {}
            _ => bail!("Chave do BFS antigo ausente no BFS novo: {:02x?}", &buf[..8]),
        }
    }
    let n_bits = live.count_ones();
    if n_file != n_bits {
        bail!("Contagens diferem: antigo {} vs novo {}", n_file, n_bits);
    }
    eprintln!("✅ Conjuntos idênticos: {} nós vivos", n_bits);
    Ok(())
}

// ───────────────────────────── testes ─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc_filter::run_gc_filter;
    use std::collections::HashSet;

    type H = [u8; 32];

    /// hash sem o byte ':' para não criar falsos "32:" por acaso
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

    /// escreve o export ORDENADO por chave (como o RocksDB entrega)
    fn write_export(path: &Path, nodes: &[&Node]) {
        let mut sorted: Vec<&&Node> = nodes.iter().collect();
        sorted.sort_by(|a, b| a.key.cmp(&b.key));
        let mut f = File::create(path).unwrap();
        for n in sorted {
            let mut value = b"d1:t1:n".to_vec();
            for c in &n.children {
                value.extend_from_slice(b"32:");
                value.extend_from_slice(c);
            }
            f.write_all(&n.key).unwrap();
            f.write_all(&(value.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&value).unwrap();
        }
    }

    /// roda BFS antigo e novo no mesmo export; devolve (antigo, novo) como conjuntos de chaves
    fn both(nodes: &[&Node], roots: Vec<H>) -> (HashSet<H>, HashSet<H>) {
        let dir = tempfile::tempdir().unwrap();
        let exp = dir.path().join("export.bin");
        let idx = index_path_for(&exp);
        let old_out = dir.path().join("live.bin");
        let bits = dir.path().join("live.bits");
        write_export(&exp, nodes);

        run_gc_filter(&exp, roots.clone(), &old_out).unwrap();
        let old: HashSet<H> = std::fs::read(&old_out)
            .unwrap()
            .chunks(32)
            .map(|c| <H>::try_from(c).unwrap())
            .collect();

        build_index_from_export(&exp, &idx).unwrap();
        let live = run_gc_filter_indexed(&exp, &idx, roots, &bits).unwrap();

        // round-trip do bitset em disco
        let live2 = LiveSet::load(&bits).unwrap();
        assert_eq!(live2.count_ones(), live.count_ones());

        let file = File::open(&exp).unwrap();
        let mm = unsafe { Mmap::map(&file).unwrap() };
        let index = Index::open(&idx, &mm[..]).unwrap();
        let new: HashSet<H> = (0..index.len() as u32)
            .filter(|&o| live2.get(o as u64))
            .map(|o| <H>::try_from(index.key(o)).unwrap())
            .collect();
        compare_with_keys_file(&index, &live2, &old_out).unwrap();
        (old, new)
    }

    #[test]
    fn indexed_equals_fixpoint_on_deep_chains() {
        for depth in [1u64, 2, 18, 19, 64, 300] {
            let nodes: Vec<Node> = (0..depth)
                .map(|i| Node { key: h(i), children: if i + 1 < depth { vec![h(i + 1)] } else { vec![] } })
                .collect();
            let refs: Vec<&Node> = nodes.iter().collect();
            let (old, new) = both(&refs, vec![h(0)]);
            assert_eq!(old.len() as u64, depth);
            assert_eq!(old, new, "profundidade {depth}");
        }
    }

    #[test]
    fn dangling_refs_garbage_and_missing_root() {
        let a = Node { key: h(1), children: vec![h(2), h(999)] }; // 999 não existe
        let b = Node { key: h(2), children: vec![] };
        let g = Node { key: h(3), children: vec![h(2)] }; // lixo
        let (old, new) = both(&[&g, &b, &a], vec![h(1)]);
        assert_eq!(new, HashSet::from([h(1), h(2)]));
        assert_eq!(old, new);

        // raiz que não existe -> conjunto vazio nos dois
        let (old, new) = both(&[&g, &b, &a], vec![h(12345)]);
        assert!(old.is_empty() && new.is_empty());
    }

    /// Grafos aleatórios com compartilhamento (DAG) e ciclos, vários roots, lixo e dangling refs.
    #[test]
    fn indexed_equals_fixpoint_on_random_graphs() {
        let mut s: u64 = 0x1234_5678_9ABC_DEF1;
        let mut rnd = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        for round in 0..60u64 {
            let n = 5 + rnd() % 600;
            let base = round * 100_000;
            let mut nodes: Vec<Node> =
                (0..n).map(|i| Node { key: h(base + i), children: vec![] }).collect();
            for i in 0..n as usize {
                for _ in 0..(rnd() % 4) {
                    let c = if rnd() % 10 == 0 {
                        h(base + 50_000 + rnd() % 50) // dangling
                    } else {
                        h(base + rnd() % n)
                    };
                    nodes[i].children.push(c);
                }
            }
            let refs: Vec<&Node> = nodes.iter().collect();
            let roots = vec![h(base), h(base + rnd() % n)];
            let (old, new) = both(&refs, roots);
            assert_eq!(old, new, "rodada {round}");
        }
    }

    #[test]
    fn unsorted_keys_are_rejected_by_writer() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = IndexWriter::create(&dir.path().join("x.idx")).unwrap();
        w.push(&[2u8; 32], 0).unwrap();
        assert!(w.push(&[1u8; 32], 36).is_err());
        let mut w = IndexWriter::create(&dir.path().join("y.idx")).unwrap();
        w.push(&[2u8; 32], 0).unwrap();
        assert!(w.push(&[2u8; 32], 36).is_err(), "duplicata também é erro");
    }

    #[test]
    fn truncated_export_is_an_error_everywhere() {
        let nodes: Vec<Node> = (0..10)
            .map(|i| Node { key: h(i), children: if i < 9 { vec![h(i + 1)] } else { vec![] } })
            .collect();
        let refs: Vec<&Node> = nodes.iter().collect();
        let dir = tempfile::tempdir().unwrap();
        let exp = dir.path().join("export.bin");
        let idx = index_path_for(&exp);
        write_export(&exp, &refs);

        // índice gerado ANTES do truncamento (índice "de outra geração")
        build_index_from_export(&exp, &idx).unwrap();
        let len = std::fs::metadata(&exp).unwrap().len();
        std::fs::OpenOptions::new().write(true).open(&exp).unwrap().set_len(len - 5).unwrap();

        // 1) construir índice de export truncado falha
        let e = build_index_from_export(&exp, &dir.path().join("z.idx")).unwrap_err();
        assert!(e.to_string().contains("truncado"), "{e}");
        // 2) usar índice antigo com export truncado falha na abertura
        let e = run_gc_filter_indexed(&exp, &idx, vec![h(0)], &dir.path().join("b")).err().unwrap();
        assert!(e.to_string().contains("não corresponde"), "{e}");
    }

    #[test]
    fn partial_index_is_never_visible_before_finish() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.idx");
        let mut w = IndexWriter::create(&p).unwrap();
        w.push(&[1u8; 32], 0).unwrap();
        assert!(!p.exists(), "índice só aparece depois do finish()");
        w.finish().unwrap();
        assert!(p.exists());
    }

    #[test]
    fn lookup_hits_every_key_and_misses_absent_ones() {
        let nodes: Vec<Node> = (0..2000).map(|i| Node { key: h(i), children: vec![] }).collect();
        let refs: Vec<&Node> = nodes.iter().collect();
        let dir = tempfile::tempdir().unwrap();
        let exp = dir.path().join("export.bin");
        let idx = index_path_for(&exp);
        write_export(&exp, &refs);
        build_index_from_export(&exp, &idx).unwrap();
        let file = File::open(&exp).unwrap();
        let mm = unsafe { Mmap::map(&file).unwrap() };
        let index = Index::open(&idx, &mm[..]).unwrap();
        for i in 0..2000 {
            let o = index.lookup(&h(i)).expect("deveria achar");
            assert_eq!(index.key(o), &h(i)[..]);
        }
        for i in 2000..2500 {
            assert!(index.lookup(&h(i)).is_none());
        }
    }
}
