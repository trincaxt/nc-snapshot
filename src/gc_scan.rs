//! GC de states/ por VARREDURA: sem arquivo de export.
//!
//! O pipeline antigo grava ~100 GB de export em disco, lê esse arquivo duas vezes (grafo e poda) e
//! o apaga no fim. Aqui o checkpoint do RocksDB (imutável) é varrido três vezes e só o que precisa
//! ficar na RAM fica:
//!
//!   1. varredura 1: só as chaves, em ordem  -> `KeyIndex` (12 B por chave, tudo em RAM)
//!   2. varredura 2: os valores              -> grafo CSR de referências (filhos como ordinais u32)
//!   3. BFS em RAM a partir da raiz          -> `LiveSet` (1 bit por nó)
//!   4. varredura 3: copia só os vivos       -> destino (`EntrySink`)
//!
//! Os ordinais são a posição na varredura (ordem crescente de chave), a mesma ordem do índice do
//! pipeline antigo; por isso o `LiveSet` é idêntico ao que o pipeline antigo produz.
//!
//! O módulo é genérico sobre `EntrySource`/`EntrySink`. As implementações para RocksDB ficam em
//! `gc_scan_rocks.rs`.
//!
//! RAM aproximada: 12 B × N (índice) + 64 MB (tabela de prefixos) + 8 B × N + 4 B × E (grafo).
//!
//! Índice compacto: de cada chave de 32 B guarda-se só os bytes 3..15 (12 B); os 3 primeiros são o
//! balde da tabela de prefixos. Duas chaves reais com os mesmos 15 primeiros bytes abortam a
//! construção (chance ~1e-19 para hashes). Uma chave AUSENTE com os mesmos 15 bytes de uma presente
//! seria tomada como presente: o efeito é só manter um nó a mais, nunca apagar um vivo.

use anyhow::{anyhow, bail, Result};
use rayon::prelude::*;
use std::sync::mpsc::sync_channel;
use std::time::Instant;

use crate::gc_filter::extract_child_hashes;
use crate::gc_index::{bfs_from_ordinals, Graph, LiveSet};
use crate::io_util::HASH_LEN;

/// Entradas por janela enviada da thread leitora para o processamento paralelo.
const WIN_ENTRIES: usize = 32_768;
/// Teto de bytes de valores por janela (valores grandes fecham a janela antes).
const WIN_BYTES: usize = 48 << 20;
/// Entradas por tarefa rayon dentro de uma janela.
const SUB_ENTRIES: usize = 2_048;
/// Entradas por lote enviado ao destino na varredura 3.
const OUT_BATCH_ENTRIES: usize = 16_384;
const OUT_BATCH_BYTES: usize = 16 << 20;
/// Janelas/lotes em voo entre leitor e consumidor.
const QUEUE_DEPTH: usize = 3;

/// Bytes 0..3 da chave = balde; bytes 3..15 ficam guardados.
const KEY_SUFFIX_START: usize = 3;
const KEY_SUFFIX_LEN: usize = 12;
const KEY_PREFIX_LEN: usize = KEY_SUFFIX_START + KEY_SUFFIX_LEN;
const KEY_CHUNK_BITS: usize = 20;
const KEY_CHUNK: usize = 1 << KEY_CHUNK_BITS;
const PREFIX_BUCKETS: usize = 1 << 24;

// ───────────────────────────── fonte e destino ─────────────────────────────

/// Fonte de entradas chave/valor, varrida sempre na MESMA ordem (ordem crescente de chave).
pub trait EntrySource: Sync {
    /// Percorre todas as entradas. Se `f` devolver `Err`, a varredura para e o erro é devolvido.
    fn scan(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()>;
}

/// Lote de entradas (chave de 32 B + valor) para o destino.
#[derive(Default)]
pub struct KvBatch {
    data: Vec<u8>,
    vlens: Vec<usize>,
}

impl KvBatch {
    fn push(&mut self, key: &[u8], value: &[u8]) {
        debug_assert_eq!(key.len(), HASH_LEN);
        self.data.extend_from_slice(key);
        self.data.extend_from_slice(value);
        self.vlens.push(value.len());
    }

    pub fn len(&self) -> usize {
        self.vlens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vlens.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.data.len()
    }

    pub fn iter(&self) -> KvIter<'_> {
        KvIter { b: self, i: 0, pos: 0 }
    }
}

pub struct KvIter<'a> {
    b: &'a KvBatch,
    i: usize,
    pos: usize,
}

impl<'a> Iterator for KvIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.b.vlens.len() {
            return None;
        }
        let vl = self.b.vlens[self.i];
        let k = &self.b.data[self.pos..self.pos + HASH_LEN];
        let v = &self.b.data[self.pos + HASH_LEN..self.pos + HASH_LEN + vl];
        self.pos += HASH_LEN + vl;
        self.i += 1;
        Some((k, v))
    }
}

/// Destino das entradas vivas (as chaves chegam em ordem crescente).
pub trait EntrySink {
    fn write(&mut self, batch: &KvBatch) -> Result<()>;
    /// Chamado uma vez, depois do último lote (flush/compactação).
    fn finish(&mut self) -> Result<()>;
}

/// Destino que só conta: serve para medir as varreduras sem escrever nada.
#[derive(Default)]
#[allow(dead_code)]
pub struct NullSink {
    pub entries: u64,
    pub bytes: u64,
}

impl EntrySink for NullSink {
    fn write(&mut self, batch: &KvBatch) -> Result<()> {
        self.entries += batch.len() as u64;
        self.bytes += batch.bytes() as u64;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

// ───────────────────────────── índice de chaves em RAM ─────────────────────────────

/// Chaves em ordem, guardando 12 B por chave (bytes 3..15), em pedaços de 1M, mais uma tabela por
/// prefixo de 3 bytes: `starts[p]` = ordinal da primeira chave com prefixo >= p. O lookup é um acesso
/// à tabela mais uma busca binária num balde de poucas chaves.
pub struct KeyIndex {
    chunks: Vec<Vec<[u8; KEY_SUFFIX_LEN]>>,
    n: usize,
    starts: Vec<u32>,
}

impl KeyIndex {
    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// RAM usada pelo índice, em bytes.
    pub fn ram_bytes(&self) -> usize {
        self.n * KEY_SUFFIX_LEN + self.starts.len() * 4
    }

    #[inline]
    fn suffix(&self, ord: usize) -> &[u8; KEY_SUFFIX_LEN] {
        &self.chunks[ord >> KEY_CHUNK_BITS][ord & (KEY_CHUNK - 1)]
    }

    #[inline]
    pub fn lookup(&self, k: &[u8; HASH_LEN]) -> Option<u32> {
        let p = ((k[0] as usize) << 16) | ((k[1] as usize) << 8) | k[2] as usize;
        let want = &k[KEY_SUFFIX_START..KEY_PREFIX_LEN];
        let (mut lo, mut hi) = (self.starts[p] as usize, self.starts[p + 1] as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.suffix(mid).as_slice().cmp(want) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid as u32),
            }
        }
        None
    }

    /// Balde (prefixo de 3 bytes) ao qual o ordinal pertence.
    fn bucket_of(&self, ord: usize) -> usize {
        self.starts.partition_point(|&s| (s as usize) <= ord) - 1
    }

    /// Avança o balde `p` até o que contém `ord` (os ordinais só crescem numa varredura).
    #[inline]
    fn advance_bucket(&self, mut p: usize, ord: usize) -> usize {
        while p + 1 < PREFIX_BUCKETS && self.starts[p + 1] as usize <= ord {
            p += 1;
        }
        p
    }

    /// A chave varrida na posição `ord` (balde `p`) é a mesma que o índice guardou? Confere os 15
    /// primeiros bytes: basta para detectar o DB mudando ou os ordinais desalinhando.
    #[inline]
    fn matches(&self, p: usize, ord: usize, key: &[u8; HASH_LEN]) -> bool {
        key[0] == (p >> 16) as u8
            && key[1] == (p >> 8) as u8
            && key[2] == p as u8
            && self.suffix(ord).as_slice() == &key[KEY_SUFFIX_START..KEY_PREFIX_LEN]
    }
}

struct Progress {
    t: Instant,
    what: &'static str,
}

impl Progress {
    fn new(what: &'static str) -> Self {
        Progress { t: Instant::now(), what }
    }

    fn tick(&self, n: usize) {
        if n > 0 && n % 10_000_000 == 0 {
            self.print(n);
        }
    }

    fn print(&self, n: usize) {
        let s = self.t.elapsed().as_secs_f64();
        eprintln!("   [{}] {}M entradas | {:.0}K/s", self.what, n / 1_000_000, n as f64 / s / 1e3);
    }
}

/// Varredura 1. Devolve o índice e quantas chaves com tamanho diferente de 32 B foram ignoradas
/// (o pipeline antigo também só considerava chaves de 32 B).
pub fn build_key_index(src: &dyn EntrySource) -> Result<(KeyIndex, u64)> {
    let t = Instant::now();
    let prog = Progress::new("varredura 1: chaves");
    let mut chunks: Vec<Vec<[u8; KEY_SUFFIX_LEN]>> = Vec::new();
    let mut starts = vec![0u32; PREFIX_BUCKETS + 1];
    let mut filled = 0usize;
    let mut n = 0usize;
    let mut skipped = 0u64;
    let mut prev: Option<[u8; HASH_LEN]> = None;

    src.scan(&mut |k, _v| {
        let Ok(key) = <[u8; HASH_LEN]>::try_from(k) else {
            skipped += 1;
            return Ok(());
        };
        if let Some(p) = &prev {
            if key <= *p {
                bail!("Chaves fora de ordem ou duplicadas na varredura (ordinal {})", n);
            }
            if key[..KEY_PREFIX_LEN] == p[..KEY_PREFIX_LEN] {
                bail!(
                    "Colisão: duas chaves reais compartilham os mesmos {} primeiros bytes (ordinal {}). \
                     O índice compacto não serve para este DB; use o pipeline com export.",
                    KEY_PREFIX_LEN,
                    n
                );
            }
        }
        if n >= u32::MAX as usize {
            bail!("Mais de 4 bilhões de chaves: o índice usa ordinais de 32 bits");
        }
        let pre = ((key[0] as usize) << 16) | ((key[1] as usize) << 8) | key[2] as usize;
        while filled <= pre {
            starts[filled] = n as u32;
            filled += 1;
        }
        if n & (KEY_CHUNK - 1) == 0 {
            chunks.push(Vec::with_capacity(KEY_CHUNK));
        }
        chunks
            .last_mut()
            .expect("chunk existe")
            .push(<[u8; KEY_SUFFIX_LEN]>::try_from(&key[KEY_SUFFIX_START..KEY_PREFIX_LEN]).expect("12 bytes"));
        n += 1;
        prev = Some(key);
        prog.tick(n);
        Ok(())
    })?;
    while filled <= PREFIX_BUCKETS {
        starts[filled] = n as u32;
        filled += 1;
    }

    let index = KeyIndex { chunks, n, starts };
    eprintln!(
        "   Índice: {} chaves em RAM ({:.2} GB) | {:.1}s{}",
        n,
        index.ram_bytes() as f64 / 1e9,
        t.elapsed().as_secs_f64(),
        if skipped > 0 { format!(" | {} chaves != 32 B ignoradas", skipped) } else { String::new() }
    );
    Ok((index, skipped))
}

// ───────────────────────────── grafo por varredura ─────────────────────────────

struct Window {
    first: usize,
    keys: Vec<[u8; HASH_LEN]>,
    data: Vec<u8>,
    ends: Vec<usize>,
}

impl Window {
    fn new(first: usize) -> Self {
        Window { first, keys: Vec::new(), data: Vec::new(), ends: Vec::new() }
    }
}

/// Varredura 2: monta o grafo CSR. O leitor (1 thread) só lê do RocksDB; o parse dos valores e os
/// lookups rodam em paralelo (rayon) sobre janelas, na ordem, enquanto o leitor já lê a próxima.
pub fn build_graph_scan(src: &dyn EntrySource, index: &KeyIndex) -> Result<Graph> {
    let t = Instant::now();
    let prog = Progress::new("varredura 2: grafo");
    let n = index.len();
    let (tx, rx) = sync_channel::<Window>(QUEUE_DEPTH);

    let mut offsets: Vec<u64> = Vec::with_capacity(n + 1);
    offsets.push(0);
    let mut edges: Vec<u32> = Vec::new();
    let mut acc = 0u64;
    let mut seen = 0usize;
    let mut next_log = 10_000_000usize;

    let outcome: Result<()> = std::thread::scope(|s| {
        let reader = s.spawn(move || -> Result<()> {
            let mut win = Window::new(0);
            src.scan(&mut |k, v| {
                if k.len() != HASH_LEN {
                    return Ok(());
                }
                win.keys.push(<[u8; HASH_LEN]>::try_from(k).expect("32 bytes"));
                win.data.extend_from_slice(v);
                win.ends.push(win.data.len());
                if win.keys.len() >= WIN_ENTRIES || win.data.len() >= WIN_BYTES {
                    let next = win.first + win.keys.len();
                    let full = std::mem::replace(&mut win, Window::new(next));
                    tx.send(full).map_err(|_| anyhow!("consumidor encerrou antes do fim da varredura"))?;
                }
                Ok(())
            })?;
            if !win.keys.is_empty() {
                tx.send(win).map_err(|_| anyhow!("consumidor encerrou antes do fim da varredura"))?;
            }
            Ok(())
        });

        let consumed: Result<()> = (|| {
            for win in rx.iter() {
                if win.first != seen {
                    bail!("Janela fora de ordem (esperava ordinal {}, veio {})", seen, win.first);
                }
                let m = win.keys.len();
                let subs: Vec<(usize, usize)> =
                    (0..m).step_by(SUB_ENTRIES).map(|a| (a, (a + SUB_ENTRIES).min(m))).collect();
                let parts: Vec<(Vec<u32>, Vec<u32>)> = subs
                    .par_iter()
                    .map(|&(a, b)| -> Result<(Vec<u32>, Vec<u32>)> {
                        let mut counts: Vec<u32> = Vec::with_capacity(b - a);
                        let mut es: Vec<u32> = Vec::new();
                        let mut buf: Vec<[u8; HASH_LEN]> = Vec::with_capacity(32);
                        let mut bucket = index.bucket_of(win.first + a);
                        for i in a..b {
                            let ord = win.first + i;
                            bucket = index.advance_bucket(bucket, ord);
                            if !index.matches(bucket, ord, &win.keys[i]) {
                                bail!(
                                    "A varredura 2 divergiu da varredura 1 no ordinal {}: o DB mudou entre as varreduras?",
                                    ord
                                );
                            }
                            let start = if i == 0 { 0 } else { win.ends[i - 1] };
                            let value = &win.data[start..win.ends[i]];
                            buf.clear();
                            extract_child_hashes(value, &mut buf);
                            let before = es.len();
                            for c in &buf {
                                if let Some(co) = index.lookup(c) {
                                    es.push(co);
                                }
                            }
                            counts.push((es.len() - before) as u32);
                        }
                        Ok((counts, es))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for (counts, es) in parts {
                    for c in counts {
                        acc += c as u64;
                        offsets.push(acc);
                    }
                    edges.extend_from_slice(&es);
                }
                seen += m;
                if seen >= next_log {
                    prog.print(seen);
                    next_log += 10_000_000;
                }
            }
            Ok(())
        })();
        // se o consumidor falhou, soltar o receptor destrava o leitor (send passa a dar Err)
        drop(rx);
        let read_res = reader.join().map_err(|_| anyhow!("thread leitora entrou em pânico"))?;
        consumed?;
        read_res
    });
    outcome?;

    if seen != n {
        bail!("A varredura 2 viu {} entradas e a varredura 1 viu {}: o DB mudou entre as varreduras?", seen, n);
    }
    eprintln!(
        "   Grafo: {} nós, {} arestas | {:.2} GB em RAM | {:.1}s",
        n,
        edges.len(),
        (offsets.len() * 8 + edges.len() * 4) as f64 / 1e9,
        t.elapsed().as_secs_f64()
    );
    Ok(Graph::from_parts(offsets, edges))
}

// ───────────────────────────── poda por varredura ─────────────────────────────

/// Varredura 3: copia para o destino só as entradas vivas, em ordem. Devolve quantas copiou.
pub fn prune_scan(
    src: &dyn EntrySource,
    index: &KeyIndex,
    live: &LiveSet,
    sink: &mut dyn EntrySink,
) -> Result<u64> {
    let t = Instant::now();
    let (tx, rx) = sync_channel::<KvBatch>(QUEUE_DEPTH);
    let mut written = 0u64;

    let outcome: Result<()> = std::thread::scope(|s| {
        let reader = s.spawn(move || -> Result<()> {
            let mut ord = 0usize;
            let mut bucket = 0usize;
            let mut batch = KvBatch::default();
            src.scan(&mut |k, v| {
                if k.len() != HASH_LEN {
                    return Ok(());
                }
                if ord >= index.len() {
                    bail!("A varredura 3 viu mais entradas que a varredura 1 (ordinal {}): o DB mudou entre as varreduras?", ord);
                }
                bucket = index.advance_bucket(bucket, ord);
                let key: &[u8; HASH_LEN] = k.try_into().expect("32 bytes");
                if !index.matches(bucket, ord, key) {
                    bail!("A varredura 3 divergiu da varredura 1 no ordinal {}: o DB mudou entre as varreduras?", ord);
                }
                if live.get(ord as u64) {
                    batch.push(k, v);
                    if batch.len() >= OUT_BATCH_ENTRIES || batch.bytes() >= OUT_BATCH_BYTES {
                        let full = std::mem::take(&mut batch);
                        tx.send(full).map_err(|_| anyhow!("destino encerrou antes do fim da varredura"))?;
                    }
                }
                ord += 1;
                Ok(())
            })?;
            if ord != index.len() {
                bail!("A varredura 3 viu {} entradas e a varredura 1 viu {}", ord, index.len());
            }
            if !batch.is_empty() {
                tx.send(batch).map_err(|_| anyhow!("destino encerrou antes do fim da varredura"))?;
            }
            Ok(())
        });

        let consumed: Result<()> = (|| {
            for batch in rx.iter() {
                sink.write(&batch)?;
                written += batch.len() as u64;
            }
            Ok(())
        })();
        drop(rx);
        let read_res = reader.join().map_err(|_| anyhow!("thread leitora entrou em pânico"))?;
        consumed?;
        read_res
    });
    outcome?;

    if written != live.count_ones() {
        bail!("Copiou {} entradas, mas o BFS marcou {} vivas", written, live.count_ones());
    }
    eprintln!("   Poda: {} entradas copiadas | {:.1}s", written, t.elapsed().as_secs_f64());
    Ok(written)
}

// ───────────────────────────── pipeline completo ─────────────────────────────

pub struct ScanGcStats {
    pub total: usize,
    pub live: u64,
    pub deleted: u64,
    #[allow(dead_code)]
    pub skipped_keys: u64,
    #[allow(dead_code)]
    pub edges: usize,
    pub secs_index: f64,
    pub secs_graph: f64,
    pub secs_bfs: f64,
    pub secs_prune: f64,
    #[allow(dead_code)]
    pub live_set: LiveSet,
}

/// Índice -> grafo -> BFS -> poda. `sink` recebe só os nós vivos, em ordem de chave.
pub fn run_scan_gc(
    src: &dyn EntrySource,
    roots: &[[u8; HASH_LEN]],
    sink: &mut dyn EntrySink,
) -> Result<ScanGcStats> {
    eprintln!("🧹 GC por varredura (sem arquivo de export)");

    let t = Instant::now();
    let (index, skipped_keys) = build_key_index(src)?;
    let secs_index = t.elapsed().as_secs_f64();

    let t = Instant::now();
    let graph = build_graph_scan(src, &index)?;
    let secs_graph = t.elapsed().as_secs_f64();
    let edges = graph.edge_count();

    let t = Instant::now();
    let mut root_ords: Vec<u32> = Vec::new();
    for r in roots {
        match index.lookup(r) {
            Some(o) => root_ords.push(o),
            None => eprintln!("   ⚠️ raiz {}… não existe no states/", hex8(r)),
        }
    }
    if root_ords.is_empty() {
        // sem raiz o BFS não marcaria nada e a poda apagaria o states/ inteiro
        bail!("Nenhuma raiz encontrada no states/: abortando antes de podar");
    }
    let live_set = bfs_from_ordinals(index.len(), &root_ords, &graph);
    let secs_bfs = t.elapsed().as_secs_f64();
    drop(graph); // libera a RAM do grafo antes da varredura 3

    let live = live_set.count_ones();
    let total = index.len();
    eprintln!("✅ {} nós vivos de {} ({} mortos)", live, total, total as u64 - live);

    let t = Instant::now();
    prune_scan(src, &index, &live_set, sink)?;
    sink.finish()?;
    let secs_prune = t.elapsed().as_secs_f64();

    Ok(ScanGcStats {
        total,
        live,
        deleted: total as u64 - live,
        skipped_keys,
        edges,
        secs_index,
        secs_graph,
        secs_bfs,
        secs_prune,
        live_set,
    })
}

fn hex8(h: &[u8; HASH_LEN]) -> String {
    h[..8].iter().map(|b| format!("{:02x}", b)).collect()
}

// ───────────────────────────── testes ─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc_index::{build_index_from_export, index_path_for, run_gc_filter_graph};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn value_of(n: &Node) -> Vec<u8> {
        let mut v = b"d1:t1:n".to_vec();
        for c in &n.children {
            v.extend_from_slice(b"32:");
            v.extend_from_slice(c);
        }
        v
    }

    /// entradas ordenadas por chave, como o RocksDB entrega
    fn sorted_entries(nodes: &[Node]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut e: Vec<(Vec<u8>, Vec<u8>)> = nodes.iter().map(|n| (n.key.to_vec(), value_of(n))).collect();
        e.sort();
        e
    }

    struct VecSource(Vec<(Vec<u8>, Vec<u8>)>);

    impl EntrySource for VecSource {
        fn scan(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
            for (k, v) in &self.0 {
                f(k, v)?;
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemSink {
        map: BTreeMap<Vec<u8>, Vec<u8>>,
        finished: bool,
    }

    impl EntrySink for MemSink {
        fn write(&mut self, b: &KvBatch) -> Result<()> {
            let mut last: Option<Vec<u8>> = None;
            for (k, v) in b.iter() {
                if let Some(l) = &last {
                    assert!(l.as_slice() < k, "chaves fora de ordem dentro do lote");
                }
                last = Some(k.to_vec());
                assert!(self.map.insert(k.to_vec(), v.to_vec()).is_none(), "chave escrita duas vezes");
            }
            Ok(())
        }

        fn finish(&mut self) -> Result<()> {
            self.finished = true;
            Ok(())
        }
    }

    fn reference_reachable(nodes: &[Node], roots: &[H]) -> HashSet<H> {
        let by: BTreeMap<H, &Node> = nodes.iter().map(|n| (n.key, n)).collect();
        let mut seen = HashSet::new();
        let mut stack: Vec<H> = roots.to_vec();
        while let Some(k) = stack.pop() {
            if let Some(n) = by.get(&k) {
                if seen.insert(k) {
                    stack.extend(n.children.iter().copied());
                }
            }
        }
        seen
    }

    fn run(nodes: &[Node], roots: &[H]) -> (ScanGcStats, MemSink) {
        let src = VecSource(sorted_entries(nodes));
        let mut sink = MemSink::default();
        let st = run_scan_gc(&src, roots, &mut sink).unwrap();
        assert!(sink.finished);
        (st, sink)
    }

    fn chain(n: u64) -> Vec<Node> {
        (0..n).map(|i| Node { key: h(i), children: if i + 1 < n { vec![h(i + 1)] } else { vec![] } }).collect()
    }

    #[test]
    fn chains_of_any_depth_keep_every_live_node() {
        for depth in [1u64, 2, 19, 64, 500] {
            let nodes = chain(depth);
            let (st, sink) = run(&nodes, &[h(0)]);
            assert_eq!(st.live, depth);
            assert_eq!(sink.map.len() as u64, depth);
            assert_eq!(st.deleted, 0);
        }
    }

    #[test]
    fn garbage_dangling_refs_and_odd_keys() {
        let a = Node { key: h(1), children: vec![h(2), h(999)] }; // h(999) não existe
        let b = Node { key: h(2), children: vec![] };
        let g = Node { key: h(3), children: vec![h(2)] }; // ninguém aponta para ele
        let nodes = vec![g, b, a];
        let mut entries = sorted_entries(&nodes);
        entries.push((b"curta".to_vec(), b"x".to_vec())); // chave != 32 B: ignorada
        entries.sort();
        let src = VecSource(entries);
        let mut sink = MemSink::default();
        let st = run_scan_gc(&src, &[h(1)], &mut sink).unwrap();
        assert_eq!(st.skipped_keys, 1);
        assert_eq!(st.total, 3);
        assert_eq!(st.live, 2);
        let kept: HashSet<H> = sink.map.keys().map(|k| <H>::try_from(k.as_slice()).unwrap()).collect();
        assert_eq!(kept, HashSet::from([h(1), h(2)]));
        // os valores chegam intactos
        assert_eq!(sink.map[&h(1).to_vec()], value_of(&Node { key: h(1), children: vec![h(2), h(999)] }));
    }

    #[test]
    fn missing_root_aborts_before_pruning_anything() {
        let nodes = chain(10);
        let src = VecSource(sorted_entries(&nodes));
        let mut sink = MemSink::default();
        let err = run_scan_gc(&src, &[h(12345)], &mut sink).err().expect("deveria falhar");
        assert!(err.to_string().contains("Nenhuma raiz"), "{err}");
        assert!(sink.map.is_empty(), "nada pode ser escrito sem raiz");
    }

    /// 200 mil nós = vários janelas e sub-tarefas. Confere: vivos == alcançáveis (DFS de referência),
    /// o que foi escrito == vivos, e o bitset é IDÊNTICO ao do pipeline antigo (export + índice em arquivo).
    #[test]
    fn large_random_graph_matches_reference_and_old_pipeline() {
        let mut s: u64 = 0x0DD_BA11_5EED_1234;
        let mut rnd = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let n = 200_000u64;
        let mut nodes: Vec<Node> = (0..n).map(|i| Node { key: h(i), children: vec![] }).collect();
        for i in 0..n as usize {
            for _ in 0..(rnd() % 4) {
                let c = if rnd() % 10 == 0 { h(n + rnd() % 50) } else { h(rnd() % n) };
                nodes[i].children.push(c);
            }
        }
        let roots = vec![h(0), h(rnd() % n), h(rnd() % n)];
        let (st, sink) = run(&nodes, &roots);

        let expected = reference_reachable(&nodes, &roots);
        assert!(expected.len() > 1_000, "grafo de teste degenerado");
        let kept: HashSet<H> = sink.map.keys().map(|k| <H>::try_from(k.as_slice()).unwrap()).collect();
        assert_eq!(kept, expected);
        assert_eq!(st.live as usize, expected.len());

        // pipeline antigo, mesmo conteúdo
        let dir = tempfile::tempdir().unwrap();
        let exp = dir.path().join("export.bin");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&exp).unwrap();
            for (k, v) in sorted_entries(&nodes) {
                f.write_all(&k).unwrap();
                f.write_all(&(v.len() as u32).to_le_bytes()).unwrap();
                f.write_all(&v).unwrap();
            }
        }
        let idx = index_path_for(&exp);
        build_index_from_export(&exp, &idx).unwrap();
        let bits = dir.path().join("old.bits");
        run_gc_filter_graph(&exp, &idx, roots.clone(), &bits).unwrap();
        let old = LiveSet::load(&bits).unwrap();
        assert_eq!(old.len(), st.live_set.len());
        for o in 0..old.len() {
            assert_eq!(old.get(o), st.live_set.get(o), "bit {o} difere do pipeline antigo");
        }
    }

    /// Fonte que na 2ª varredura em diante troca um valor: tem que dar erro, sem travar.
    struct Flaky {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        scans: AtomicUsize,
        change_from_scan: usize,
        change_at: usize,
    }

    impl EntrySource for Flaky {
        fn scan(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
            let this = self.scans.fetch_add(1, Ordering::SeqCst) + 1;
            for (i, (k, v)) in self.entries.iter().enumerate() {
                if this >= self.change_from_scan && i == self.change_at {
                    // a chave muda (dentro dos 15 primeiros bytes): o ordinal deixa de bater com o índice
                    let mut k2 = k.clone();
                    k2[5] ^= 0x01;
                    f(&k2, v)?;
                } else {
                    f(k, v)?;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn db_changing_between_scans_is_an_error_not_a_hang() {
        let nodes = chain(300_000); // bem mais que a fila (3 janelas de 32 mil)
        let entries = sorted_entries(&nodes);
        for (from, label) in [(2usize, "varredura 2"), (3usize, "varredura 3")] {
            let src = Flaky {
                entries: entries.clone(),
                scans: AtomicUsize::new(0),
                change_from_scan: from,
                change_at: 150_000,
            };
            let mut sink = MemSink::default();
            let err = run_scan_gc(&src, &[h(0)], &mut sink).err().unwrap_or_else(|| panic!("{label}: deveria falhar"));
            assert!(err.to_string().contains("divergiu") || err.to_string().contains("mudou"), "{label}: {err}");
        }
    }

    struct FailingSink;

    impl EntrySink for FailingSink {
        fn write(&mut self, _b: &KvBatch) -> Result<()> {
            bail!("disco cheio (simulado)")
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn sink_error_propagates_without_deadlock() {
        let nodes = chain(300_000);
        let src = VecSource(sorted_entries(&nodes));
        let err = run_scan_gc(&src, &[h(0)], &mut FailingSink).err().expect("deveria falhar");
        assert!(err.to_string().contains("disco cheio"), "{err}");
    }

    #[test]
    fn null_sink_counts_only_live_entries() {
        let nodes = chain(1_000);
        let src = VecSource(sorted_entries(&nodes));
        let mut sink = NullSink::default();
        let st = run_scan_gc(&src, &[h(0)], &mut sink).unwrap();
        assert_eq!(sink.entries, 1_000);
        assert_eq!(st.live, 1_000);
    }

    #[test]
    fn key_index_lookup_hits_all_and_misses_absent() {
        let nodes = chain(50_000);
        let src = VecSource(sorted_entries(&nodes));
        let (idx, skipped) = build_key_index(&src).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(idx.len(), 50_000);
        for (o, (k, _)) in src.0.iter().enumerate() {
            let k: H = k.as_slice().try_into().unwrap();
            assert_eq!(idx.lookup(&k), Some(o as u32));
        }
        for i in 0..2_000u64 {
            assert_eq!(idx.lookup(&h(1_000_000 + i)), None);
        }
    }
}
