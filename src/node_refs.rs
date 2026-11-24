//! Referências de um nó do trie lidas pela ESTRUTURA Bencodex, não por busca de bytes.
//!
//! `extract_child_hashes` procura os bytes `32:` em qualquer lugar do valor e pula 35 bytes a cada
//! acerto. Um acerto falso (os bytes `32:` dentro de um texto, de um nó embutido, de um binário
//! maior como `132:`) pula 35 bytes e pode engolir uma referência de verdade logo depois. Aqui o
//! registro é percorrido como Bencodex, e só os lugares onde o formato PREVÊ um hash contam.
//!
//! Formato assumido (Libplanet, trie Merkle Patricia), cada registro é uma lista Bencodex:
//!   - 17 itens: FullNode. Itens 0..15 são os filhos e o 16 é o valor; cada um é `n` (nulo), um
//!     binário de 32 bytes (hash de um nó guardado à parte) ou uma lista (nó embutido, sem refs).
//!   - 2 itens: ShortNode. Item 0 = caminho (NUNCA é referência, mesmo com 32 bytes), item 1 = filho.
//!   - 1 item: ValueNode (folha). O conteúdo é o estado e não tem referências de trie.
//!   - qualquer outra coisa: `Unknown` (o chamador decide; o GC une com a busca de bytes).
//!
//! Esse formato é a minha leitura do Libplanet. Quem confere é a contagem de `Unknown` e a
//! comparação com a busca de bytes no DB real (ver `RefStats` em `gc_scan`).

use anyhow::Context;
use crate::io_util::HASH_LEN;

/// Hash de uma subárvore/trie vazia no Libplanet: `SHA256(b"n")`.
/// Árvores vazias não possuem nós gravados no RocksDB (Root é virtualmente null).
pub const EMPTY_ROOT_HASH: [u8; HASH_LEN] = [
    0x1b, 0x16, 0xb1, 0xdf, 0x53, 0x8b, 0xa1, 0x2d,
    0xc3, 0xf9, 0x7e, 0xdb, 0xb8, 0x5c, 0xaa, 0x70,
    0x50, 0xd4, 0x6c, 0x14, 0x81, 0x34, 0x29, 0x0f,
    0xeb, 0xa8, 0x0f, 0x82, 0x36, 0xc8, 0x3d, 0xb9,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeShape {
    Full,
    Short,
    Value,
    Unknown,
}

const MAX_DEPTH: usize = 128;
const MAX_ITEMS: usize = 17;

/// Lê `<dígitos>:` a partir de `pos`. Devolve (tamanho, posição depois dos dois pontos).
fn parse_len(d: &[u8], pos: usize) -> Option<(usize, usize)> {
    let mut p = pos;
    let mut n: usize = 0;
    while let Some(&c) = d.get(p) {
        if !c.is_ascii_digit() {
            break;
        }
        if p - pos >= 12 {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((c - b'0') as usize)?;
        p += 1;
    }
    if p == pos || *d.get(p)? != b':' {
        return None;
    }
    Some((n, p + 1))
}

fn skip_len_prefixed(d: &[u8], pos: usize) -> Option<usize> {
    let (len, after) = parse_len(d, pos)?;
    let end = after.checked_add(len)?;
    if end > d.len() {
        None
    } else {
        Some(end)
    }
}

/// Pula um valor Bencodex que começa em `pos`. `None` se estiver malformado ou fundo demais.
fn skip_value(d: &[u8], pos: usize, depth: usize) -> Option<usize> {
    if depth > MAX_DEPTH {
        return None;
    }
    match *d.get(pos)? {
        b'n' | b't' | b'f' => Some(pos + 1),
        b'i' => {
            let mut p = pos + 1;
            if *d.get(p)? == b'-' {
                p += 1;
            }
            let start = p;
            while d.get(p)?.is_ascii_digit() {
                p += 1;
            }
            if p == start || *d.get(p)? != b'e' {
                return None;
            }
            Some(p + 1)
        }
        b'l' | b'd' => {
            let mut p = pos + 1;
            loop {
                if *d.get(p)? == b'e' {
                    return Some(p + 1);
                }
                p = skip_value(d, p, depth + 1)?;
            }
        }
        b'u' => skip_len_prefixed(d, pos + 1),
        b'0'..=b'9' => skip_len_prefixed(d, pos),
        _ => None,
    }
}

/// Acrescenta em `out` os hashes que o nó referencia e devolve o formato. Se o formato for
/// `Unknown`, `out` não é tocado.
pub fn structural_refs(d: &[u8], out: &mut Vec<[u8; HASH_LEN]>) -> NodeShape {
    if d.first() != Some(&b'l') {
        return NodeShape::Unknown;
    }
    let mut items: [(usize, usize); MAX_ITEMS] = [(0, 0); MAX_ITEMS];
    let mut n = 0usize;
    let mut pos = 1usize;
    loop {
        match d.get(pos) {
            None => return NodeShape::Unknown,
            Some(&b'e') => break,
            Some(_) => {
                let Some(end) = skip_value(d, pos, 1) else {
                    return NodeShape::Unknown;
                };
                if n == MAX_ITEMS {
                    return NodeShape::Unknown;
                }
                items[n] = (pos, end);
                n += 1;
                pos = end;
            }
        }
    }
    if pos + 1 != d.len() {
        return NodeShape::Unknown; // sobrou coisa depois do fim da lista
    }

    // um item é referência se for um binário de exatamente 32 bytes: `32:` + 32 bytes
    let hash_at = |i: usize| -> Option<[u8; HASH_LEN]> {
        let (s, e) = items[i];
        if e - s == 3 + HASH_LEN && &d[s..s + 3] == b"32:" {
            <[u8; HASH_LEN]>::try_from(&d[s + 3..e]).ok()
        } else {
            None
        }
    };

    match n {
        17 => {
            for i in 0..17 {
                if let Some(h) = hash_at(i) {
                    out.push(h);
                }
            }
            NodeShape::Full
        }
        2 => {
            // No Libplanet (NodeDecoder.cs):
            // Se o item 0 for Null ('n') -> é ValueNode: new List(Null.Value, Value)
            //   O Value pode ser Binary(32 bytes) = raiz de um sub-trie de conta (account subtrie root)
            //   Neste caso o hash DOS 32 bytes é uma referência ao sub-trie e DEVE ser seguido.
            // Se o item 0 for Binary ('0'..='9') -> é ShortNode: new List(Binary(Key), Child)
            let (first_start, _) = items[0];
            match d.get(first_start) {
                Some(b'n') => {
                    // ValueNode: o item 1 é o valor. Se for Binary de exatamente 32 bytes,
                    // é o hash raiz de um sub-trie de conta – deve ser tratado como referência.
                    if let Some(h) = hash_at(1) {
                        out.push(h);
                    }
                    NodeShape::Value
                }
                Some(b'0'..=b'9') => {
                    if let Some(h) = hash_at(1) {
                        out.push(h);
                    }
                    NodeShape::Short
                }
                _ => NodeShape::Unknown,
            }
        }
        1 => NodeShape::Value,
        _ => NodeShape::Unknown,
    }
}

/// Extrai hashes de referências do nó do trie.
/// Se o nó tiver formato conhecido (FullNode ou ShortNode), extrai estritamente pela estrutura Bencodex.
/// Se for ValueNode com value = Binary(32 bytes), extrai o hash do sub-trie de conta (account subtrie root).
/// Se for Unknown (formato não reconhecido), faz fallback para busca de bytes.
pub fn extract_trie_refs(d: &[u8], out: &mut Vec<[u8; HASH_LEN]>) -> NodeShape {
    let shape = structural_refs(d, out);
    if shape == NodeShape::Unknown {
        crate::gc_filter::extract_child_hashes(d, out);
    }
    shape
}

#[derive(Debug, Default, Clone)]
pub struct TrieVerifyStats {
    pub nodes_visited: u64,
    pub full_nodes: u64,
    pub short_nodes: u64,
    pub value_nodes: u64,
    pub unknown_nodes: u64,
    pub max_depth: usize,
}

/// Caminha pelo banco `states/` (ou `states_gc/`) a partir de `root_hash` usando o parser estrutural
/// e confirma que 100% dos nós referenciados existem no RocksDB.
///
/// Se qualquer filho estiver ausente, aborta com erro indicando o hash do pai e do filho faltante.
pub fn verify_state_trie_completeness(
    db: &rocksdb::DB,
    root_hash: &[u8; HASH_LEN],
) -> anyhow::Result<TrieVerifyStats> {
    use rustc_hash::FxHashSet;
    use std::collections::VecDeque;

    let mut stats = TrieVerifyStats::default();
    let mut visited: FxHashSet<[u8; HASH_LEN]> = FxHashSet::default();
    let mut queue: VecDeque<([u8; HASH_LEN], usize)> = VecDeque::new();
    let mut children_buf: Vec<[u8; HASH_LEN]> = Vec::with_capacity(17);

    // Checa a raiz
    if db.get_pinned(root_hash)?.is_none() {
        anyhow::bail!("Nó raiz do estado não encontrado no banco: {:02x?}", root_hash);
    }

    visited.insert(*root_hash);
    queue.push_back((*root_hash, 0));

    while let Some((node_hash, depth)) = queue.pop_front() {
        stats.nodes_visited += 1;
        if depth > stats.max_depth {
            stats.max_depth = depth;
        }

        let val_pinned = db
            .get_pinned(&node_hash)?
            .ok_or_else(|| anyhow::anyhow!("Nó ausente no banco de estados: {:02x?}", node_hash))?;

        children_buf.clear();
        let shape = extract_trie_refs(&val_pinned, &mut children_buf);
        match shape {
            NodeShape::Full => stats.full_nodes += 1,
            NodeShape::Short => stats.short_nodes += 1,
            NodeShape::Value => stats.value_nodes += 1,
            NodeShape::Unknown => stats.unknown_nodes += 1,
        }

        for &child in &children_buf {
            // No Libplanet, EmptyRootHash (SHA256 de 'n') representa uma subárvore/conta vazia.
            // Ela não possui nós gravados no RocksDB e não deve ser expandida.
            if child == EMPTY_ROOT_HASH {
                continue;
            }

            if visited.insert(child) {
                // Confirma que o filho referenciado existe no RocksDB
                if db.get_pinned(&child)?.is_none() {
                    anyhow::bail!(
                        "Inconsistência no Merkle Trie: filho {:02x?} referenciado pelo nó {:02x?} (profundidade {}) NÃO existe no banco!",
                        child,
                        node_hash,
                        depth
                    );
                }
                queue.push_back((child, depth + 1));
            }
        }

        if stats.nodes_visited % 5_000_000 == 0 {
            eprintln!(
                "   [verificação trie] {}M nós verificados | fila: {} | profundidade max: {}",
                stats.nodes_visited / 1_000_000,
                queue.len(),
                stats.max_depth
            );
        }
    }

    Ok(stats)
}

/// Abre `states_path` em modo somente leitura (format_version 5) e verifica a completude
/// do trie a partir do hash da raiz.
///
/// Se o banco tiver arquivos WAL ativos (nó em execução), abre em modo secondary + catch_up
/// para ver os dados mais recentes. Caso contrário, abre em modo read-only puro.
pub fn verify_state_trie_at_path(
    states_path: &std::path::Path,
    root_hash: &[u8; HASH_LEN],
) -> anyhow::Result<TrieVerifyStats> {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(false);
    opts.set_max_open_files(500);
    opts.set_paranoid_checks(false);
    opts.set_skip_stats_update_on_db_open(true);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_format_version(5);
    opts.set_block_based_table_factory(&block_opts);

    // Se tiver WAL ativo (nó em execução), usa secondary mode para ver dados recentes.
    // Caso contrário, abre read-only (checkpoints/estados GC não têm WAL ativo).
    let has_wal = std::fs::read_dir(states_path)
        .ok()
        .map(|dir| dir.flatten().any(|e| {
            e.path().extension().and_then(|x| x.to_str()) == Some("log")
        }))
        .unwrap_or(false);

    if has_wal {
        // Secondary mode: lê SST + replay WAL para ver dados do nó em execução
        let temp_dir = tempfile::tempdir()
            .context("Falha ao criar diretório temporário para secondary mode")?;
        let db = rocksdb::DB::open_as_secondary(&opts, states_path, temp_dir.path())
            .with_context(|| format!("Falha ao abrir states em modo secondary: {}", states_path.display()))?;
        db.try_catch_up_with_primary()
            .context("Falha ao sincronizar com primary (WAL catch-up)")?;
        verify_state_trie_completeness(&db, root_hash)
    } else {
        // Read-only puro: para checkpoints e estados GC (sem WAL ativo)
        let db = rocksdb::DB::open_for_read_only(&opts, states_path, false)
            .with_context(|| format!("Falha ao abrir states em {}", states_path.display()))?;
        verify_state_trie_completeness(&db, root_hash)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::gc_filter::extract_child_hashes;

    pub fn bin(b: &[u8]) -> Vec<u8> {
        let mut v = format!("{}:", b.len()).into_bytes();
        v.extend_from_slice(b);
        v
    }

    pub fn text(s: &str) -> Vec<u8> {
        let mut v = format!("u{}:", s.len()).into_bytes();
        v.extend_from_slice(s.as_bytes());
        v
    }

    pub fn list(items: &[Vec<u8>]) -> Vec<u8> {
        let mut v = vec![b'l'];
        for i in items {
            v.extend_from_slice(i);
        }
        v.push(b'e');
        v
    }

    /// FullNode com os filhos dados (índice -> item já codificado); o resto é nulo.
    pub fn full_node(children: &[(usize, Vec<u8>)], value: Option<Vec<u8>>) -> Vec<u8> {
        let mut items: Vec<Vec<u8>> = (0..17).map(|_| b"n".to_vec()).collect();
        for (i, c) in children {
            items[*i] = c.clone();
        }
        if let Some(v) = value {
            items[16] = v;
        }
        list(&items)
    }

    pub fn short_node(path: &[u8], child: Vec<u8>) -> Vec<u8> {
        list(&[bin(path), child])
    }

    pub fn value_node(inner: Vec<u8>) -> Vec<u8> {
        list(&[b"n".to_vec(), inner])
    }

    fn hh(n: u8) -> [u8; 32] {
        let mut h = [n; 32];
        h[0] = 0xA0 ^ n; // nunca ':' nem dígito no começo
        h
    }

    #[test]
    fn full_node_refs_come_from_children_and_value_slot() {
        let rec = full_node(&[(0, bin(&hh(1))), (7, bin(&hh(2))), (15, bin(&hh(3)))], Some(bin(&hh(4))));
        let mut out = Vec::new();
        assert_eq!(structural_refs(&rec, &mut out), NodeShape::Full);
        assert_eq!(out, vec![hh(1), hh(2), hh(3), hh(4)]);
    }

    #[test]
    fn short_node_path_is_never_a_reference_even_with_32_bytes() {
        let rec = short_node(&hh(9), bin(&hh(5)));
        let mut out = Vec::new();
        assert_eq!(structural_refs(&rec, &mut out), NodeShape::Short);
        assert_eq!(out, vec![hh(5)], "o caminho de 32 bytes não é referência");
        // a busca de bytes erra aqui: toma o caminho por filho
        let mut heur = Vec::new();
        extract_child_hashes(&rec, &mut heur);
        assert!(heur.contains(&hh(9)));
    }

    #[test]
    fn value_node_with_non_hash_content_has_no_refs() {
        // ValueNode cujo valor NÃO é Binary de 32 bytes (ex: lista, texto) → sem refs
        let rec = value_node(list(&[bin(&hh(7)), text("x")]));
        let mut out = Vec::new();
        assert_eq!(structural_refs(&rec, &mut out), NodeShape::Value);
        assert!(out.is_empty());
        let mut heur = Vec::new();
        extract_child_hashes(&rec, &mut heur);
        assert!(!heur.is_empty(), "a busca de bytes acharia uma 'referência' falsa na folha");
    }

    #[test]
    fn value_node_with_32byte_binary_value_is_account_subtrie_ref() {
        // ValueNode cujo valor É Binary de 32 bytes → é hash raiz de sub-trie de conta, deve ser seguido
        let rec = value_node(bin(&hh(7)));
        let mut out = Vec::new();
        assert_eq!(structural_refs(&rec, &mut out), NodeShape::Value);
        assert_eq!(out, vec![hh(7)], "o hash de 32 bytes do ValueNode é referência de sub-trie");
    }

    #[test]
    fn embedded_child_has_no_refs_and_does_not_hide_the_next_real_one() {
        // filho 0 embutido contendo os bytes `32:` perto do fim; filho 1 é uma referência de verdade
        let inline = value_node(text("aaaa32:b"));
        let rec = full_node(&[(0, inline), (1, bin(&hh(6)))], None);
        let mut out = Vec::new();
        assert_eq!(structural_refs(&rec, &mut out), NodeShape::Full);
        assert_eq!(out, vec![hh(6)]);
    }

    /// O caso do dev da Planetarium: a busca de bytes casa `32:` dentro do texto, pula 35 bytes e
    /// engole a referência real que vem logo depois. A leitura estrutural não perde.
    #[test]
    fn byte_scan_swallows_a_real_reference_but_structure_does_not() {
        let inline = value_node(text("aaaa32:b"));
        let rec = full_node(&[(0, inline), (1, bin(&hh(6)))], None);

        let mut heur = Vec::new();
        extract_child_hashes(&rec, &mut heur);
        assert!(!heur.contains(&hh(6)), "a busca de bytes deveria ter perdido a referência real");

        let mut out = Vec::new();
        structural_refs(&rec, &mut out);
        assert!(out.contains(&hh(6)), "a leitura estrutural tem que achar");
    }

    #[test]
    fn unknown_shapes_leave_out_untouched() {
        for rec in [
            b"d1:t1:ne".to_vec(),                 // dicionário
            b"n".to_vec(),                        // nulo
            list(&[bin(&hh(1)), bin(&hh(2)), bin(&hh(3))]), // 3 itens
            [list(&[bin(&hh(1))]), b"x".to_vec()].concat(), // sobra depois da lista
            b"l3:abc".to_vec(),                   // lista sem fechar
            Vec::new(),
        ] {
            let mut out = vec![hh(0)];
            assert_eq!(structural_refs(&rec, &mut out), NodeShape::Unknown, "{:?}", rec);
            assert_eq!(out, vec![hh(0)]);
        }
    }

    #[test]
    fn deep_nesting_is_rejected_not_a_stack_overflow() {
        let mut rec = vec![b'l'];
        rec.extend(std::iter::repeat(b'l').take(100_000));
        rec.extend(std::iter::repeat(b'e').take(100_001));
        let mut out = Vec::new();
        assert_eq!(structural_refs(&rec, &mut out), NodeShape::Unknown);
    }

    #[test]
    fn random_garbage_and_mutations_never_panic() {
        let mut s: u64 = 0xC0FFEE_1234_5678;
        let mut rnd = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let base = full_node(&[(0, bin(&hh(1))), (3, value_node(text("zz")))], Some(bin(&hh(2))));
        for _ in 0..200_000 {
            let len = (rnd() % 90) as usize;
            let alphabet = b"lde:niutf0123456789";
            let mut buf: Vec<u8> = (0..len)
                .map(|_| if rnd() % 3 == 0 { rnd() as u8 } else { alphabet[(rnd() % alphabet.len() as u64) as usize] })
                .collect();
            let mut out = Vec::new();
            let _ = structural_refs(&buf, &mut out);
            // mutação de um registro válido
            buf = base.clone();
            let i = (rnd() as usize) % buf.len();
            buf[i] = rnd() as u8;
            if rnd() % 2 == 0 {
                buf.truncate((rnd() as usize) % (buf.len() + 1));
            }
            let _ = structural_refs(&buf, &mut out);
        }
    }

    #[test]
    fn verify_state_trie_succeeds_on_complete_tree_and_fails_on_missing_node() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_states");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, &db_path).unwrap();

        let leaf1_hash = hh(1);
        let leaf1_val = value_node(text("player_state_1"));
        let leaf2_hash = hh(2);
        let leaf2_val = value_node(text("player_state_2"));

        let short_hash = hh(3);
        let short_val = short_node(&[0x12, 0x34], bin(&leaf2_hash));

        let root_hash = hh(4);
        let root_val = full_node(&[(0, bin(&leaf1_hash)), (5, bin(&short_hash))], None);

        // Escreve todos os nós
        db.put(&leaf1_hash, &leaf1_val).unwrap();
        db.put(&leaf2_hash, &leaf2_val).unwrap();
        db.put(&short_hash, &short_val).unwrap();
        db.put(&root_hash, &root_val).unwrap();

        // 1. Árvore completa deve passar com sucesso
        let stats = verify_state_trie_completeness(&db, &root_hash).expect("deveria validar com sucesso");
        assert_eq!(stats.nodes_visited, 4);
        assert_eq!(stats.full_nodes, 1);
        assert_eq!(stats.short_nodes, 1);
        assert_eq!(stats.value_nodes, 2);

        // 2. Se removermos uma folha (leaf2), a verificação tem que falhar e apontar o nó faltante!
        db.delete(&leaf2_hash).unwrap();
        let err = verify_state_trie_completeness(&db, &root_hash).err().expect("deveria falhar com nó ausente");
        assert!(err.to_string().contains("NÃO existe no banco"), "{err}");
    }
}
