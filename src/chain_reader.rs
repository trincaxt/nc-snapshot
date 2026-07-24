//! Lê StateRootHash direto do checkpoint em Rust puro — byte-idêntico ao C#.
//!
//! Caminho do C# (RocksDBStore.cs):
//!   1. chain/ → I + chainId + index → blockHash (32 bytes)
//!   2. block/blockindex → B + blockHash → nome do epoch DB ("epoch20642")
//!   3. block/epochXXXX → B + blockHash → BlockDigest serializado (Bencodex)
//!   4. Decodifica o BlockDigest Bencodex, extrai o header["StateRootHash"]
//!
//! Formato das keys (fonte: Libplanet RocksDBStore.cs):
//!   chain/ DB:        `I` (0x49) + chainId(16) + idx(8BE) → blockHash(32)
//!   block/blockindex: `B` (0x42) + blockHash(32) → epoch DB name (string)
//!   block/epochXXXX:  `B` (0x42) + blockHash(32) → BlockDigest (bencodex dict)
//!
//! BlockDigest Bencodex structure (simplified,ما نحتاجه):
//!   { "header": { ... "stateRootHash": <32 bytes>, ... }, ... }

use anyhow::Context;
use bencodex::{BencodexValue, BencodexKey, decode_borrowed};
use rocksdb::{DB, Options};
use std::path::Path;

const CANONICAL_CHAIN_KEY: u8 = b'C'; // 0x43
const INDEX_KEY_PREFIX: u8 = b'I'; // 0x49
const BLOCK_KEY_PREFIX: u8 = b'B'; // 0x42
const INDEX_KEY_LEN: usize = 1 + 16 + 8; // 25

#[derive(Debug)]
pub struct ChainTip {
    pub state_root_hash: [u8; 32],
    pub block_index: i64,
    #[allow(dead_code)]
    pub block_hash: [u8; 32],
}

/// Informações completas do BlockHeader (para metadata generation).
///
/// Nota: `tx_hash` é `Option` porque o C# BlockMarshaler só inclui TxHash
/// no dicionário Bencodex se `metadata.TxHash != null`:
///   if (metadata.TxHash is { } th) dict = dict.Add(TxHashKey, th.Bencoded);
#[derive(Debug)]
pub struct BlockHeaderInfo {
    pub index: i64,
    pub timestamp: String,
    pub state_root_hash: [u8; 32],
    pub previous_hash: [u8; 32],
    pub tx_hash: Option<[u8; 32]>,  // Opcional: C# só inclui se != null
    #[allow(dead_code)]
    pub block_hash: [u8; 32],
}

fn open_ro(path: &Path) -> anyhow::Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    DB::open_for_read_only(&opts, path, false)
    .with_context(|| format!("Abrindo DB: {}", path.display()))
}

/// Encontra o canonical chain ID (key `C` / 0x43 → GUID 16 bytes).
fn find_chain_id(db: &DB) -> anyhow::Result<Vec<u8>> {
    if let Ok(Some(val)) = db.get(&[CANONICAL_CHAIN_KEY]) {
        if val.len() == 16 {
            return Ok(val.to_vec());
        }
    }

    let candidates: &[&[u8]] = &[
        b"canonical_chain_id",
        b"cid",
        b"c",
    ];

    for key in candidates {
        if let Ok(Some(val)) = db.get(key) {
            if val.len() == 16 {
                return Ok(val.to_vec());
            }
        }
    }

    anyhow::bail!(
        "Canonical chain ID não encontrado (key 'C'/0x43)."
    )
}

/// Parseia uma key de índice `I` + chainId + index.
fn parse_index_key(key: &[u8], val: &[u8]) -> Option<(Vec<u8>, i64, [u8; 32])> {
    if key.len() != INDEX_KEY_LEN || key[0] != INDEX_KEY_PREFIX || val.len() != 32 {
        return None;
    }
    let chain_id = key[1..17].to_vec();
    let idx = i64::from_be_bytes(key[17..25].try_into().ok()?);
    let hash: [u8; 32] = val.try_into().ok()?;
    Some((chain_id, idx, hash))
}

/// Encontra o tip block.
fn find_tip(db: &DB, canonical_chain_id: &[u8]) -> anyhow::Result<(Vec<u8>, i64, [u8; 32])> {
    // Busca pelo prefixo I + canonical chain ID, do fim pro começo
    let mut prefix = vec![INDEX_KEY_PREFIX];
    prefix.extend_from_slice(canonical_chain_id);

    if let Some(tip) = tip_for_prefix(db, &prefix) {
        return Ok(tip);
    }

    // Fallback: itera do fim do DB
    let mut iter = db.raw_iterator();
    iter.seek_to_last();
    while iter.valid() {
        if let Some((chain_id, idx, hash)) = parse_index_key(iter.key().unwrap(), iter.value().unwrap()) {
            return Ok((chain_id, idx, hash));
        }
        iter.prev();
    }

    anyhow::bail!(
        "Tip block não encontrado.\n\
Canonical chain ID: {}.\n\
Use diagnose_db() para verificar.",
                  hex::encode(canonical_chain_id)
    )
}

/// Calcula upper bound lexicográfico para prefix iteration.
fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] != 0xFF {
            upper[i] += 1;
            upper.truncate(i + 1);
            return upper;
        }
    }
    upper.push(0x00);
    upper
}

/// Retorna o tip (maior índice) para um prefixo I+chainId.
fn tip_for_prefix(
    db: &DB,
    prefix: &[u8],
) -> Option<(Vec<u8>, i64, [u8; 32])> {
    let upper = prefix_upper_bound(prefix);
    let mut iter = db.raw_iterator();
    iter.seek_for_prev(&upper);

    while iter.valid() {
        let key = iter.key()?;
        if !key.starts_with(prefix) {
            break;
        }
        if let Some(parsed) = parse_index_key(key, iter.value()?) {
            return Some(parsed);
        }
        iter.prev();
    }
    None
}

/// Busca block hash para um índice específico.
fn block_hash_at(db: &DB, chain_id: &[u8], index: i64) -> anyhow::Result<[u8; 32]> {
    let mut key = vec![INDEX_KEY_PREFIX];
    key.extend_from_slice(chain_id);
    key.extend_from_slice(&index.to_be_bytes());

    match db.get(&key)? {
        Some(val) if val.len() == 32 => Ok(val[..].try_into().expect("32 bytes")),
        Some(val) => anyhow::bail!(
            "Block hash para índice {} tem tamanho inesperado: {}b",
            index,
            val.len()
        ),
        None => anyhow::bail!("Índice {} não encontrado no chain DB", index),
    }
}

/// Lê o nome do epoch DB do blockindex: key = B + blockHash → value = "epochXXXXX" (string)
fn get_block_epoch_db_name(block_index_db: &DB, block_hash: &[u8; 32]) -> anyhow::Result<String> {
    let mut key = vec![BLOCK_KEY_PREFIX];
    key.extend_from_slice(block_hash);

    match block_index_db.get(&key)? {
        Some(val) => {
            // RocksDBStoreBitConverter.GetString — UTF-8 string
            String::from_utf8(val.to_vec())
                .with_context(|| format!("Epoch DB name não é UTF-8 válido para bloco {}", hex::encode(block_hash)))
        }
        None => anyhow::bail!(
            "Bloco {} não encontrado no blockindex DB",
            hex::encode(block_hash)
        ),
    }
}

/// Extrai informações completas do BlockHeader (para metadata generation).
/// 
/// Campos extraídos do header (chaves binárias):
/// - 0x69 ('i'): index (número do bloco)
/// - 0x74 ('t'): timestamp (string ISO 8601)
/// - 0x73 ('s'): stateRootHash (32 bytes)
/// - 0x70 ('p'): previousHash (32 bytes)
/// - 0x78 ('x'): txHash (transaction root hash, 32 bytes)
fn extract_header_info(
    block_db: &DB,
    block_hash: &[u8; 32],
) -> anyhow::Result<BlockHeaderInfo> {
    let mut key = vec![BLOCK_KEY_PREFIX];
    key.extend_from_slice(block_hash);

    let bytes = block_db.get(&key)?
        .with_context(|| format!("BlockDigest não encontrado para bloco {}", hex::encode(block_hash)))?;

    // Decodifica Bencodex
    let value = decode_borrowed(&bytes)
        .map_err(|e| anyhow::anyhow!("Falha ao decodificar BlockDigest Bencodex: {:?}", e))?;

    // Extrai header (key 0x48 = 'H')
    let dict = match value {
        BencodexValue::Dictionary(d) => d,
        _ => anyhow::bail!("BlockDigest não é um dicionário Bencodex"),
    };

    let header_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x48])); // 'H'
    let header = match dict.get(&header_key)
        .with_context(|| "Campo 0x48 (header) não encontrado no BlockDigest")? {
        BencodexValue::Dictionary(d) => d,
        _ => anyhow::bail!("Header (0x48) não é um dicionário"),
    };

    // Extrai cada campo do header
    
    // Index: 0x69 ('i')
    let index_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x69]));
    let index = match header.get(&index_key)
        .with_context(|| "Campo 'index' (0x69) não encontrado")? {
        BencodexValue::Number(n) => n.to_string().parse::<i64>()
            .context("Index não é um i64 válido")?,
        _ => anyhow::bail!("Index (0x69) não é um número"),
    };

    // Timestamp: 0x74 ('t')
    let timestamp_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x74]));
    let timestamp = match header.get(&timestamp_key)
        .with_context(|| "Campo 'timestamp' (0x74) não encontrado")? {
        BencodexValue::Text(s) => s.to_string(),
        _ => anyhow::bail!("Timestamp (0x74) não é texto"),
    };

    // StateRootHash: 0x73 ('s')
    let state_root_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x73]));
    let state_root_bytes = match header.get(&state_root_key)
        .with_context(|| "Campo 'stateRootHash' (0x73) não encontrado")? {
        BencodexValue::Binary(b) => b.as_ref(),
        _ => anyhow::bail!("StateRootHash (0x73) não é binary"),
    };
    if state_root_bytes.len() != 32 {
        anyhow::bail!("StateRootHash tem tamanho inesperado: {}b", state_root_bytes.len());
    }
    let state_root_hash: [u8; 32] = state_root_bytes[..32].try_into().expect("32 bytes");

    // PreviousHash: 0x70 ('p')
    let prev_hash_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x70]));
    let prev_hash_bytes = match header.get(&prev_hash_key)
        .with_context(|| "Campo 'previousHash' (0x70) não encontrado")? {
        BencodexValue::Binary(b) => b.as_ref(),
        _ => anyhow::bail!("PreviousHash (0x70) não é binary"),
    };
    if prev_hash_bytes.len() != 32 {
        anyhow::bail!("PreviousHash tem tamanho inesperado: {}b", prev_hash_bytes.len());
    }
    let previous_hash: [u8; 32] = prev_hash_bytes[..32].try_into().expect("32 bytes");

    // TxHash: 0x78 ('x') — OPCIONAL (C# BlockMarshaler usa TryGetValue)
    // Só incluído se metadata.TxHash não for nulo:
    //   if (metadata.TxHash is { } th) dict = dict.Add(TxHashKey, th.Bencoded);
    // Tratamos silenciosamente como None (sem warning) — é um caso normal
    let tx_hash_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x78]));
    let tx_hash: Option<[u8; 32]> = match header.get(&tx_hash_key) {
        Some(BencodexValue::Binary(b)) if b.len() == 32 => {
            Some(b[..32].try_into().expect("32 bytes"))
        }
        Some(BencodexValue::Binary(b)) => {
            anyhow::bail!("TxHash tem tamanho inesperado: {}b", b.len());
        }
        Some(_) => {
            anyhow::bail!("TxHash (0x78) não é binary");
        }
        None => None, // Silencioso: TxHash é opcional no C# (TryGetValue)
    };

    Ok(BlockHeaderInfo {
        index,
        timestamp,
        state_root_hash,
        previous_hash,
        tx_hash,
        block_hash: *block_hash,
    })
}

/// Lê o StateRootHash do BlockDigest (função original, mais leve).
/// BlockDigest é um dict Bencodex com estrutura:
///   { "header": { "stateRootHash": <32 bytes>, ... }, "txHashes": [...], ... }
fn get_state_root_from_digest(
    block_db: &DB,
    block_hash: &[u8; 32],
) -> anyhow::Result<[u8; 32]> {
    let mut key = vec![BLOCK_KEY_PREFIX];
    key.extend_from_slice(block_hash);

    let bytes = block_db.get(&key)?
        .with_context(|| format!("BlockDigest não encontrado para bloco {}", hex::encode(block_hash)))?;

    // Decodifica Bencodex
    let value = decode_borrowed(&bytes)
        .map_err(|e| anyhow::anyhow!("Falha ao decodificar BlockDigest Bencodex: {:?}", e))?;

    // Extrai header (key 0x48 = 'H')
    // BlockDigest usa CHAVES BINÁRIAS (bytes únicos), não texto!
    let dict = match value {
        BencodexValue::Dictionary(d) => d,
        _ => anyhow::bail!("BlockDigest não é um dicionário Bencodex"),
    };

    let header_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x48])); // 'H'
    let header = match dict.get(&header_key)
        .with_context(|| "Campo 0x48 (header) não encontrado no BlockDigest")? {
        BencodexValue::Dictionary(d) => d,
        _ => anyhow::bail!("Header (0x48) não é um dicionário"),
    };

    let state_root_key = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x73])); // 's' = stateRootHash
    let state_root_bytes = match header.get(&state_root_key)
        .with_context(|| "StateRootHash (0x73) não encontrado no header")? {
        BencodexValue::Binary(b) => b.as_ref(),
        _ => anyhow::bail!("StateRootHash (0x73) não é binary"),
    };

    if state_root_bytes.len() != 32 {
        anyhow::bail!(
            "stateRootHash tem tamanho inesperado: {}b (esperado 32)",
            state_root_bytes.len()
        );
    }

    Ok(state_root_bytes[..32].try_into().expect("32 bytes"))
}

/// Lê o ChainTip (StateRootHash + block info) do checkpoint em Rust puro.
/// Byte-idêntico ao C# RocksDBStore.GetBlock().StateRootHash.
///
/// Caminho:
///   1. chain/ → I + chainId + index → blockHash (tip - block_before)
///   2. block/blockindex → B + blockHash → epoch DB name
///   3. block/epochXXXX → B + blockHash → BlockDigest (bencodex)
///   4. Decodifica BlockDigest, extrai header["stateRootHash"]
pub fn read_state_root_from_checkpoint(
    store_checkpoint: &Path,
    block_before: u64,
) -> anyhow::Result<ChainTip> {
    let chain_path = store_checkpoint.join("chain");
    let block_root_path = store_checkpoint.join("block");
    let block_index_path = block_root_path.join("blockindex");

    if !chain_path.exists() {
        anyhow::bail!(
            "chain/ não encontrado em {}",
            store_checkpoint.display()
        );
    }
    if !block_index_path.exists() {
        anyhow::bail!(
            "block/blockindex não encontrado em {}",
            store_checkpoint.display()
        );
    }

    let chain_db = open_ro(&chain_path)?;
    let block_index_db = open_ro(&block_index_path)?;

    let canonical_chain_id =
        find_chain_id(&chain_db).context("Falha ao encontrar canonical chain ID")?;

    let (index_chain_id, tip_index, _tip_hash) =
        find_tip(&chain_db, &canonical_chain_id).context("Falha ao encontrar tip block")?;

    // Calcula target index
    let target_index = tip_index.saturating_sub(block_before as i64);

    // Busca block hash
    let block_hash = block_hash_at(&chain_db, &index_chain_id, target_index)
        .with_context(|| format!("Falha ao buscar block hash no índice {}", target_index))?;

    // Busca epoch DB name
    let epoch_db_name = get_block_epoch_db_name(&block_index_db, &block_hash)?;

    // Abre epoch DB
    let epoch_db_path = block_root_path.join(&epoch_db_name);
    if !epoch_db_path.exists() {
        anyhow::bail!(
            "Epoch DB {} não encontrado em {}",
            epoch_db_name,
            block_root_path.display()
        );
    }
    let epoch_db = open_ro(&epoch_db_path)?;

    // Extrai StateRootHash do BlockDigest
    let state_root_hash = get_state_root_from_digest(&epoch_db, &block_hash)?;

    Ok(ChainTip {
        state_root_hash,
        block_index: target_index,
        block_hash,
    })
}

/// Lê as informações completas do BlockHeader do checkpoint (para metadata generation).
/// 
/// Retorna todos os campos necessários para gerar o metadata JSON.
pub fn read_block_header_from_checkpoint(
    store_checkpoint: &Path,
    block_before: u64,
) -> anyhow::Result<BlockHeaderInfo> {
    let chain_path = store_checkpoint.join("chain");
    let block_root_path = store_checkpoint.join("block");
    let block_index_path = block_root_path.join("blockindex");

    if !chain_path.exists() {
        anyhow::bail!("chain/ não encontrado em {}", store_checkpoint.display());
    }
    if !block_index_path.exists() {
        anyhow::bail!("block/blockindex não encontrado em {}", store_checkpoint.display());
    }

    let chain_db = open_ro(&chain_path)?;
    let block_index_db = open_ro(&block_index_path)?;

    let canonical_chain_id =
        find_chain_id(&chain_db).context("Falha ao encontrar canonical chain ID")?;

    let (index_chain_id, tip_index, _tip_hash) =
        find_tip(&chain_db, &canonical_chain_id).context("Falha ao encontrar tip block")?;

    // Calcula target index
    let target_index = tip_index.saturating_sub(block_before as i64);

    // Busca block hash
    let block_hash = block_hash_at(&chain_db, &index_chain_id, target_index)
        .with_context(|| format!("Falha ao buscar block hash no índice {}", target_index))?;

    // Busca epoch DB name
    let epoch_db_name = get_block_epoch_db_name(&block_index_db, &block_hash)?;

    // Abre epoch DB
    let epoch_db_path = block_root_path.join(&epoch_db_name);
    if !epoch_db_path.exists() {
        anyhow::bail!(
            "Epoch DB {} não encontrado em {}",
            epoch_db_name,
            block_root_path.display()
        );
    }
    let epoch_db = open_ro(&epoch_db_path)?;

    // Extrai informações completas do header
    extract_header_info(&epoch_db, &block_hash)
}

/// Valida um DB states/ abrindo ele com RocksDB.
/// Se abriu sem erro, o DB é válido.
pub fn validate_states(path: &Path) -> anyhow::Result<()> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_max_open_files(500);
    let _db = DB::open_for_read_only(&opts, path, false)
    .with_context(|| format!("Falha ao validar states/: {}", path.display()))?;
    // Se chegou aqui, o DB é válido
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_state_root_from_live_store() {
        let store = std::path::Path::new("~/9c-blockchain/");
        if !store.join("chain").exists() || !store.join("nextstateroothash").exists() {
            eprintln!("Skipping test: chain/ or nextstateroothash/ not found");
            return;
        }
        let tip = read_state_root_from_checkpoint(store, 100)
        .expect("should read tip from live store");
        assert!(tip.block_index > 0);
        assert_ne!(tip.state_root_hash, [0u8; 32]);
        eprintln!(
            "✅ Test passed: block #{} state_root={}",
            tip.block_index,
            hex::encode(tip.state_root_hash)
        );
    }
}
