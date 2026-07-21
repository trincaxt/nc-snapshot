//! 🔍 Diagnostic: verifica se a chave TxHash (0x78) está presente
//! no header Bencodex de um bloco específico do checkpoint.
//!
//! Uso:
//!   cargo run --example debug_txhash -- [block_index] [checkpoint_path]
//!
//! Padrão: block_index=19018450, checkpoint=~/snapshots/.nc-snapshot-live-checkpoint
//!
//! O script:
//!   1. Abre o checkpoint RocksDB
//!   2. Lê o BlockDigest Bencodex do bloco alvo
//!   3. Imprime TODAS as chaves do dicionário raiz E do header
//!   4. Verifica especificamente se a chave 0x78 (TxHash) existe
//!   5. Se existe, valida se tem 32 bytes

use bencodex::{BencodexValue, decode_borrowed, BencodexKey};
use rocksdb::{DB, Options};
use std::env;
use std::path::Path;

fn main() {
    let args: Vec<String> = env::args().collect();

    let block_index: i64 = args.get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(19018450);

    let checkpoint = args.get(2)
        .map(Path::new)
        .unwrap_or_else(|| Path::new("/home/vrunnx/snapshots/.nc-snapshot-live-checkpoint"));

    println!("╔══════════════════════════════════════════════╗");
    println!("║  🔍 TxHash Debug - Block #{:<8}      ║", block_index);
    println!("╚══════════════════════════════════════════════╝");
    println!("  Checkpoint: {}", checkpoint.display());
    println!();

    // ── Abrir DBs ────────────────────────────────────────────
    let mut opts = Options::default();
    opts.create_if_missing(false);

    let chain_path = checkpoint.join("chain");
    let block_index_path = checkpoint.join("block/blockindex");

    if !chain_path.exists() {
        eprintln!("❌ chain/ não encontrado em {}", chain_path.display());
        return;
    }
    if !block_index_path.exists() {
        eprintln!("❌ block/blockindex não encontrado em {}", block_index_path.display());
        return;
    }

    let chain_db = match DB::open_for_read_only(&opts, &chain_path, false) {
        Ok(db) => db,
        Err(e) => { eprintln!("❌ Erro abrindo chain/: {}", e); return; }
    };
    let block_index_db = match DB::open_for_read_only(&opts, &block_index_path, false) {
        Ok(db) => db,
        Err(e) => { eprintln!("❌ Erro abrindo block/blockindex: {}", e); return; }
    };

    // ── Encontrar chain ID ────────────────────────────────────
    let chain_id = match chain_db.get(&[b'C']) {
        Ok(Some(id)) if id.len() == 16 => id,
        Ok(v) => { eprintln!("❌ Chain ID não encontrado ou inválido: {:?}", v); return; }
        Err(e) => { eprintln!("❌ Erro lendo chain ID: {}", e); return; }
    };
    println!("  Chain ID: {}", hex::encode(&chain_id));
    println!();

    // ── Buscar block hash ─────────────────────────────────────
    let mut idx_key = vec![b'I'];
    idx_key.extend_from_slice(&chain_id);
    idx_key.extend_from_slice(&block_index.to_be_bytes());

    let block_hash = match chain_db.get(&idx_key) {
        Ok(Some(hash)) if hash.len() == 32 => hash,
        Ok(None) => { eprintln!("❌ Bloco #{} não encontrado no chain DB", block_index); return; }
        Ok(Some(v)) => { eprintln!("❌ Block hash com tamanho inesperado: {}b", v.len()); return; }
        Err(e) => { eprintln!("❌ Erro lendo block hash: {}", e); return; }
    };
    println!("  Block hash: {}", hex::encode(&block_hash));
    println!();

    // ── Buscar epoch DB name ─────────────────────────────────
    let mut bkey = vec![b'B'];
    bkey.extend_from_slice(&block_hash);

    let epoch_name = match block_index_db.get(&bkey) {
        Ok(Some(val)) => String::from_utf8(val.to_vec()).unwrap_or_else(|_| hex::encode(&val)),
        Ok(None) => { eprintln!("❌ Bloco não encontrado no blockindex DB"); return; }
        Err(e) => { eprintln!("❌ Erro lendo blockindex: {}", e); return; }
    };
    println!("  Epoch DB: {}", epoch_name);
    println!();

    // ── Abrir epoch DB e ler BlockDigest ───────────────────────
    let epoch_path = checkpoint.join("block").join(&epoch_name);
    if !epoch_path.exists() {
        eprintln!("❌ Epoch DB não encontrado: {}", epoch_path.display());
        return;
    }
    let epoch_db = match DB::open_for_read_only(&opts, &epoch_path, false) {
        Ok(db) => db,
        Err(e) => { eprintln!("❌ Erro abrindo epoch DB: {}", e); return; }
    };

    let digest_bytes = match epoch_db.get(&bkey) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => { eprintln!("❌ BlockDigest não encontrado no epoch DB"); return; }
        Err(e) => { eprintln!("❌ Erro lendo BlockDigest: {}", e); return; }
    };
    println!("  BlockDigest raw: {} bytes", digest_bytes.len());
    println!();

    // ── Decodificar Bencodex ──────────────────────────────────
    let value = match decode_borrowed(&digest_bytes) {
        Ok(v) => v,
        Err(e) => { eprintln!("❌ Erro decodificando Bencodex: {:?}", e); return; }
    };

    let digest_dict = match &value {
        BencodexValue::Dictionary(d) => d,
        _ => { eprintln!("❌ BlockDigest não é um dicionário"); return; }
    };

    // ══════════════════════════════════════════════════════════
    // PARTE 1: TODAS as chaves do dicionário raiz do BlockDigest
    // ══════════════════════════════════════════════════════════
    println!("╔═══ BLOCKDIGEST - TODAS AS CHAVES ════════════════╗");

    let mut header_dict = None;
    for (k, v) in digest_dict.iter() {
        let key_label = match k {
            BencodexKey::Binary(b) => {
                let bytes = b.as_ref();
                format!("0x{} ({}b)", hex::encode(bytes), bytes.len())
            }
            BencodexKey::Text(s) => format!("\"{}\"", s),
        };

        let val_desc = match v {
            BencodexValue::Dictionary(d) => {
                if let BencodexKey::Binary(ref b) = k {
                    if b.as_ref() == &[0x48] {
                        header_dict = Some(d);
                    }
                }
                format!("Dictionary ({} keys)", d.len())
            }
            BencodexValue::List(l) => format!("List ({} items)", l.len()),
            BencodexValue::Binary(b) => {
                let bytes = b.as_ref();
                if bytes.len() <= 32 {
                    format!("Binary: {}", hex::encode(bytes))
                } else {
                    format!("Binary: {}... ({}b)", hex::encode(&bytes[..16]), bytes.len())
                }
            }
            BencodexValue::Text(s) => format!("\"{}\"", s),
            BencodexValue::Number(n) => format!("Number: {}", n),
            BencodexValue::Boolean(b) => format!("Boolean: {}", b),
            BencodexValue::Null => "Null".to_string(),
        };
        println!("  {}: {}", key_label, val_desc);
    }
    println!();

    // ══════════════════════════════════════════════════════════
    // PARTE 2: HEADER — cada campo detalhado, checando 0x78
    // ══════════════════════════════════════════════════════════
    let header = match header_dict {
        Some(d) => d,
        None => {
            let hk = BencodexKey::Binary(std::borrow::Cow::Borrowed(&[0x48]));
            match digest_dict.get(&hk) {
                Some(BencodexValue::Dictionary(d)) => d,
                _ => { eprintln!("❌ Header (0x48) não encontrado no BlockDigest"); return; }
            }
        }
    };

    println!("╔═══ HEADER - DETALHADO ════════════════════════════╗");
    let mut found_tx_hash = false;
    let mut tx_hash_valid_len = false;
    let mut found_tx_hashes_list = false;
    let mut tx_count = 0;

    for (k, v) in header.iter() {
        // Extrair bytes da chave se for Binary
        let key_bytes: Option<&[u8]> = match k {
            BencodexKey::Binary(b) => Some(b.as_ref()),
            BencodexKey::Text(_) => None,
        };

        let key_label = match k {
            BencodexKey::Binary(b) => {
                let bytes = b.as_ref();
                let chr = if bytes.len() == 1 && bytes[0].is_ascii_graphic() {
                    format!(" ('{}')", bytes[0] as char)
                } else {
                    String::new()
                };
                format!("0x{}{}", hex::encode(bytes), chr)
            }
            BencodexKey::Text(s) => format!("\"{}\"", s),
        };

        // Nome do campo e detecção de TxHash
        let field_name = match key_bytes {
            Some(b) if b == [0x73] => "stateRootHash",
            Some(b) if b == [0x70] => "previousHash",
            Some(b) if b == [0x69] => "index",
            Some(b) if b == [0x74] => "timestamp",
            Some(b) if b == [0x78] => "TxHash ★★★",
            Some(b) if b == [0x54] => "txHashes",
            _ => "unknown",
        };

        if key_bytes == Some(&[0x78]) {
            found_tx_hash = true;
        }

        match v {
            BencodexValue::Binary(b) => {
                let bytes = b.as_ref();
                let desc = if bytes.len() <= 32 {
                    hex::encode(bytes)
                } else {
                    format!("{}... ({}b)", hex::encode(&bytes[..16]), bytes.len())
                };
                println!("  {}: {} = {}", key_label, field_name, desc);

                // Validar tamanho do TxHash
                if key_bytes == Some(&[0x78]) {
                    tx_hash_valid_len = bytes.len() == 32;
                    if bytes.len() != 32 {
                        println!("    ⚠️  TxHash TEM TAMANHO INESPERADO: {}b (esperado 32)!", bytes.len());
                    }
                }
            }
            BencodexValue::Text(s) => {
                println!("  {}: timestamp = \"{}\"", key_label, s);
            }
            BencodexValue::Number(n) => {
                println!("  {}: {} = {}", key_label, field_name, n);
            }
            BencodexValue::Boolean(b) => {
                println!("  {}: {} = {}", key_label, field_name, b);
            }
            BencodexValue::List(l) => {
                let tx_list = key_bytes == Some(&[0x54]);
                if tx_list {
                    found_tx_hashes_list = true;
                    tx_count = l.len();
                }
                println!("  {}: {} = List ({} items)", key_label, field_name, l.len());
                if tx_list && l.len() > 0 {
                    for (i, item) in l.iter().enumerate().take(3) {
                        if let BencodexValue::Binary(b) = item {
                            println!(
                                "    [{}]: {}",
                                i,
                                hex::encode(&b.as_ref()[..b.as_ref().len().min(32)])
                            );
                        }
                    }
                    if l.len() > 3 {
                        println!("    ... e mais {} items", l.len() - 3);
                    }
                }
            }
            BencodexValue::Dictionary(d) => {
                println!("  {}: {} = Dictionary ({} keys)", key_label, field_name, d.len());
            }
            BencodexValue::Null => {
                println!("  {}: {} = null", key_label, field_name);
            }
        }
    }
    println!();

    // ══════════════════════════════════════════════════════════
    // PARTE 3: DIAGNÓSTICO FINAL
    // ══════════════════════════════════════════════════════════
    println!("╔═══ DIAGNÓSTICO FINAL ═════════════════════════════╗");
    println!("  Bloco alvo: #{}", block_index);
    println!();
    println!("  Key 0x78 (TxHash) presente no header?");
    if found_tx_hash {
        println!("    ✅ SIM");
        if tx_hash_valid_len {
            println!("    ✅ Tamanho correto: 32 bytes");
        } else {
            println!("    ❌ TAMANHO INVÁLIDO (não são 32 bytes)");
        }
    } else {
        println!("    ❌ NÃO");
    }
    println!();
    println!("  Key 0x54 (txHashes) presente?");
    if found_tx_hashes_list {
        println!("    ✅ SIM — {} transações no bloco", tx_count);
    } else {
        println!("    ❌ NÃO — bloco sem lista de transações");
    }
    println!();
    if found_tx_hash {
        println!("  ✅ CONCLUSÃO: TxHash ESTÁ no header.");
        println!("     O problema NÃO é o bloco — o bug está em outro lugar.");
    } else if found_tx_hashes_list && tx_count > 0 {
        println!("  ⚠️  CONCLUSÃO: TxHash NÃO está no header, mas o bloco TEM {} transações!", tx_count);
        println!("     ISSO É ESTRANHO. Pode ser um bug na serialização do bloco.");
        println!("     Ou a chave 0x78 pode estar em outro lugar (ex: dentro de um sub-dicionário).");
    } else {
        println!("  ℹ️  CONCLUSÃO: TxHash NÃO está no header.");
        println!("     O bloco não tem transações — é normal o TxHash estar ausente.");
        println!("     O C# trata isso com TryGetValue (opcional).");
        println!("     A correção com Option<[u8; 32]> é a abordagem correta.");
    }

    // ── Raw bytes (primeiros 40) ─────────────────────────────
    println!();
    println!("╔═══ RAW BYTES (primeiros 40) ════════════════════╗");
    let dump: Vec<String> = digest_bytes.iter().take(40).map(|b| format!("{:02x}", b)).collect();
    println!("  {}", dump.join(" "));
    if digest_bytes.len() > 40 {
        println!("  ... ({} bytes total)", digest_bytes.len());
    }
}
