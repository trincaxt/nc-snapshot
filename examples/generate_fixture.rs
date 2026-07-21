//! Gera fixture sintética "mini-chain" para testes do chain_reader.
//! Rode UMA VEZ: cargo run --example generate_fixture
//! Cria tests/fixtures/mini-chain/ com 3 blocos encadeados via RocksDB real.

use rocksdb::{BlockBasedOptions, Options, DB};
use std::collections::BTreeMap;
use std::path::Path;

use bencodex::{BencodexKey, BencodexValue, Encode};

const CHAIN_ID: [u8; 16] = [
    0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
];

fn make_db(path: &Path) -> DB {
    let _ = std::fs::remove_dir_all(path);
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_format_version(5);
    opts.set_block_based_table_factory(&block_opts);
    DB::open(&opts, path).unwrap()
}

fn block_hash(seed: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = seed;
    h[31] = seed;
    h
}

/// Constrói um BlockDigest Bencodex e retorna os bytes codificados.
///
/// Estrutura Bencodex do BlockDigest (fonte: Libplanet C#):
///   { 0x48('H'): {            // header
///       0x69('i'): <index>,        // index (i64)
///       0x74('t'): <timestamp>,     // timestamp (text)
///       0x73('s'): <stateRootHash>, // 32 bytes binary
///       0x70('p'): <previousHash>,  // 32 bytes binary
///       0x78('x'): <txHash>,        // OPCIONAL: 32 bytes binary
///     }
///   }
fn make_block_digest(
    index: i64,
    ts: &str,
    state_root: [u8; 32],
    prev_hash: [u8; 32],
    tx_hash: Option<[u8; 32]>,
) -> Vec<u8> {
    let mut header: BTreeMap<BencodexKey, BencodexValue> = BTreeMap::new();

    // index: 0x69 = 'i'
    header.insert(vec![0x69u8].into(), index.into());
    // timestamp: 0x74 = 't'
    header.insert(vec![0x74u8].into(), ts.to_string().into());
    // stateRootHash: 0x73 = 's'
    header.insert(vec![0x73u8].into(), state_root.to_vec().into());
    // previousHash: 0x70 = 'p'
    header.insert(vec![0x70u8].into(), prev_hash.to_vec().into());
    // txHash: 0x78 = 'x' — OPCIONAL
    if let Some(tx) = tx_hash {
        header.insert(vec![0x78u8].into(), tx.to_vec().into());
    }

    let mut digest: BTreeMap<BencodexKey, BencodexValue> = BTreeMap::new();
    digest.insert(vec![0x48u8].into(), BencodexValue::Dictionary(header)); // 0x48 = 'H'

    let value = BencodexValue::Dictionary(digest);
    let mut buf = Vec::new();
    value.encode(&mut buf).expect("Failed to encode BlockDigest");
    buf
}

fn main() {
    let root = Path::new("tests/fixtures/mini-chain");
    let _ = std::fs::create_dir_all(root);

    // ── chain DB ────────────────────────────────────────────────────
    // Keys: I(0x49) + chainId(16) + index(8BE) → blockHash(32)
    let chain = make_db(&root.join("chain"));
    chain.put([0x43u8], &CHAIN_ID).unwrap(); // 'C' → canonical chain ID

    let h0 = block_hash(0x00);
    let h1 = block_hash(0x01);
    let h2 = block_hash(0x02);

    for (idx, hash) in [(0i64, h0), (1, h1), (2, h2)] {
        let mut key = vec![0x49u8]; // 'I'
        key.extend_from_slice(&CHAIN_ID);
        key.extend_from_slice(&idx.to_be_bytes());
        chain.put(&key, &hash).unwrap();
    }

    // ── block/blockindex DB ─────────────────────────────────────────
    // Keys: B(0x42) + blockHash(32) → "epoch0" (string)
    let idx_db = make_db(&root.join("block").join("blockindex"));
    for hash in &[h0, h1, h2] {
        let mut key = vec![0x42u8]; // 'B'
        key.extend_from_slice(hash);
        idx_db.put(&key, b"epoch0").unwrap();
    }

    // ── block/epoch0 DB ─────────────────────────────────────────────
    // Keys: B(0x42) + blockHash(32) → BlockDigest (bencodex)
    let epoch = make_db(&root.join("block").join("epoch0"));

    let state0 = [0xaau8; 32];
    let state1 = [0xbbu8; 32];
    let state2 = [0xccu8; 32];

    let prev0 = [0x00u8; 32]; // gênese
    let prev1 = h0;
    let prev2 = h1;

    let tx0 = Some([0xffu8; 32]);
    let tx1 = None; // ← bloco SEM txHash
    let tx2 = Some([0xeeu8; 32]);

    let blocks = [
        (&h0, 0i64, "2024-01-01T00:00:00.000000Z", &state0, &prev0, tx0),
        (&h1, 1i64, "2024-01-01T00:01:00.000000Z", &state1, &prev1, tx1),
        (&h2, 2i64, "2024-01-01T00:02:00.000000Z", &state2, &prev2, tx2),
    ];

    for &(hash, index, ts, state, prev, tx) in &blocks {
        let digest = make_block_digest(index, ts, *state, *prev, tx);
        let mut key = vec![0x42u8]; // 'B'
        key.extend_from_slice(hash);
        epoch.put(&key, &digest).unwrap();
    }

    eprintln!("✅ Fixture sintética gerada em: {:?}", root);
    eprintln!("   3 blocos: gênese(0) → bloco#1 → bloco#2 (tip)");
}
