//! Benchmarks for the Bencodex decoder.
//!
//! Run with: cargo bench --bench bencodex_bench

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use nc_snapshot_rs::trie::bencodex::{decode, encode, BencodexValue};
use num_bigint::BigInt;

fn create_trie_node_data() -> Vec<u8> {
    // Simulate a FullNode with 4 hash children
    let hash = vec![0xAB_u8; 32];
    let mut items = vec![BencodexValue::Null; 17];
    items[0] = BencodexValue::Bytes(hash.clone());
    items[3] = BencodexValue::Bytes(hash.clone());
    items[7] = BencodexValue::Bytes(hash.clone());
    items[15] = BencodexValue::Bytes(hash);
    items[16] = BencodexValue::Bytes(b"some_leaf_value_here".to_vec());
    encode(&BencodexValue::List(items))
}

fn create_short_node_data() -> Vec<u8> {
    let hash = vec![0xCD_u8; 32];
    encode(&BencodexValue::List(vec![
        BencodexValue::Bytes(vec![0x01, 0x23, 0x45]),
        BencodexValue::Bytes(hash),
    ]))
}

fn create_complex_data() -> Vec<u8> {
    // Simulate a complex state value with nested structures
    encode(&BencodexValue::Dict(vec![
        (
            nc_snapshot_rs::trie::bencodex::BencodexKey::Text("address".into()),
            BencodexValue::Bytes(vec![0x12; 20]),
        ),
        (
            nc_snapshot_rs::trie::bencodex::BencodexKey::Text("balance".into()),
            BencodexValue::Integer(BigInt::from(1_000_000_000_i64)),
        ),
        (
            nc_snapshot_rs::trie::bencodex::BencodexKey::Text("inventory".into()),
            BencodexValue::List(vec![
                BencodexValue::Bytes(vec![1, 2, 3, 4]),
                BencodexValue::Bytes(vec![5, 6, 7, 8]),
                BencodexValue::Text("sword_of_fire".into()),
            ]),
        ),
    ]))
}

fn bench_decode_full_node(c: &mut Criterion) {
    let data = create_trie_node_data();
    c.bench_function("decode_full_node", |b| {
        b.iter(|| decode(black_box(&data)).unwrap())
    });
}

fn bench_decode_short_node(c: &mut Criterion) {
    let data = create_short_node_data();
    c.bench_function("decode_short_node", |b| {
        b.iter(|| decode(black_box(&data)).unwrap())
    });
}

fn bench_decode_complex(c: &mut Criterion) {
    let data = create_complex_data();
    c.bench_function("decode_complex_dict", |b| {
        b.iter(|| decode(black_box(&data)).unwrap())
    });
}

fn bench_encode_roundtrip(c: &mut Criterion) {
    let data = create_trie_node_data();
    let decoded = decode(&data).unwrap();
    c.bench_function("encode_roundtrip", |b| {
        b.iter(|| {
            let encoded = encode(black_box(&decoded));
            decode(black_box(&encoded)).unwrap()
        })
    });
}

criterion_group!(
    benches,
    bench_decode_full_node,
    bench_decode_short_node,
    bench_decode_complex,
    bench_encode_roundtrip,
);
criterion_main!(benches);
