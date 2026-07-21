//! Testes de integração contra a fixture sintética mini-chain.
//!
//! Testa read_state_root_from_checkpoint() e read_block_header_from_checkpoint()
//! com um RocksDB de 3 blocos encadeados — sem depender de 9c-blockchain real.
//!
//! A fixture foi gerada por: cargo run --example generate_fixture

use nc_snapshot::chain_reader::{
    read_block_header_from_checkpoint, read_state_root_from_checkpoint,
};

const FIXTURE_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/mini-chain");

#[test]
fn test_read_tip_block_0() {
    let tip = read_state_root_from_checkpoint(std::path::Path::new(FIXTURE_PATH), 0)
        .expect("should read tip");

    assert_eq!(tip.block_index, 2, "tip should be block #2");
    assert_eq!(tip.state_root_hash[0], 0xcc, "state root should start with 0xcc");
    assert_eq!(tip.state_root_hash[31], 0xcc);
}

#[test]
fn test_read_tip_block_before_1() {
    let tip = read_state_root_from_checkpoint(std::path::Path::new(FIXTURE_PATH), 1)
        .expect("should read block #1");

    assert_eq!(tip.block_index, 1);
    assert_eq!(tip.state_root_hash[0], 0xbb);
    assert_eq!(tip.state_root_hash[31], 0xbb);
}

#[test]
fn test_read_tip_block_before_2() {
    let tip = read_state_root_from_checkpoint(std::path::Path::new(FIXTURE_PATH), 2)
        .expect("should read genesis");

    assert_eq!(tip.block_index, 0);
    assert_eq!(tip.state_root_hash[0], 0xaa);
    assert_eq!(tip.state_root_hash[31], 0xaa);
}

#[test]
fn test_read_block_header_full_metadata() {
    let header = read_block_header_from_checkpoint(std::path::Path::new(FIXTURE_PATH), 0)
        .expect("should read header");

    assert_eq!(header.index, 2);
    assert_eq!(header.timestamp, "2024-01-01T00:02:00.000000Z");
    assert_eq!(header.state_root_hash[0], 0xcc);
    assert_eq!(header.previous_hash[0], 0x01); // aponta pro bloco 1
    assert!(header.tx_hash.is_some(), "block #2 should have tx_hash");
    assert_eq!(header.tx_hash.unwrap()[0], 0xee);
}

#[test]
fn test_read_block_header_optional_txhash_none() {
    // Block #1 é deliberadamente sem txHash (Option::None path)
    let header = read_block_header_from_checkpoint(std::path::Path::new(FIXTURE_PATH), 1)
        .expect("should read block #1 header");

    assert_eq!(header.index, 1);
    assert!(header.tx_hash.is_none(), "block #1 should have NO tx_hash (None)");
}
