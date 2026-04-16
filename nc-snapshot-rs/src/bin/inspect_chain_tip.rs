use anyhow::Result;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options, IteratorMode};
use nc_snapshot_rs::trie::bencodex;

fn get_chain_tip(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<u8>> {
    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.unwrap();
        if key.starts_with(b"chain-tip/") {
            return Ok(value.to_vec());
        }
    }
    if let Ok(Some(value)) = db.get(b"tip") {
        return Ok(value.to_vec());
    }
    let mut highest_index = 0u64;
    let mut highest_hash = None;
    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.unwrap();
        if key.starts_with(b"block-index/") && key.len() == 20 { // block-index/ + 8 bytes
            let index_bytes: [u8; 8] = key[12..].try_into().unwrap_or([0u8; 8]);
            let index = u64::from_be_bytes(index_bytes);
            if index >= highest_index {
                highest_index = index;
                highest_hash = Some(value.to_vec());
            }
        }
    }
    highest_hash.ok_or_else(|| anyhow::anyhow!("Could not find any block in chain/ DB"))
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let db_path = &args[1];
    let mut opts = Options::default();
    opts.set_max_open_files(64);
    let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, db_path, false)?;
    
    println!("Looking for tip...");
    let tip_hash = get_chain_tip(&db)?;
    println!("Tip hash: {}", hex::encode(&tip_hash));
    
    // Read block
    let mut key = Vec::with_capacity(6 + tip_hash.len());
    key.extend_from_slice(b"block/");
    key.extend_from_slice(&tip_hash);
    
    let raw = db.get(&key)?.or_else(|| db.get(&tip_hash).unwrap()).unwrap();
    println!("Block size: {}", raw.len());
    
    let block = bencodex::decode(&raw)?;
    // just print first level keys
    if let bencodex::BencodexValue::Dict(entries) = block {
        println!("Block keys:");
        for (k, _) in entries {
            println!(" - {:?}", k);
        }
    } else {
        println!("Block is not a Dict!");
    }
    
    Ok(())
}
