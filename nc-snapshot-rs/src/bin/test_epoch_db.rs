use anyhow::Result;
use std::path::PathBuf;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};

fn main() -> Result<()> {
    // 2e18c72c67ece493099c6e8913778a5411336d7553ff403e759d1c2e2eb3330a 
    let block_db_path = PathBuf::from("/home/vrunnx/9c-blockchain/block/epoch18562");
    
    let mut opts = Options::default();
    opts.create_if_missing(false);
    let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, &block_db_path, false)?;
    
    for (i, item) in db.iterator(rocksdb::IteratorMode::Start).enumerate() {
        if i >= 10 { break; }
        if let Ok((k, v)) = item {
            println!("Key: {} | Val len: {} | Val head: {}", hex::encode(&k), v.len(), hex::encode(&v[..std::cmp::min(v.len(), 16)]));
        }
    }
    Ok(())
}
