use anyhow::Result;
use std::path::PathBuf;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};

fn main() -> Result<()> {
    let block_db_path = PathBuf::from("/home/vrunnx/9c-blockchain/block/blockindex");
    let mut opts = Options::default();
    opts.create_if_missing(false);
    let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, &block_db_path, false)?;
    for (i, item) in db.iterator(rocksdb::IteratorMode::Start).enumerate() {
        if i >= 10 { break; }
        if let Ok((k, v)) = item {
            println!("Key: {} | Val len: {} | Val: {}", hex::encode(&k), v.len(), hex::encode(&v));
        }
    }
    Ok(())
}
