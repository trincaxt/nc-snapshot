use anyhow::Result;
use std::path::PathBuf;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};
use nc_snapshot_rs::trie::bencodex::{self, BencodexValue, BencodexKey};

fn main() -> Result<()> {
    let blockindex_db_path = PathBuf::from("/home/vrunnx/9c-blockchain/block/blockindex");
    let mut opts = Options::default();
    opts.create_if_missing(false);
    let index_db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, &blockindex_db_path, false)?;
    
    let hash = hex::decode("c23a4562de352781f4499210a7b2959c5687b782f0ea9d5e4b51e6f2b0a01b88")?;
    let mut key = vec![0x42];
    key.extend_from_slice(&hash);
    
    if let Ok(Some(epoch_val)) = index_db.get(&key) {
        let epoch_str = String::from_utf8_lossy(&epoch_val);
        let epoch_dir = PathBuf::from("/home/vrunnx/9c-blockchain/block").join(epoch_str.as_ref());
        let epoch_db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, &epoch_dir, false)?;
        if let Ok(Some(raw)) = epoch_db.get(&key) {
            let block = bencodex::decode(&raw)?;
            if let BencodexValue::Dict(entries) = block {
                for (k, v) in entries {
                    if let BencodexKey::Bytes(b) = &k {
                        if b == b"H" {
                            if let BencodexValue::Dict(h_entries) = v {
                                for (hk, hv) in h_entries {
                                    if let BencodexKey::Bytes(hb) = &hk {
                                        if hb == b"s" {
                                            println!("StateRoot Type: {:?}", hv);
                                            if let BencodexValue::Bytes(v_str) = hv {
                                                println!("Bytes len: {}", v_str.len());
                                            } else if let BencodexValue::Dict(d) = hv {
                                                println!("It's a struct!");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
