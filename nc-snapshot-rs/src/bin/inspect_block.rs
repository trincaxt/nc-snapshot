use anyhow::Result;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options, IteratorMode};
use nc_snapshot_rs::trie::bencodex;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <chain_db_path>", args[0]);
        std::process::exit(1);
    }
    let db_path = &args[1];
    
    let mut opts = Options::default();
    opts.set_max_open_files(64);
    let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, db_path, false)?;
    
    // Look for tip
    let mut tip_hash = None;
    for item in db.iterator(IteratorMode::Start) {
        let (key, val) = item.unwrap();
        if key.starts_with(b"chain-tip/") {
            println!("Found chain-tip key: {}", String::from_utf8_lossy(&key));
            tip_hash = Some(val.to_vec());
            break;
        }
    }
    
    if tip_hash.is_none() {
        if let Ok(Some(val)) = db.get(b"tip") {
            println!("Found 'tip' key");
            tip_hash = Some(val.to_vec());
        }
    }
    
    if tip_hash.is_none() {
        println!("No tip found!");
        return Ok(());
    }
    
    let hash = tip_hash.unwrap();
    println!("Tip hash: {}", hex::encode(&hash));
    
    // Get block
    let mut block_key = b"block/".to_vec();
    block_key.extend_from_slice(&hash);
    
    let raw = match db.get(&block_key)? {
        Some(v) => { println!("Got block with block_key prefix"); v }
        None => match db.get(&hash)? {
            Some(v) => { println!("Got block with no prefix"); v }
            None => { println!("Block not found"); return Ok(()); }
        }
    };
    
    let decoded = bencodex::decode(&raw)?;
    
    // Print Bencodex structure
    println!("Block decoded structure:");
    fn print_val(v: &bencodex::BencodexValue, indent: &str, depth: usize) {
        if depth > 5 { return; }
        match v {
            bencodex::BencodexValue::Dict(d) => {
                println!("{}Dict ({} entries):", indent, d.len());
                for (k, v2) in d {
                    print!("{}  {:?} => ", indent, k);
                    if let bencodex::BencodexValue::Bytes(b) = v2 {
                        if b.len() == 32 {
                            println!("Bytes(32: {})", hex::encode(b));
                        } else if b.len() > 64 {
                            println!("Bytes({} bytes)", b.len());
                        } else {
                            println!("{:?}", v2);
                        }
                    } else if let bencodex::BencodexValue::Dict(_) | bencodex::BencodexValue::List(_) = v2 {
                        println!();
                        print_val(v2, &format!("{}    ", indent), depth + 1);
                    } else {
                        println!("{:?}", v2);
                    }
                }
            }
            bencodex::BencodexValue::List(l) => {
                println!("{}List ({} entries):", indent, l.len());
                for (i, v2) in l.iter().enumerate() {
                    print!("{}[{}] ", indent, i);
                    if let bencodex::BencodexValue::Bytes(b) = v2 {
                        if b.len() == 32 {
                            println!("Bytes(32: {})", hex::encode(b));
                        } else {
                            println!("Bytes({} bytes)", b.len());
                        }
                    } else if let bencodex::BencodexValue::Dict(_) | bencodex::BencodexValue::List(_) = v2 {
                        println!();
                        print_val(v2, &format!("{}    ", indent), depth + 1);
                    } else {
                        println!("{:?}", v2);
                    }
                }
            }
            _ => println!("{}{:?}", indent, v),
        }
    }
    
    print_val(&decoded, "", 0);
    
    Ok(())
}
