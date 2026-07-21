use bencodex::{BencodexValue, decode_borrowed, BencodexKey};
use rocksdb::{DB, Options};
use std::path::Path;

fn main() {
    let checkpoint = Path::new("/home/vrunnx/snapshots/.nc-snapshot-live-checkpoint");
    let block_index_path = checkpoint.join("block/blockindex");
    let chain_path = checkpoint.join("chain");
    
    let mut opts = Options::default();
    opts.create_if_missing(false);
    
    let chain_db = DB::open_for_read_only(&opts, &chain_path, false).unwrap();
    let block_index_db = DB::open_for_read_only(&opts, &block_index_path, false).unwrap();
    
    let chain_id = chain_db.get(&[b'C']).unwrap().unwrap();
    
    // Usa o bloco que o C# achou
    let block_index = 18943467i64;
    
    let mut key = vec![b'I'];
    key.extend_from_slice(&chain_id);
    key.extend_from_slice(&block_index.to_be_bytes());
    
    let block_hash = chain_db.get(&key).expect("Block not found").expect("Block not found in chain");
    
    let mut bkey = vec![b'B'];
    bkey.extend_from_slice(&block_hash);
    let epoch_name = String::from_utf8(block_index_db.get(&bkey).unwrap().unwrap()).unwrap();
    
    println!("Block #{}: {}", block_index, hex::encode(&block_hash));
    println!("Epoch: {}", epoch_name);
    
    let epoch_path = checkpoint.join("block").join(&epoch_name);
    let epoch_db = DB::open_for_read_only(&opts, &epoch_path, false).unwrap();
    
    let digest_bytes = epoch_db.get(&bkey).unwrap().unwrap();
    
    println!("\nBlockDigest raw size: {} bytes", digest_bytes.len());
    
    let value = decode_borrowed(&digest_bytes).unwrap();
    
    println!("\nBlockDigest structure:");
    print_bencodex(&value, 0);
}

fn print_bencodex(val: &BencodexValue, indent: usize) {
    let prefix = "  ".repeat(indent);
    match val {
        BencodexValue::Dictionary(d) => {
            println!("{}Dictionary with {} keys:", prefix, d.len());
            for (k, v) in d.iter() {
                let key_str = match k {
                    BencodexKey::Text(s) => format!("\"{}\"", s),
                    BencodexKey::Binary(b) => format!("0x{}", hex::encode(b.as_ref())),
                };
                println!("{}  {}:", prefix, key_str);
                print_bencodex(v, indent + 2);
            }
        }
        BencodexValue::List(l) => {
            println!("{}List with {} items", prefix, l.len());
            if l.len() <= 3 {
                for (i, item) in l.iter().enumerate() {
                    println!("{}  [{}]:", prefix, i);
                    print_bencodex(item, indent + 2);
                }
            }
        }
        BencodexValue::Binary(b) => {
            let bytes = b.as_ref();
            if bytes.len() <= 32 {
                println!("{}Binary({} bytes): {}", prefix, bytes.len(), hex::encode(bytes));
            } else {
                println!("{}Binary({} bytes): {}...", prefix, bytes.len(), hex::encode(&bytes[..16]));
            }
        }
        BencodexValue::Text(s) => println!("{}Text: \"{}\"", prefix, s),
        BencodexValue::Number(n) => println!("{}Number: {}", prefix, n),
        BencodexValue::Boolean(b) => println!("{}Boolean: {}", prefix, b),
        BencodexValue::Null => println!("{}Null", prefix),
    }
}
