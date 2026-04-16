//! Quick diagnostic to inspect the raw bytes and Bencodex structure of a specific node.
//! Usage: cargo run --bin inspect_node -- <db_path> <hex_hash>

use anyhow::{Context, Result};
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <states_db_path> <hex_hash>", args[0]);
        eprintln!("Example: {} ~/states-test/states 63e2345f...", args[0]);
        std::process::exit(1);
    }

    let db_path = &args[1];
    let hex_hash = &args[2];

    // Parse hex hash
    let hash_bytes = hex::decode(hex_hash)
        .context("Invalid hex hash")?;
    
    println!("=== Node Inspector ===");
    println!("DB path: {}", db_path);
    println!("Hash: {} ({} bytes)", hex_hash, hash_bytes.len());

    // Open DB readonly
    let mut opts = Options::default();
    opts.set_max_open_files(64);
    let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, db_path, false)
        .context("Failed to open DB")?;

    // Get the raw value
    let raw = db.get(&hash_bytes)
        .context("RocksDB get error")?
        .context("Key not found in DB")?;

    println!("\n=== Raw Bytes ({} bytes) ===", raw.len());
    
    // Show first 256 bytes as hex
    let show_len = raw.len().min(512);
    println!("Hex (first {} bytes):", show_len);
    for (i, chunk) in raw[..show_len].chunks(32).enumerate() {
        print!("  {:04x}: ", i * 32);
        for b in chunk {
            print!("{:02x} ", b);
        }
        // ASCII representation
        print!(" |");
        for b in chunk {
            if *b >= 0x20 && *b < 0x7f {
                print!("{}", *b as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }

    // Show as UTF-8 (Bencodex uses ASCII-compatible prefix chars)
    println!("\n=== As UTF-8 string (first {} bytes) ===", show_len);
    let as_str = String::from_utf8_lossy(&raw[..show_len]);
    println!("{}", as_str);

    // Try to identify the Bencodex type from the first byte
    if !raw.is_empty() {
        let first = raw[0];
        println!("\n=== First byte analysis ===");
        match first {
            b'l' => println!("Starts with 'l' → Bencodex LIST"),
            b'd' => println!("Starts with 'd' → Bencodex DICT"),
            b'i' => println!("Starts with 'i' → Bencodex INTEGER"),
            b'n' => println!("Starts with 'n' → Bencodex NULL"),
            b't' => println!("Starts with 't' → Bencodex TRUE"),
            b'f' => println!("Starts with 'f' → Bencodex FALSE"),
            b'u' => println!("Starts with 'u' → Bencodex TEXT"),
            b'0'..=b'9' => println!("Starts with '{}' → Bencodex BYTES (length-prefixed)", first as char),
            b'e' => println!("Starts with 'e' → END marker"),
            _ => println!("Starts with 0x{:02x} ('{}') → UNKNOWN", first, 
                if first >= 0x20 && first < 0x7f { first as char } else { '?' }),
        }
    }

    // Try decode as Bencodex
    println!("\n=== Bencodex Decode ===");
    match nc_snapshot_rs::trie::bencodex::decode(&raw) {
        Ok(benc) => {
            println!("Success! Decoded as: {:?}", benc);
            
            // Try deeper analysis for lists
            if let nc_snapshot_rs::trie::bencodex::BencodexValue::List(items) = &benc {
                println!("\nList has {} elements:", items.len());
                for (i, item) in items.iter().enumerate() {
                    let type_str = match item {
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Null => "Null".to_string(),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Bool(b) => format!("Bool({})", b),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Integer(n) => format!("Integer({})", n),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Bytes(b) => format!("Bytes({} bytes: {})", b.len(), hex::encode(&b[..b.len().min(16)])),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Text(s) => format!("Text({:?})", &s[..s.len().min(50)]),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::List(inner) => format!("List({} items)", inner.len()),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Dict(entries) => format!("Dict({} entries)", entries.len()),
                    };
                    println!("  [{}]: {}", i, type_str);
                }
            }
            
            if let nc_snapshot_rs::trie::bencodex::BencodexValue::Dict(entries) = &benc {
                println!("\nDict has {} entries:", entries.len());
                for (key, val) in entries.iter() {
                    let val_str = match val {
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Null => "Null".to_string(),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Bool(b) => format!("Bool({})", b),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Bytes(b) => format!("Bytes({} bytes)", b.len()),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::List(items) => format!("List({} items)", items.len()),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Dict(e) => format!("Dict({} entries)", e.len()),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Text(s) => format!("Text({:?})", &s[..s.len().min(50)]),
                        nc_snapshot_rs::trie::bencodex::BencodexValue::Integer(n) => format!("Integer({})", n),
                    };
                    println!("  {:?} => {}", key, val_str);
                }
            }
        }
        Err(e) => {
            println!("FAILED: {}", e);
        }
    }

    // Try decode as TrieNode
    println!("\n=== TrieNode Decode ===");
    match nc_snapshot_rs::trie::node::TrieNode::decode(&raw) {
        Ok(node) => {
            println!("Success! Decoded as: {:?}", node);
            let hashes = node.child_hashes();
            println!("Child hashes: {} found", hashes.len());
            for (i, h) in hashes.iter().enumerate().take(20) {
                println!("  child[{}]: {}", i, hex::encode(h));
            }
        }
        Err(e) => {
            println!("FAILED: {}", e);
            println!("\nThis is the error we need to fix!");
        }
    }
    
    // Also try a few neighbor keys to understand the DB structure
    println!("\n=== Sample keys near this hash ===");
    let mut count = 0;
    for item in db.iterator(rocksdb::IteratorMode::From(&hash_bytes, rocksdb::Direction::Forward)) {
        if count >= 5 { break; }
        if let Ok((key, val)) = item {
            println!("  key({} bytes): {}  val({} bytes): first_byte=0x{:02x}", 
                key.len(), hex::encode(&key[..key.len().min(16)]),
                val.len(),
                if val.is_empty() { 0 } else { val[0] });
            count += 1;
        }
    }

    Ok(())
}
