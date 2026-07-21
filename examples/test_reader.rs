use nc_snapshot::chain_reader;
use std::path::Path;

fn main() {
    let checkpoint = Path::new("/home/vrunnx/snapshots/.nc-snapshot-live-checkpoint");
    
    match chain_reader::read_state_root_from_checkpoint(checkpoint, 0) {
        Ok(tip) => {
            println!("✅ Rust Chain Reader FUNCIONOU!");
            println!("   StateRoot: {}", hex::encode(tip.state_root_hash));
            println!("   Block: #{}", tip.block_index);
            println!("   BlockHash: {}", hex::encode(tip.block_hash));
        }
        Err(e) => {
            println!("❌ Erro: {}", e);
        }
    }
}
