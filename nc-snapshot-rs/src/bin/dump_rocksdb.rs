use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    db_path: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut opts = Options::default();
    opts.create_if_missing(false);
    let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, &cli.db_path, false)?;
    for (i, item) in db.iterator(rocksdb::IteratorMode::Start).enumerate() {
        if i >= 10 { break; }
        if let Ok((k, v)) = item {
            println!("Key: {} | Val len: {} | Val subset: {}", hex::encode(&k), v.len(), hex::encode(&v));
        }
    }
    Ok(())
}
