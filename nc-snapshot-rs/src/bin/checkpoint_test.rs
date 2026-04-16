use rocksdb::{DBWithThreadMode, MultiThreaded, Options, checkpoint::Checkpoint};
fn main() {
    let mut opts = Options::default();
    opts.set_max_open_files(128);
    let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&opts, "/home/vrunnx/9c-blockchain/states", false).unwrap();
    let checkpoint = Checkpoint::new(&db).unwrap();
    let _ = std::fs::remove_dir_all("/tmp/states_chk");
    checkpoint.create_checkpoint("/tmp/states_chk").unwrap();
    println!("Checkpoint success!");
}
