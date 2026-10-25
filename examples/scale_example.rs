use nc_snapshot::{gc_filter::run_gc_filter, gc_index::*};
use std::io::Write;
use std::time::Instant;

fn h(n: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    for b in out.iter_mut() {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        let mut v = (x >> 24) as u8;
        if v == b':' { v = b';'; }
        *b = v;
    }
    out
}

fn main() {
    let n: u64 = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(3_000_000);
    let dir = std::env::temp_dir().join("idx_scale");
    std::fs::create_dir_all(&dir).unwrap();
    let exp = dir.join("export.bin");
    let idx = index_path_for(&exp);
    let (old_out, bits) = (dir.join("live.bin"), dir.join("live.bits"));

    let mut s: u64 = 0x1234_5678_9ABC_DEF1;
    let mut rnd = move || { s ^= s << 13; s ^= s >> 7; s ^= s << 17; s };
    let mut recs: Vec<([u8; 32], Vec<[u8; 32]>)> = (0..n).map(|i| {
        let k = 1 + (rnd() % 3) as usize; // 1..3 filhos (supercrítico)
        (h(i), (0..k).map(|_| h(rnd() % (n + n / 10))).collect()) // ~9% dangling
    }).collect();
    recs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut f = std::io::BufWriter::new(std::fs::File::create(&exp).unwrap());
    for (k, ch) in &recs {
        let mut v = b"d1:t1:n".to_vec();
        for c in ch { v.extend_from_slice(b"32:"); v.extend_from_slice(c); }
        f.write_all(k).unwrap(); f.write_all(&(v.len() as u32).to_le_bytes()).unwrap(); f.write_all(&v).unwrap();
    }
    drop(f); drop(recs);
    println!("export: {} nós, {:.2} GB", n, std::fs::metadata(&exp).unwrap().len() as f64 / 1e9);

    let t = Instant::now();
    build_index_from_export(&exp, &idx).unwrap();
    println!("build_index: {:.2}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    run_gc_filter(&exp, vec![h(0)], &old_out).unwrap();
    println!("BFS fixpoint antigo: {:.2}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    let live = run_gc_filter_indexed(&exp, &idx, vec![h(0)], &bits).unwrap();
    println!("BFS indexado: {:.2}s", t.elapsed().as_secs_f64());

    let file = std::fs::File::open(&exp).unwrap();
    let mm = unsafe { memmap2::Mmap::map(&file).unwrap() };
    let index = Index::open(&idx, &mm[..]).unwrap();
    compare_with_keys_file(&index, &live, &old_out).unwrap();
    println!("bitset: {} bytes vs live_keys.bin: {} bytes",
        std::fs::metadata(&bits).unwrap().len(), std::fs::metadata(&old_out).unwrap().len());
}
