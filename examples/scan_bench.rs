//! Mede as varreduras 1 e 2 do GC por varredura, sem BFS nem escrita.
//!
//!   cargo run --release --example scan_bench -- <pasta states/ do checkpoint>
//!
//! Use o `states/` de um checkpoint (o do `.nc-snapshot-live-checkpoint` existe enquanto um
//! `nc-snapshot create --live` está rodando) ou uma cópia. Acompanhe com `free -m` e `iostat -dxm 5`.

use nc_snapshot::gc_scan::{build_graph_scan, build_key_index};
use nc_snapshot::gc_scan_rocks::RocksSource;
use std::path::Path;
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).expect("uso: scan_bench <pasta states/>");
    let src = RocksSource::open(Path::new(&path))?;

    let t = Instant::now();
    let (index, skipped) = build_key_index(&src)?;
    eprintln!("== varredura 1 (chaves): {:.1}s | {} chaves | {} ignoradas", t.elapsed().as_secs_f64(), index.len(), skipped);

    let t = Instant::now();
    let graph = build_graph_scan(&src, &index)?;
    eprintln!("== varredura 2 (grafo): {:.1}s | {} arestas", t.elapsed().as_secs_f64(), graph.edge_count());
    Ok(())
}
