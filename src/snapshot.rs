use crate::errors::SnapshotError;
use crate::types::{FingerprintDb, FileFingerprint, SnapshotConfig, SnapshotMode, SnapshotResult, BridgeResult};
use blake3::Hasher;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

const BUF_SIZE: usize = 64 * 1024 * 1024; // 64 MiB buffer
const READ_BUF: usize = 256 * 1024; // 256 KiB read chunks

/// State snapshot dirs — matches Nine Chronicles Snapshot tool.
/// Only indexes for block/tx, full for everything else.
const STATE_DIRS: &[&str] = &[
    "block/blockindex",
    "tx/txindex",
    "txbindex",
    "states",
    "chain",
    "blockcommit",
    "txexec",
];

/// Partition/base snapshot dirs — block + tx epoch data (excluding indexes).
const PARTITION_DIRS: &[&str] = &["block", "tx"];

/// Subdirs to exclude in partition mode (indexes go in state snapshot only).
const PARTITION_EXCLUDE: &[&str] = &["blockindex", "txindex"];

/// Get the list of directories to include based on snapshot mode.
#[allow(dead_code)]
fn get_mode_dirs(mode: &SnapshotMode) -> Option<Vec<String>> {
    match mode {
        SnapshotMode::State => Some(STATE_DIRS.iter().map(|s| s.to_string()).collect()),
        SnapshotMode::Partition => Some(PARTITION_DIRS.iter().map(|s| s.to_string()).collect()),
        SnapshotMode::Full => None, // None = walk entire source
    }
}

/// Get extra excludes based on snapshot mode.
#[allow(dead_code)]
fn get_mode_excludes(mode: &SnapshotMode) -> Vec<String> {
    match mode {
        SnapshotMode::Partition => PARTITION_EXCLUDE.iter().map(|s| s.to_string()).collect(),
        _ => Vec::new(),
    }
}

/// Implementa GetEpochLimit do C# oficial (3 branches).
/// Determina o limite de epochs para incluir no archive partition.
fn get_epoch_limit_from_metadata(latest_epoch: i32, current_metadata_epoch: i32, previous_metadata_epoch: i32) -> u64 {
    if latest_epoch == current_metadata_epoch {
        if latest_epoch == previous_metadata_epoch {
            return (previous_metadata_epoch - 1).max(0) as u64;
        }
        if previous_metadata_epoch == 0 {
            return (current_metadata_epoch - 1).max(0) as u64;
        }
        return previous_metadata_epoch as u64;
    }
    current_metadata_epoch as u64
}

/// Parse epoch number from a directory name like "epoch20536".
fn parse_epoch(name: &str) -> Option<u64> {
    name.strip_prefix("epoch").and_then(|n| n.parse::<u64>().ok())
}

/// Collect all files from source, respecting include/exclude filters and epoch limit.
fn collect_files(
    source: &Path,
    include: Option<&[String]>,
    exclude: &[String],
    epoch_limit: Option<u64>,
) -> Vec<(PathBuf, u64)> {
    let dirs_to_walk: Vec<PathBuf> = match include {
        Some(dirs) if !dirs.is_empty() => {
            dirs.iter()
                .map(|d| source.join(d))
                .filter(|p| p.exists())
                .collect()
        }
        _ => vec![source.to_path_buf()],
    };

    let mut files = Vec::new();

    for dir in &dirs_to_walk {
        for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.path();

            // Check exclude list against path components
            let excluded = path.components().any(|c| {
                exclude.iter().any(|ex| c.as_os_str() == ex.as_str())
            });
            if excluded {
                continue;
            }

            // Check epoch limit — skip epoch dirs below the limit
            if let Some(limit) = epoch_limit {
                let below_limit = path.components().any(|c| {
                    if let Some(epoch) = parse_epoch(&c.as_os_str().to_string_lossy()) {
                        epoch < limit
                    } else {
                        false
                    }
                });
                if below_limit {
                    continue;
                }
            }

            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            files.push((path.to_path_buf(), size));
        }
    }

    // Sort for deterministic archives
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

/// Load fingerprint database from disk.
fn load_fingerprints(path: &Path) -> Option<FingerprintDb> {
    let data = fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

/// Save fingerprint database to disk.
fn save_fingerprints(db: &FingerprintDb, path: &Path) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(db)?;
    fs::write(path, json)?;
    Ok(())
}

/// Check if a file has changed since the last fingerprint.
fn file_changed(path: &Path, size: u64, db: &FingerprintDb, source: &Path) -> bool {
    let rel = path.strip_prefix(source).unwrap_or(path);
    let key = rel.to_string_lossy().to_string();

    match db.files.get(&key) {
        Some(fp) => {
            if fp.size != size {
                return true;
            }
            if let Ok(meta) = fs::metadata(path) {
                if let Ok(mtime) = meta.modified() {
                    let secs = mtime
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    return secs != fp.mtime_secs;
                }
            }
            true
        }
        None => true,
    }
}

/// Create a fingerprint for a file.
fn make_fingerprint(path: &Path) -> FileFingerprint {
    let meta = fs::metadata(path).ok();
    let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let mtime_secs = meta
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    FileFingerprint { size, mtime_secs }
}

/// Create a single tar.zst archive with BLAKE3 manifest.
///
/// Core archive-creation logic extracted for reuse.
fn create_single_archive(
    source: &Path,
    output: &Path,
    include_dirs: Option<&[String]>,
    exclude: &[String],
    epoch_limit: Option<u64>,
    level: i32,
    threads: usize,
    json: bool,
    incremental: bool,
    mode_label: &str,
) -> anyhow::Result<SnapshotResult> {
    if !source.exists() {
        return Err(SnapshotError::SourceNotFound(source.to_path_buf()).into());
    }

    // Collect files
    if !json {
        eprint!("📂 Scanning files... ");
    }

    let files = collect_files(source, include_dirs, exclude, epoch_limit);
    let total_size: u64 = files.iter().map(|(_, s)| *s).sum();

    if !json {
        eprintln!(
            "{} files | {:.2} GiB",
            files.len(),
            total_size as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    }

    if files.is_empty() {
        anyhow::bail!("No files found to archive. Check --source and --include paths.");
    }

    // Load previous fingerprint for incremental mode
    let fingerprint_path = output
        .parent()
        .unwrap_or(Path::new("."))
        .join(".nc-snapshot-fingerprint.json");
    let prev_db = if incremental {
        load_fingerprints(&fingerprint_path)
    } else {
        None
    };

    // Filter for incremental: skip unchanged files
    let files: Vec<(PathBuf, u64)> = if let Some(ref db) = prev_db {
        let filtered: Vec<_> = files
            .into_iter()
            .filter(|(path, size)| file_changed(path, *size, db, source))
            .collect();
        if !json {
            eprintln!("📊 Incremental: {} changed files", filtered.len());
        }
        filtered
    } else {
        files
    };

    let archive_size: u64 = files.iter().map(|(_, s)| *s).sum();

    let start = Instant::now();

    // Create temp file in same directory for atomic rename
    let output_dir = output.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(output_dir)?;

    let temp_file = tempfile::NamedTempFile::new_in(output_dir)
        .map_err(|e| SnapshotError::io(e, "creating temp file"))?;

    let buf_writer = BufWriter::with_capacity(BUF_SIZE, temp_file.as_file().try_clone()?);

    // zstd encoder — multi-threaded
    let mut zstd_encoder = zstd::Encoder::new(buf_writer, level)?;
    zstd_encoder.multithread(threads as u32)?;

    let zstd_writer = zstd_encoder.auto_finish();
    let mut tar_builder = tar::Builder::new(zstd_writer);
    tar_builder.follow_symlinks(false);

    // Progress bar
    let pb = if !json {
        let pb = ProgressBar::new(archive_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) ETA {eta}",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        Some(pb)
    } else {
        None
    };

    // Build manifest of BLAKE3 checksums
    let mut manifest_lines: Vec<String> = Vec::with_capacity(files.len());
    let mut new_fingerprints = FingerprintDb {
        files: std::collections::HashMap::new(),
        created_at: chrono_now(),
    };
    let mut bytes_done: u64 = 0;
    let mut read_buf = vec![0u8; READ_BUF];

    for (path, size) in &files {
        let relative = path.strip_prefix(source).map_err(|_| {
            SnapshotError::io(
                std::io::Error::new(std::io::ErrorKind::Other, "strip_prefix failed"),
                format!("stripping prefix from {}", path.display()),
            )
        })?;

        // Compute BLAKE3 hash while reading
        let mut hasher = Hasher::new();
        let file = File::open(path)
            .map_err(|e| SnapshotError::io(e, format!("opening {}", path.display())))?;
        let mut reader = BufReader::with_capacity(READ_BUF, file);

        loop {
            let n = reader
                .read(&mut read_buf)
                .map_err(|e| SnapshotError::io(e, format!("reading {}", path.display())))?;
            if n == 0 {
                break;
            }
            hasher.update(&read_buf[..n]);
        }
        let hash = hasher.finalize();
        let hash_hex = hash.to_hex();

        manifest_lines.push(format!("{}  {}", hash_hex, relative.display()));

        // Add to tar
        tar_builder
            .append_path_with_name(path, relative)
            .map_err(|e| SnapshotError::io(e, format!("archiving {}", path.display())))?;

        // Record fingerprint
        let rel_str = relative.to_string_lossy().to_string();
        new_fingerprints
            .files
            .insert(rel_str, make_fingerprint(path));

        bytes_done += size;
        if let Some(ref pb) = pb {
            pb.set_position(bytes_done);
        }
    }

    // Append manifest as the last entry in the tar
    let manifest_content = manifest_lines.join("\n") + "\n";
    let manifest_bytes = manifest_content.as_bytes();
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar_builder
        .append_data(&mut header, "manifest.blake3", manifest_bytes)
        .map_err(|e| SnapshotError::io(e, "writing manifest"))?;

    // Finish tar
    tar_builder
        .finish()
        .map_err(|e| SnapshotError::io(e, "finalizing tar archive"))?;

    if let Some(ref pb) = pb {
        pb.finish_with_message("✅ Done!");
    }

    // Atomically move temp file to final output
    temp_file
        .persist(output)
        .map_err(|e| SnapshotError::io(e.error, "persisting output file"))?;

    let elapsed = start.elapsed();
    let compressed_size = fs::metadata(output)
        .map(|m| m.len())
        .unwrap_or(0);
    let throughput = archive_size as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let reduction = if archive_size > 0 {
        (1.0 - compressed_size as f64 / archive_size as f64) * 100.0
    } else {
        0.0
    };

    // Save fingerprints
    let _ = save_fingerprints(&new_fingerprints, &fingerprint_path);

    // Also write manifest alongside the archive
    let manifest_path = output.with_extension("blake3");
    let _ = fs::write(&manifest_path, &manifest_content);

    let result = SnapshotResult {
        output_path: output.display().to_string(),
        mode: mode_label.to_string(),
        original_size: archive_size,
        compressed_size,
        file_count: files.len(),
        elapsed_secs: elapsed.as_secs_f64(),
        throughput_mbps: throughput,
        reduction_pct: reduction,
        checksum_file: manifest_path.display().to_string(),
    };

    if !json {
        eprintln!();
        eprintln!("✅ Snapshot criado: {}", output.display());
        eprintln!("   Original   : {}", format_size(archive_size));
        eprintln!("   Comprimido : {}", format_size(compressed_size));
        eprintln!("   Redução    : {:.1}%", reduction);
        eprintln!("   Tempo      : {:.1}s", elapsed.as_secs_f64());
        eprintln!("   Throughput : {:.0} MB/s", throughput);
        eprintln!("   Manifest   : {}", manifest_path.display());
    }

    Ok(result)
}

/// Main snapshot creation function.
/// For Partition mode, creates TWO archives (partition + state) + metadata JSON.
pub fn create_snapshot(config: &SnapshotConfig, bridge_res: Option<BridgeResult>) -> anyhow::Result<SnapshotResult> {
    let source = &config.source;

    if !source.exists() {
        return Err(SnapshotError::SourceNotFound(source.clone()).into());
    }

    // Merge excludes: user-provided + mode-specific
    let mut all_excludes = config.exclude.clone();
    if config.mode == SnapshotMode::Partition {
        all_excludes.extend(PARTITION_EXCLUDE.iter().map(|s| s.to_string()));
    }

    // ── PARTITION MODE: create 2 archives + metadata ──────────────
    if config.mode == SnapshotMode::Partition {
        let output_dir = config.output.parent().unwrap_or(Path::new("."));

        // Dry run check
        if config.dry_run {
            if !config.json {
                eprintln!("🔍 Dry run — no archive created");
            }
            return Ok(SnapshotResult {
                output_path: String::new(),
                mode: "partition".to_string(),
                original_size: 0,
                compressed_size: 0,
                file_count: 0,
                elapsed_secs: 0.0,
                throughput_mbps: 0.0,
                reduction_pct: 0.0,
                checksum_file: String::new(),
            });
        }

        // 1. Partition archive: block + tx epochs (com PARTITION_EXCLUDE)
        //    Usa GetEpochLimit do bridge ou fallback para --epoch-limit
        let partition_dirs: Vec<String> = PARTITION_DIRS.iter().map(|s| s.to_string()).collect();
        let partition_epoch_limit = if let Some(ref res) = bridge_res {
            Some(get_epoch_limit_from_metadata(
                res.latest_epoch,
                res.current_metadata_block_epoch,
                res.previous_metadata_block_epoch,
            ))
        } else {
            config.epoch_limit
        };
        let partition_dir = output_dir.join("partition");
        fs::create_dir_all(&partition_dir)?;
        let partition_output = if let Some(ref res) = bridge_res {
            partition_dir.join(format!("{}.tar.zst", res.partition_base_filename))
        } else {
            let fname = config.output.file_name().unwrap_or(std::ffi::OsStr::new("partition.tar.zst"));
            partition_dir.join(fname)
        };

        if !config.json {
            eprintln!("📦 Creating partition snapshot (block + tx epochs)...");
        }
        let _partition_result = create_single_archive(
            source,
            &partition_output,
            Some(&partition_dirs),
            &all_excludes, // inclui PARTITION_EXCLUDE (blockindex, txindex)
            partition_epoch_limit,
            config.level,
            config.threads,
            config.json,
            config.incremental,
            "partition",
        )?;

        // 2. State archive: block/blockindex, tx/txindex, states, chain, etc.
        //    SEM PARTITION_EXCLUDE — precisa incluir blockindex e txindex!
        let state_dirs: Vec<String> = STATE_DIRS.iter().map(|s| s.to_string()).collect();
        let state_dir = output_dir.join("state");
        fs::create_dir_all(&state_dir)?;
        let state_output = state_dir.join("state_latest.tar.zst");

        if !config.json {
            eprintln!("📦 Creating state snapshot (indexes + states)...");
        }
        let state_result = create_single_archive(
            source,
            &state_output,
            Some(&state_dirs),
            &config.exclude, // SÓ excludes do usuário, SEM PARTITION_EXCLUDE
            None, // state snapshot nao usa epoch_limit
            config.level,
            config.threads,
            config.json,
            config.incremental,
            "state",
        )?;

        // 3. Metadata JSON (USANDO bridge_res!)
        if let Some(res) = bridge_res {
            let meta_dir = output_dir.join("metadata");
            fs::create_dir_all(&meta_dir)?;
            let meta_path = meta_dir.join(format!("{}.json", res.partition_base_filename));
            fs::write(&meta_path, &res.stringfy_metadata)?;
            if !config.json {
                eprintln!("   Metadata   : {}", meta_path.display());
            }
        }

        return Ok(state_result);
    }

    // ── STATE / FULL MODE: create single archive ──────────────────

    // Dry run check
    if config.dry_run {
        if !config.json {
            eprintln!("🔍 Dry run — no archive created");
        }
        return Ok(SnapshotResult {
            output_path: String::new(),
            mode: config.mode.to_string(),
            original_size: 0,
            compressed_size: 0,
            file_count: 0,
            elapsed_secs: 0.0,
            throughput_mbps: 0.0,
            reduction_pct: 0.0,
            checksum_file: String::new(),
        });
    }

    let mode_dirs = if !config.include.is_empty() {
        Some(config.include.clone())
    } else {
        match config.mode {
            SnapshotMode::State => Some(STATE_DIRS.iter().map(|s| s.to_string()).collect()),
            SnapshotMode::Full => None,
            SnapshotMode::Partition => unreachable!(), // handled above
        }
    };

    let result = create_single_archive(
        source,
        &config.output,
        mode_dirs.as_deref(),
        &all_excludes,
        config.epoch_limit,
        config.level,
        config.threads,
        config.json,
        config.incremental,
        config.mode.to_string().as_str(),
    )?;

    // Metadata only for Partition mode (handled above)

    Ok(result)
}

fn format_size(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    }
}

fn chrono_now() -> String {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_epoch_valid() {
        assert_eq!(parse_epoch("epoch20536"), Some(20536));
        assert_eq!(parse_epoch("epoch0"), Some(0));
        assert_eq!(parse_epoch("epoch999999"), Some(999999));
    }

    #[test]
    fn test_parse_epoch_edge_cases() {
        assert_eq!(parse_epoch("epoch"), None);
        assert_eq!(parse_epoch("epoc"), None);
        assert_eq!(parse_epoch(""), None);
        assert_eq!(parse_epoch("20536"), None);
        assert_eq!(parse_epoch("epoch12a34"), None); // parse error
    }

    #[test]
    fn test_get_epoch_limit_same_epoch() {
        // latest == current_metadata == previous_metadata
        assert_eq!(get_epoch_limit_from_metadata(100, 100, 100), 99);
    }

    #[test]
    fn test_get_epoch_limit_previous_zero() {
        // latest == current_metadata, previous == 0
        assert_eq!(get_epoch_limit_from_metadata(100, 100, 0), 99);
    }

    #[test]
    fn test_get_epoch_limit_previous_nonzero() {
        // latest == current_metadata, previous > 0 and < latest
        assert_eq!(get_epoch_limit_from_metadata(100, 100, 95), 95);
    }

    #[test]
    fn test_get_epoch_limit_different() {
        // latest != current_metadata
        assert_eq!(get_epoch_limit_from_metadata(200, 100, 50), 100);
    }

    #[test]
    fn test_get_epoch_limit_zero_edge() {
        assert_eq!(get_epoch_limit_from_metadata(0, 0, 0), 0);
        assert_eq!(get_epoch_limit_from_metadata(1, 0, 0), 0);
    }

    // ── collect_files() tests: partition mode core ─────────────

    #[test]
    fn test_collect_files_no_filter() {
        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join("a.txt"), "a").unwrap();
        fs::write(dir.path().join("b.txt"), "bb").unwrap();

        let files = collect_files(dir.path(), None, &[], None);
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_collect_files_epoch_limit_below() {
        let dir = tempfile::TempDir::new().unwrap();

        // Cria block/epoch10 e block/epoch11, cada um com 1 arquivo
        let e10 = dir.path().join("block/epoch10");
        let e11 = dir.path().join("block/epoch11");
        std::fs::create_dir_all(&e10).unwrap();
        std::fs::create_dir_all(&e11).unwrap();
        fs::write(e10.join("data.sst"), "x").unwrap();
        fs::write(e11.join("data.sst"), "y").unwrap();

        // epoch_limit=11: só epochs >= 11 entram
        let exclude: Vec<String> = vec![];
        let files = collect_files(dir.path(), None, &exclude, Some(11));

        // Só epoch11 deve aparecer
        let paths: Vec<_> = files.iter().map(|(p, _)| p.to_string_lossy().to_string()).collect();
        assert_eq!(files.len(), 1, "esperado 1 arquivo (epoch11), got {}: {:?}", files.len(), paths);
        assert!(paths[0].contains("epoch11"), "arquivo deve estar em epoch11, got: {}", paths[0]);
        assert!(!paths[0].contains("epoch10"), "epoch10 não deve aparecer");
    }

    #[test]
    fn test_collect_files_epoch_limit_all_above() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("block/epoch5")).unwrap();
        std::fs::create_dir_all(dir.path().join("block/epoch6")).unwrap();
        fs::write(dir.path().join("block/epoch5/data.sst"), "x").unwrap();
        fs::write(dir.path().join("block/epoch6/data.sst"), "y").unwrap();

        // epoch_limit=0: todos entram
        let exclude: Vec<String> = vec![];
        let files = collect_files(dir.path(), None, &exclude, Some(0));
        assert_eq!(files.len(), 2, "epoch_limit=0 deve incluir todos");
    }

    #[test]
    fn test_collect_files_epoch_limit_none() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("block/epoch10")).unwrap();
        fs::write(dir.path().join("block/epoch10/data.sst"), "x").unwrap();

        // Sem epoch_limit: inclui tudo
        let exclude: Vec<String> = vec![];
        let files = collect_files(dir.path(), None, &exclude, None);
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_collect_files_with_exclude() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("block/epoch10")).unwrap();
        std::fs::create_dir_all(dir.path().join("tx/epoch10")).unwrap();
        std::fs::create_dir_all(dir.path().join("chain")).unwrap();
        fs::write(dir.path().join("block/epoch10/data.sst"), "x").unwrap();
        fs::write(dir.path().join("tx/epoch10/data.sst"), "y").unwrap();
        // Arquivo fora de epochs
        fs::write(dir.path().join("chain/CURRENT"), "z").unwrap();

        // Exclui "chain" (simula partition mode que exclui blockindex/txindex)
        let exclude = vec!["chain".to_string()];
        let files = collect_files(dir.path(), None, &exclude, None);
        let paths: Vec<_> = files.iter().map(|(p, _)| p.to_string_lossy().to_string()).collect();

        assert_eq!(files.len(), 2, "deve incluir block/epoch10 e tx/epoch10, got: {:?}", paths);
        assert!(paths.iter().any(|p| p.contains("block/epoch10")));
        assert!(paths.iter().any(|p| p.contains("tx/epoch10")));
        assert!(!paths.iter().any(|p| p.contains("chain")));
    }

    #[test]
    fn test_collect_files_with_include_dirs() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("block/epoch10")).unwrap();
        std::fs::create_dir_all(dir.path().join("states")).unwrap();
        fs::write(dir.path().join("block/epoch10/data.sst"), "x").unwrap();
        fs::write(dir.path().join("states/000001.sst"), "y").unwrap();

        // Inclui só "block" (simula partition mode que pega block/ + tx/ separado)
        let include = vec!["block".to_string()];
        let files = collect_files(dir.path(), Some(&include), &[], None);

        assert_eq!(files.len(), 1, "só block/ deve ser incluído");
        assert!(files[0].0.to_string_lossy().contains("block/epoch10"));
    }

    #[test]
    fn test_collect_files_deterministic_order() {
        let dir = tempfile::TempDir::new().unwrap();

        fs::write(dir.path().join("z.txt"), "z").unwrap();
        fs::write(dir.path().join("a.txt"), "a").unwrap();
        fs::write(dir.path().join("m.txt"), "m").unwrap();

        let files = collect_files(dir.path(), None, &[], None);
        let names: Vec<_> = files.iter().map(|(p, _)| {
            p.file_name().unwrap().to_string_lossy().to_string()
        }).collect();

        assert_eq!(names, vec!["a.txt", "m.txt", "z.txt"], "deve estar ordenado");
    }
}
