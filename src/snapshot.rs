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
fn get_mode_dirs(mode: &SnapshotMode) -> Option<Vec<String>> {
    match mode {
        SnapshotMode::State => Some(STATE_DIRS.iter().map(|s| s.to_string()).collect()),
        SnapshotMode::Partition => Some(PARTITION_DIRS.iter().map(|s| s.to_string()).collect()),
        SnapshotMode::Full => None, // None = walk entire source
    }
}

/// Get extra excludes based on snapshot mode.
fn get_mode_excludes(mode: &SnapshotMode) -> Vec<String> {
    match mode {
        SnapshotMode::Partition => PARTITION_EXCLUDE.iter().map(|s| s.to_string()).collect(),
        _ => Vec::new(),
    }
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

/// Main snapshot creation function.
pub fn create_snapshot(config: &SnapshotConfig, bridge_res: Option<BridgeResult>) -> anyhow::Result<SnapshotResult> {
    let source = &config.source;

    if !source.exists() {
        return Err(SnapshotError::SourceNotFound(source.clone()).into());
    }

    // Determine directories based on mode
    let mode_dirs = if !config.include.is_empty() {
        // User override takes priority
        Some(config.include.clone())
    } else {
        match config.mode {
            SnapshotMode::State => Some(STATE_DIRS.iter().map(|s| s.to_string()).collect()),
            SnapshotMode::Partition => Some(PARTITION_DIRS.iter().map(|s| s.to_string()).collect()),
            SnapshotMode::Full => None,
        }
    };

    // Merge excludes: user-provided + mode-specific
    let mut all_excludes = config.exclude.clone();
    if config.mode == SnapshotMode::Partition {
        all_excludes.extend(PARTITION_EXCLUDE.iter().map(|s| s.to_string()));
    }

    // Collect files
    if !config.json {
        eprint!("📂 Scanning files... ");
    }

    let files = collect_files(
        source,
        mode_dirs.as_deref(),
        &all_excludes,
        config.epoch_limit,
    );
    let total_size: u64 = files.iter().map(|(_, s)| *s).sum();

    if !config.json {
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
    let fingerprint_path = config
        .output
        .parent()
        .unwrap_or(Path::new("."))
        .join(".nc-snapshot-fingerprint.json");
    let prev_db = if config.incremental {
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
        if !config.json {
            eprintln!("📊 Incremental: {} changed files", filtered.len());
        }
        filtered
    } else {
        files
    };

    let archive_size: u64 = files.iter().map(|(_, s)| *s).sum();

    // Dry run — just report
    if config.dry_run {
        if !config.json {
            eprintln!("🔍 Dry run — no archive created");
            eprintln!(
                "   Would archive: {} files, {:.2} GiB",
                files.len(),
                archive_size as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }
        return Ok(SnapshotResult {
            output_path: config.output.display().to_string(),
            mode: config.mode.to_string(),
            original_size: archive_size,
            compressed_size: 0,
            file_count: files.len(),
            elapsed_secs: 0.0,
            throughput_mbps: 0.0,
            reduction_pct: 0.0,
            checksum_file: String::new(),
        });
    }

    let start = Instant::now();

    // Create temp file in same directory for atomic rename
    let output_dir = config.output.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(output_dir)?;

    let mut temp_file = tempfile::NamedTempFile::new_in(output_dir)
        .map_err(|e| SnapshotError::io(e, "creating temp file"))?;

    let buf_writer = BufWriter::with_capacity(BUF_SIZE, temp_file.as_file().try_clone()?);

    // zstd encoder — multi-threaded
    let mut zstd_encoder = zstd::Encoder::new(buf_writer, config.level)?;
    zstd_encoder.multithread(config.threads as u32)?;

    let zstd_writer = zstd_encoder.auto_finish();
    let mut tar_builder = tar::Builder::new(zstd_writer);
    tar_builder.follow_symlinks(false);

    // Progress bar
    let pb = if !config.json {
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
        .persist(&config.output)
        .map_err(|e| SnapshotError::io(e.error, "persisting output file"))?;

    let elapsed = start.elapsed();
    let compressed_size = fs::metadata(&config.output)
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
    let manifest_path = config.output.with_extension("blake3");
    let _ = fs::write(&manifest_path, &manifest_content);

    // Write Metadata if available
    if let Some(res) = bridge_res {
        let meta_dir = config.output.parent().unwrap_or(Path::new(".")).join("metadata");
        let _ = fs::create_dir_all(&meta_dir);
        let meta_name = if config.mode == SnapshotMode::Partition { &res.partition_base_filename } else { "state_latest" };
        let meta_path = meta_dir.join(format!("{}.json", meta_name));
        let _ = fs::write(&meta_path, &res.stringfy_metadata);
        if !config.json {
            eprintln!("   Metadata   : {}", meta_path.display());
        }
    }

    let result = SnapshotResult {
        output_path: config.output.display().to_string(),
        mode: config.mode.to_string(),
        original_size: archive_size,
        compressed_size,
        file_count: files.len(),
        elapsed_secs: elapsed.as_secs_f64(),
        throughput_mbps: throughput,
        reduction_pct: reduction,
        checksum_file: manifest_path.display().to_string(),
    };

    if !config.json {
        eprintln!();
        eprintln!("✅ Snapshot criado: {}", config.output.display());
        eprintln!("   Modo       : {}", config.mode);
        eprintln!("   Original   : {}", format_size(archive_size));
        eprintln!("   Comprimido : {}", format_size(compressed_size));
        eprintln!("   Redução    : {:.1}%", reduction);
        eprintln!("   Tempo      : {:.1}s", elapsed.as_secs_f64());
        eprintln!("   Throughput : {:.0} MB/s", throughput);
        eprintln!("   Manifest   : {}", manifest_path.display());
    }

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
