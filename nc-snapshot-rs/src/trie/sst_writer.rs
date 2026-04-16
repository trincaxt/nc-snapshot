//! High-performance SST file writer for creating a clean RocksDB from reachable nodes.
//!
//! Instead of using `db.put()` for each key (which goes through WAL + memtable),
//! we use `SstFileWriter` to generate sorted SST files directly, then ingest them
//! into the target DB in one shot. This is 5-10x faster for bulk writes.
//!
//! Since RocksDB iterates keys in sorted order (bytewise comparator), and we want
//! to copy a subset of those keys, the output is naturally sorted — no manual
//! sorting needed!

use crate::trie::bloom::ReachableSet;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{
    DBWithThreadMode, IngestExternalFileOptions, IteratorMode, MultiThreaded,
    Options, SstFileWriter,
};
use std::path::{Path, PathBuf};

use std::time::Instant;

/// Statistics from an SST write operation.
#[derive(Debug, Clone)]
pub struct SstWriteStats {
    /// Number of keys copied to the new DB.
    pub keys_copied: u64,
    /// Number of keys skipped (unreachable).
    pub keys_skipped: u64,
    /// Number of non-node keys copied (metadata, etc.).
    pub metadata_keys_copied: u64,
    /// Total keys scanned in source DB.
    pub total_scanned: u64,
    /// Number of SST files generated.
    pub sst_files_created: u32,
    /// Bytes written to SST files.
    pub bytes_written: u64,
    /// Time taken for the SST generation + ingest.
    pub elapsed_secs: f64,
}

/// Maximum number of key-value pairs per SST file.
/// 500K entries ≈ 150-250 MB per file depending on value sizes.
const KEYS_PER_SST: u64 = 500_000;

/// Build RocksDB options compatible with a source DB for SST file creation.
fn build_sst_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    // Match typical Libplanet/RocksDB defaults
    opts.set_max_open_files(256);
    opts.set_max_background_jobs(4);
    // Optimize for bulk loading
    opts.set_max_write_buffer_number(4);
    opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MB
    // Disable WAL for bulk load (we don't need crash recovery during build)
    opts.set_manual_wal_flush(true);
    opts
}

/// Create a clean RocksDB at `target_path` containing only reachable nodes.
///
/// This reads the source DB sequentially (ReadOnly), checks each key against
/// the Bloom filter, and writes matching keys to SST files which are then
/// ingested into the target DB.
///
/// # Arguments
/// - `source_db`: Open RocksDB handle (ReadOnly) for the original states/
/// - `target_path`: Path for the new clean DB (e.g., `states_new/`)
/// - `reachable`: Bloom filter set of reachable node hashes
/// - `json`: If true, suppress progress output
///
/// # Returns
/// Statistics about the write operation.
pub fn write_reachable_to_new_db(
    source_db: &DBWithThreadMode<MultiThreaded>,
    target_path: &Path,
    reachable: &ReachableSet,
    json: bool,
) -> Result<SstWriteStats> {
    let start = Instant::now();

    // Ensure target directory exists and is empty
    if target_path.exists() {
        std::fs::remove_dir_all(target_path)
            .with_context(|| format!("Failed to clean target dir: {:?}", target_path))?;
    }
    std::fs::create_dir_all(target_path)
        .with_context(|| format!("Failed to create target dir: {:?}", target_path))?;

    // Create a temporary directory for SST files
    let sst_dir = target_path.join("_sst_temp");
    std::fs::create_dir_all(&sst_dir)?;

    let opts = build_sst_options();

    // Counters
    let mut keys_copied: u64 = 0;
    let mut keys_skipped: u64 = 0;
    let mut metadata_keys_copied: u64 = 0;
    let mut total_scanned: u64 = 0;
    let mut sst_files_created: u32 = 0;
    let mut bytes_written: u64 = 0;
    let mut keys_in_current_sst: u64 = 0;

    // Track generated SST file paths
    let mut sst_paths: Vec<PathBuf> = Vec::new();

    // Create first SST writer
    let mut writer = SstFileWriter::create(&opts);
    let first_sst = sst_dir.join(format!("batch_{:05}.sst", sst_files_created));
    writer
        .open(&first_sst)
        .context("Failed to open first SST file")?;
    sst_paths.push(first_sst);

    // Progress bar
    let pb = if !json {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template(
                    "{spinner:.green} [{elapsed_precise}] Copying reachable nodes... \
                     scanned={msg}",
                )
                .unwrap(),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(200));
        Some(pb)
    } else {
        None
    };

    // Iterate the source DB sequentially (already sorted by key!)
    let iter = source_db.iterator(IteratorMode::Start);
    for item in iter {
        let (key, value) = item.context("RocksDB iteration error during SST copy")?;
        total_scanned += 1;

        if total_scanned % 500_000 == 0 {
            if let Some(ref pb) = pb {
                pb.set_message(format!(
                    "{} (copied: {}, skipped: {}, meta: {})",
                    total_scanned, keys_copied, keys_skipped, metadata_keys_copied
                ));
            }
        }

        let should_copy = if key.len() == 32 {
            // Node key: check if reachable
            let hash: [u8; 32] = key
                .as_ref()
                .try_into()
                .expect("Key length already checked");
            reachable.contains(&hash)
        } else {
            // Non-node key (metadata, internal RocksDB data): always copy
            true
        };

        if should_copy {
            // Write to current SST file
            writer
                .put(&key, &value)
                .with_context(|| {
                    format!(
                        "Failed to write key to SST (key_len={}, sst={})",
                        key.len(),
                        sst_files_created
                    )
                })?;

            bytes_written += key.len() as u64 + value.len() as u64;
            keys_in_current_sst += 1;

            if key.len() == 32 {
                keys_copied += 1;
            } else {
                metadata_keys_copied += 1;
            }

            // Rotate SST file when it gets large enough
            if keys_in_current_sst >= KEYS_PER_SST {
                writer.finish().context("Failed to finish SST file")?;
                sst_files_created += 1;
                keys_in_current_sst = 0;

                let next_sst = sst_dir.join(format!("batch_{:05}.sst", sst_files_created));
                writer = SstFileWriter::create(&opts);
                writer
                    .open(&next_sst)
                    .with_context(|| format!("Failed to open SST file: {:?}", next_sst))?;
                sst_paths.push(next_sst);
            }
        } else {
            keys_skipped += 1;
        }
    }

    // Finish the last SST file
    if keys_in_current_sst > 0 {
        writer.finish().context("Failed to finish final SST file")?;
        sst_files_created += 1;
    } else {
        // Empty last file — remove it from the list
        writer.finish().ok(); // May fail if empty, that's fine
        if let Some(last) = sst_paths.last() {
            let _ = std::fs::remove_file(last);
        }
        sst_paths.pop();
    }

    if let Some(ref pb) = pb {
        pb.finish_with_message(format!(
            "Done! scanned={}, copied={}, skipped={}, sst_files={}",
            total_scanned, keys_copied, keys_skipped, sst_files_created
        ));
    }

    tracing::info!(
        "SST generation complete: {} files, {} keys copied, {} keys skipped",
        sst_files_created,
        keys_copied,
        keys_skipped,
    );

    // Now open the target DB and ingest all SST files
    if !sst_paths.is_empty() {
        if !json {
            tracing::info!(
                "Ingesting {} SST files into target DB at {:?}...",
                sst_paths.len(),
                target_path,
            );
        }

        let target_db = DBWithThreadMode::<MultiThreaded>::open(&opts, target_path)
            .context("Failed to open target DB for ingest")?;

        let mut ingest_opts = IngestExternalFileOptions::default();
        ingest_opts.set_move_files(true); // Move instead of copy (faster)

        let sst_path_strs: Vec<&Path> = sst_paths.iter().map(|p| p.as_path()).collect();
        target_db
            .ingest_external_file_opts(&ingest_opts, sst_path_strs)
            .context("Failed to ingest SST files into target DB")?;

        drop(target_db);

        tracing::info!("SST ingest complete.");
    }

    // Clean up temp SST directory
    let _ = std::fs::remove_dir_all(&sst_dir);

    let elapsed = start.elapsed().as_secs_f64();

    Ok(SstWriteStats {
        keys_copied,
        keys_skipped,
        metadata_keys_copied,
        total_scanned,
        sst_files_created,
        bytes_written,
        elapsed_secs: elapsed,
    })
}

/// Calculate the total size of a directory (recursively).
pub fn dir_size(path: &Path) -> u64 {
    fn walk(dir: &Path) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    total += entry.metadata().map(|m| m.len()).unwrap_or(0);
                } else if path.is_dir() {
                    total += walk(&path);
                }
            }
        }
        total
    }
    walk(path)
}

/// Check available disk space at a given path.
/// Returns available bytes, or 0 if unable to determine.
pub fn available_disk_space(path: &Path) -> u64 {
    // Use statvfs on Unix via raw FFI (same pattern as node_detect.rs)
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;

        // statvfs struct layout (linux x86_64)
        #[repr(C)]
        struct Statvfs {
            f_bsize: u64,
            f_frsize: u64,
            f_blocks: u64,
            f_bfree: u64,
            f_bavail: u64,
            f_files: u64,
            f_ffree: u64,
            f_favail: u64,
            f_fsid: u64,
            f_flag: u64,
            f_namemax: u64,
            __f_spare: [i32; 6],
        }

        extern "C" {
            fn statvfs(path: *const i8, buf: *mut Statvfs) -> i32;
        }

        let c_path = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(p) => p,
            Err(_) => return 0,
        };

        unsafe {
            let mut stat = MaybeUninit::<Statvfs>::uninit();
            if statvfs(c_path.as_ptr(), stat.as_mut_ptr()) == 0 {
                let stat = stat.assume_init();
                stat.f_bavail * stat.f_frsize
            } else {
                0
            }
        }
    }

    #[cfg(not(unix))]
    {
        0
    }
}
