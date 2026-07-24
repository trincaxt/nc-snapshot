//! Checkpoint creation using RocksDB Secondary Mode - 100% Rust native.
//!
//! This replaces the C# CheckpointBridge by using the RocksDB Rust API directly.
//! 
//! ## How It Works
//! 
//! 1. **Open as Secondary**: Opens the source DB as a secondary instance (read-only)
//! 2. **Catch Up**: Syncs with primary's WAL and memtable
//! 3. **Create Checkpoint**: Creates a consistent checkpoint with all data
//! 
//! This ensures the checkpoint includes data from the memtable that hasn't been
//! flushed to SST files yet, avoiding off-by-one errors in tip lookups.

use anyhow::{Context, Result};
use rocksdb::{DB, Options, checkpoint::Checkpoint};
use std::path::Path;
use std::time::Instant;

/// Creates a consistent checkpoint of a RocksDB using secondary mode.
/// 
/// This is equivalent to the C# CheckpointBridge's CheckpointSingleRocksDb(),
/// but implemented in pure Rust using the rocksdb crate.
/// 
/// # Algorithm
/// 
/// 1. Open source DB as secondary (allows reading while primary is running)
/// 2. Call try_catch_up_with_primary() to sync memtable/WAL data
/// 3. Create checkpoint from the synced secondary
/// 4. Validate checkpoint can be opened
/// 5. Clean up temporary files
/// 
/// # Why This Works
/// 
/// Hard-links copy SST files but lose memtable data. Secondary mode + catch_up
/// captures memtable data, ensuring tip lookups work correctly.
pub fn create_checkpoint_secondary(
    source_db: &Path,
    checkpoint_path: &Path,
) -> Result<()> {
    let start = Instant::now();
    
    if !source_db.exists() {
        anyhow::bail!("Source DB not found: {}", source_db.display());
    }

    // Create parent directory if needed
    if let Some(parent) = checkpoint_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Temporary directory for secondary instance
    let temp_secondary = checkpoint_path.parent()
        .unwrap_or_else(|| Path::new("/tmp"))
        .join(format!(".secondary-{}-{}", 
                     std::process::id(),
                     std::time::SystemTime::now()
                         .duration_since(std::time::UNIX_EPOCH)
                         .unwrap()
                         .as_secs()));

    std::fs::create_dir_all(&temp_secondary)?;

    let result = (|| -> Result<()> {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1: Open as secondary
        // ═══════════════════════════════════════════════════════════════
        let mut opts = Options::default();
        opts.set_paranoid_checks(false);
        opts.set_skip_stats_update_on_db_open(true);

        // Limit open SST files to avoid EMFILE (Too many open files) on large databases.
        // The primary (node) typically has thousands of SST files open, and when the
        // secondary opens the same files concurrently we can hit system/inner limits.
        // Setting max_open_files to a moderate value (0 = keep closed, >0 = max open)
        // avoids exhausting file descriptors.
        opts.set_max_open_files(500);

        // Use same BlockBasedOptions as exporter.rs for Libplanet RocksDB format compatibility.
        // The database was created by RocksDB 8.5.3 but we use librocksdb-sys 10.4.2 bundled
        // with rocksdb 0.24. Setting format_version(5) ensures backward-compatible reads.
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_format_version(5);
        opts.set_block_based_table_factory(&block_opts);

        let db = DB::open_as_secondary(&opts, source_db, &temp_secondary)
            .with_context(|| format!("Failed to open {} as secondary", source_db.display()))?;

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: Catch up with primary (sync memtable + WAL)
        // ═══════════════════════════════════════════════════════════════
        db.try_catch_up_with_primary()
            .context("Failed to catch up with primary")?;

        // ═══════════════════════════════════════════════════════════════
        // STEP 3: Create checkpoint from synced secondary
        // ═══════════════════════════════════════════════════════════════
        let checkpoint = Checkpoint::new(&db)?;
        
        // Remove destination if exists
        if checkpoint_path.exists() {
            std::fs::remove_dir_all(checkpoint_path)?;
        }

        checkpoint.create_checkpoint(checkpoint_path)
            .with_context(|| format!("Failed to create checkpoint at {}", checkpoint_path.display()))?;

        // ═══════════════════════════════════════════════════════════════
        // STEP 4: Validate checkpoint (optional but recommended)
        // ═══════════════════════════════════════════════════════════════
        validate_checkpoint_light(checkpoint_path)?;

        Ok(())
    })();

    // Cleanup temp directory
    if temp_secondary.exists() {
        let _ = std::fs::remove_dir_all(&temp_secondary);
    }

    result?;

    eprintln!("    ✓ checkpoint created in {:.1}s", start.elapsed().as_secs_f64());
    
    Ok(())
}

/// Lightweight validation: just tests if the checkpoint can be opened.
/// 
/// Uses the same options as exporter.rs for states/ compatibility,
/// including BlockBasedOptions with format_version(5) which is required
/// by Libplanet's RocksDB format.
fn validate_checkpoint_light(path: &Path) -> Result<()> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_paranoid_checks(false);
    opts.set_max_open_files(500);

    // Use same BlockBasedOptions as exporter.rs for states/ compatibility
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_format_version(5);
    opts.set_block_based_table_factory(&block_opts);

    let _db = DB::open_for_read_only(&opts, path, false)
        .with_context(|| format!("Checkpoint validation failed: cannot open {}", path.display()))?;
    
    Ok(())
}

/// Creates checkpoints for all epoch subdirectories matching a pattern.
/// 
/// Example: checkpoint_batch_epochs("block", source, dest, 20640)
/// will checkpoint block/epoch20640, block/epoch20641, etc.
pub fn checkpoint_batch_epochs(
    db_name: &str,
    source_root: &Path,
    checkpoint_root: &Path,
    epoch_limit: u64,
) -> Result<Vec<String>> {
    let source_db_root = source_root.join(db_name);
    
    if !source_db_root.exists() {
        return Ok(Vec::new());
    }

    // Find all epoch* directories
    let mut epoch_dirs = Vec::new();
    
    for entry in std::fs::read_dir(&source_db_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        
        if name_str.starts_with("epoch") && entry.path().is_dir() {
            // Parse epoch number
            if let Some(epoch_num_str) = name_str.strip_prefix("epoch") {
                if let Ok(epoch_num) = epoch_num_str.parse::<u64>() {
                    if epoch_num >= epoch_limit {
                        epoch_dirs.push((epoch_num, name_str.to_string()));
                    }
                }
            }
        }
    }

    // Sort by epoch number
    epoch_dirs.sort_by_key(|(num, _)| *num);

    eprintln!("  📦 Processing {} epochs (>= {})...", epoch_dirs.len(), epoch_limit);

    let mut checkpointed = Vec::new();
    
    for (idx, (_epoch_num, epoch_name)) in epoch_dirs.iter().enumerate() {
        eprintln!("      [{}/{}] {}", idx + 1, epoch_dirs.len(), epoch_name);
        
        let src = source_db_root.join(epoch_name);
        let dst = checkpoint_root.join(db_name).join(epoch_name);
        
        create_checkpoint_secondary(&src, &dst)?;
        checkpointed.push(epoch_name.clone());
    }

    Ok(checkpointed)
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore] // Requires a real RocksDB to test
    fn test_create_checkpoint_secondary() {
        // This would need a real DB to test properly
        // For now, just ensure the code compiles
    }
}
