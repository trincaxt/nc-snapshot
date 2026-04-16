//! Chain cleanup: stale directory removal and pre-snapshot cleanup.
//!
//! Nine Chronicles blockchain stores accumulate stale directories over time
//! (e.g., from protocol upgrades or interrupted operations). This module
//! provides filesystem-level cleanup of known stale paths.
//!
//! ## Stale Directories Removed
//!
//! The following directories are removed if they exist under the store path:
//! - `9c-main/`     — Legacy chain data from older protocol versions
//! - `state/`       — Old state directory (replaced by `states/`)
//! - `stateref/`    — Stale state references
//! - `state_hashes/` — Cached state hash lookups
//! - `new_states/`  — Intermediate state data from migrations
//! - `blockpercept/` — Block perception cache
//! - `stagedtx/`    — Staged transaction cache
//!
//! ## Chain Pruning (Future)
//!
//! Non-canonical chain pruning requires reading LiteDB index collections.
//! Currently delegated to the bridge binary. A future version may implement
//! direct LiteDB parsing for offline chain cleanup.

use anyhow::{Context, Result};
use std::path::Path;
use tracing;

/// Directories to remove during cleanup.
/// These are known stale/legacy paths from Nine Chronicles protocol evolution.
const STALE_DIRECTORIES: &[&str] = &[
    "9c-main",
    "state",
    "stateref",
    "state_hashes",
    "new_states",
    "blockpercept",
    "stagedtx",
];

/// Result of a cleanup operation.
#[derive(Debug, Clone)]
pub struct CleanupResult {
    /// Names of directories that were successfully removed.
    pub dirs_removed: Vec<String>,
    /// Estimated bytes freed by the cleanup.
    pub bytes_freed: u64,
    /// Directories that were checked but did not exist (no action needed).
    pub dirs_not_found: Vec<String>,
}

impl CleanupResult {
    fn new() -> Self {
        Self {
            dirs_removed: Vec::new(),
            bytes_freed: 0,
            dirs_not_found: Vec::new(),
        }
    }

    /// Whether any cleanup was actually performed.
    pub fn has_cleaned(&self) -> bool {
        !self.dirs_removed.is_empty()
    }
}

impl std::fmt::Display for CleanupResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.dirs_removed.is_empty() {
            writeln!(f, "No stale directories found. Store is clean.")?;
        } else {
            writeln!(f, "=== Cleanup Summary ===")?;
            writeln!(f, "Directories removed: {}", self.dirs_removed.len())?;
            for dir in &self.dirs_removed {
                writeln!(f, "  - {}", dir)?;
            }
            writeln!(f, "Space freed: {}", format_bytes(self.bytes_freed))?;
        }
        if !self.dirs_not_found.is_empty() {
            writeln!(
                f,
                "Directories already absent: {}",
                self.dirs_not_found.len()
            )?;
        }
        Ok(())
    }
}

/// Format a byte count into a human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Calculate the total size of a directory recursively.
///
/// Returns 0 if the path does not exist or cannot be read.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let ty = entry.file_type();
            if let Ok(ft) = ty {
                if ft.is_dir() {
                    total = total.saturating_add(dir_size(&entry.path()));
                } else if ft.is_file() {
                    if let Ok(meta) = entry.metadata() {
                        total = total.saturating_add(meta.len());
                    }
                }
            }
        }
    }
    total
}

/// Remove a directory and its contents if it exists.
///
/// Returns the estimated bytes freed (calculated before removal).
/// Logs the removal at info level.
///
/// # Errors
///
/// Returns an error if the directory exists but cannot be removed
/// (e.g., permission denied, filesystem error).
fn remove_dir_if_exists(path: &Path) -> Result<Option<u64>> {
    if !path.exists() {
        return Ok(None);
    }

    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| path.display().to_string());

    tracing::info!("Calculating size of stale directory: {}", name);
    let size = dir_size(path);

    tracing::info!("Removing stale directory: {} ({})", name, format_bytes(size));
    std::fs::remove_dir_all(path)
        .with_context(|| format!("Failed to remove directory: {}", path.display()))?;

    tracing::info!("Successfully removed: {}", name);
    Ok(Some(size))
}

/// Clean all known stale directories from the Nine Chronicles store path.
///
/// This is a pure filesystem operation that removes directories known to be
/// stale or legacy from Nine Chronicles protocol evolution. It does NOT modify
/// the active chain data (`chain/`, `states/`, `block/`).
///
/// # Arguments
///
/// * `store_path` — Root of the Nine Chronicles blockchain store directory.
///
/// # Returns
///
/// A `CleanupResult` summarizing what was removed and how much space was freed.
///
/// # Errors
///
/// Returns an error if:
/// - The store path does not exist
/// - A stale directory exists but cannot be removed (permission denied, etc.)
pub fn clean_stale_directories(store_path: &Path) -> Result<CleanupResult> {
    if !store_path.exists() {
        anyhow::bail!(
            "Store path does not exist: {}. Cannot perform cleanup.",
            store_path.display()
        );
    }

    if !store_path.is_dir() {
        anyhow::bail!(
            "Store path is not a directory: {}. Cannot perform cleanup.",
            store_path.display()
        );
    }

    tracing::info!(
        "Starting stale directory cleanup at: {}",
        store_path.display()
    );
    tracing::info!(
        "Checking {} known stale directory patterns...",
        STALE_DIRECTORIES.len()
    );

    let mut result = CleanupResult::new();

    for dir_name in STALE_DIRECTORIES {
        let dir_path = store_path.join(dir_name);
        match remove_dir_if_exists(&dir_path) {
            Ok(Some(bytes)) => {
                result.dirs_removed.push(dir_name.to_string());
                result.bytes_freed = result.bytes_freed.saturating_add(bytes);
            }
            Ok(None) => {
                result.dirs_not_found.push(dir_name.to_string());
                tracing::debug!("Stale directory not present (OK): {}", dir_name);
            }
            Err(e) => {
                tracing::error!("Failed to remove {}: {}", dir_name, e);
                return Err(e);
            }
        }
    }

    tracing::info!("Cleanup complete.");
    Ok(result)
}

/// Clean old snapshot artifacts before creating new ones.
///
/// Removes previously generated snapshot directories/files that may conflict
/// with a new snapshot operation:
/// - `states_pruned/` — Previous pruned states output
///
/// # Arguments
///
/// * `store_path` — Root of the Nine Chronicles blockchain store directory.
///
/// # Returns
///
/// The number of bytes freed by cleaning old snapshot artifacts.
pub fn clean_old_snapshots(store_path: &Path) -> Result<u64> {
    if !store_path.exists() {
        anyhow::bail!("Store path does not exist: {}", store_path.display());
    }

    let mut bytes_freed = 0u64;

    // Clean old pruned states
    let pruned_path = store_path.join("states_pruned");
    if let Some(size) = remove_dir_if_exists(&pruned_path)? {
        bytes_freed = bytes_freed.saturating_add(size);
    }

    Ok(bytes_freed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cleanup_empty_store() {
        let tmp = TempDir::new().unwrap();
        let result = clean_stale_directories(tmp.path()).unwrap();

        assert!(!result.has_cleaned());
        assert_eq!(result.dirs_removed.len(), 0);
        assert_eq!(result.bytes_freed, 0);
        assert_eq!(result.dirs_not_found.len(), STALE_DIRECTORIES.len());
    }

    #[test]
    fn test_cleanup_with_stale_dirs() {
        let tmp = TempDir::new().unwrap();

        // Create some stale directories with dummy files
        for dir_name in &["9c-main", "state", "stagedtx"] {
            let dir_path = tmp.path().join(dir_name);
            std::fs::create_dir_all(&dir_path).unwrap();
            std::fs::write(dir_path.join("dummy.bin"), b"test data 12345").unwrap();
        }

        // Create an active directory that should NOT be removed
        let active_dir = tmp.path().join("chain");
        std::fs::create_dir_all(&active_dir).unwrap();
        std::fs::write(active_dir.join("db"), b"active chain data").unwrap();

        let result = clean_stale_directories(tmp.path()).unwrap();

        assert!(result.has_cleaned());
        assert_eq!(result.dirs_removed.len(), 3);
        assert!(result.bytes_freed > 0);
        assert!(result.dirs_not_found.len() == STALE_DIRECTORIES.len() - 3);

        // Verify stale dirs are gone
        assert!(!tmp.path().join("9c-main").exists());
        assert!(!tmp.path().join("state").exists());
        assert!(!tmp.path().join("stagedtx").exists());

        // Verify active dir is untouched
        assert!(active_dir.exists());
    }

    #[test]
    fn test_cleanup_nonexistent_path() {
        let result = clean_stale_directories(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_clean_old_snapshots() {
        let tmp = TempDir::new().unwrap();

        // Create a stale snapshot directory
        let pruned = tmp.path().join("states_pruned");
        std::fs::create_dir_all(&pruned).unwrap();
        std::fs::write(pruned.join("CURRENT"), b"test").unwrap();

        let bytes = clean_old_snapshots(tmp.path()).unwrap();
        assert!(bytes > 0);
        assert!(!pruned.exists());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(512), "512 bytes");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1048576), "1.00 MiB");
        assert_eq!(format_bytes(1073741824), "1.00 GiB");
    }

    #[test]
    fn test_dir_size() {
        let tmp = TempDir::new().unwrap();
        let subdir = tmp.path().join("sub");
        std::fs::create_dir_all(&subdir).unwrap();
        std::fs::write(tmp.path().join("a.txt"), b"hello").unwrap(); // 5 bytes
        std::fs::write(subdir.join("b.txt"), b"world!!").unwrap(); // 7 bytes

        let size = dir_size(tmp.path());
        assert_eq!(size, 12);
    }

    #[test]
    fn test_cleanup_result_display() {
        let result = CleanupResult::new();
        let display = format!("{}", result);
        assert!(display.contains("No stale directories found"));

        let mut result = CleanupResult::new();
        result.dirs_removed.push("test-dir".to_string());
        result.bytes_freed = 1048576;
        let display = format!("{}", result);
        assert!(display.contains("Directories removed: 1"));
        assert!(display.contains("test-dir"));
        assert!(display.contains("1.00 MiB"));
    }
}
