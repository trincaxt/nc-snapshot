use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Snapshot mode matching the Nine Chronicles tool behavior.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotMode {
    /// State snapshot: block/blockindex, tx/txindex, txbindex, states, chain, blockcommit, txexec
    State,
    /// Partition/base snapshot: block + tx epoch dirs (excluding indexes)
    Partition,
    /// Full snapshot: everything in the store directory
    Full,
}

impl std::str::FromStr for SnapshotMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "state" => Ok(Self::State),
            "partition" | "base" => Ok(Self::Partition),
            "full" => Ok(Self::Full),
            _ => Err(format!("Invalid mode '{}'. Use: state, partition, full", s)),
        }
    }
}

impl std::fmt::Display for SnapshotMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::State => write!(f, "state"),
            Self::Partition => write!(f, "partition"),
            Self::Full => write!(f, "full"),
        }
    }
}

/// Configuration for snapshot creation.
pub struct SnapshotConfig {
    pub source: PathBuf,
    pub output: PathBuf,
    pub level: i32,
    pub threads: usize,
    pub exclude: Vec<String>,
    pub include: Vec<String>,
    pub mode: SnapshotMode,
    pub epoch_limit: Option<u64>,
    pub force: bool,
    pub json: bool,
    pub dry_run: bool,
    pub incremental: bool,
    pub apv: Option<String>,
    pub block_before: i32,
}

/// Result of a snapshot creation.
#[derive(Serialize)]
pub struct SnapshotResult {
    pub output_path: String,
    pub mode: String,
    pub original_size: u64,
    pub compressed_size: u64,
    pub file_count: usize,
    pub elapsed_secs: f64,
    pub throughput_mbps: f64,
    pub reduction_pct: f64,
    pub checksum_file: String,
}

/// Result of archive verification.
#[derive(Serialize)]
pub struct VerifyResult {
    pub archive_path: String,
    pub files_checked: usize,
    pub files_ok: usize,
    pub files_failed: usize,
    pub mismatches: Vec<VerifyMismatch>,
}

#[derive(Serialize)]
pub struct VerifyMismatch {
    pub path: String,
    pub expected: String,
    pub actual: String,
}

/// File fingerprint for incremental snapshots.
#[derive(Serialize, Deserialize, Clone)]
pub struct FileFingerprint {
    pub size: u64,
    pub mtime_secs: i64,
}

/// Fingerprint database.
#[derive(Serialize, Deserialize, Default)]
pub struct FingerprintDb {
    pub files: HashMap<String, FileFingerprint>,
    pub created_at: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct BridgeResult {
    pub success: bool,
    pub error: Option<String>,
    pub partition_base_filename: String,
    pub state_base_filename: String,
    pub latest_epoch: i32,
    pub current_metadata_block_epoch: i32,
    pub previous_metadata_block_epoch: i32,
    pub stringfy_metadata: String,
}
