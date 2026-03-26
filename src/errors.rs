use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("Node appears to be running — lock held on: {lock_path}")]
    NodeRunning { lock_path: PathBuf },

    #[error("I/O error on {context}: {source}")]
    Io {
        source: std::io::Error,
        context: String,
    },

    #[error("Compression error: {0}")]
    Compression(#[from] std::io::Error),

    #[error("Checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },

    #[error("Manifest not found in archive")]
    ManifestNotFound,

    #[error("Source directory does not exist: {0}")]
    SourceNotFound(PathBuf),

    #[error("Archive file does not exist: {0}")]
    ArchiveNotFound(PathBuf),
}

impl SnapshotError {
    pub fn io(source: std::io::Error, context: impl Into<String>) -> Self {
        Self::Io {
            source,
            context: context.into(),
        }
    }
}
