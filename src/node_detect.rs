use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Find all RocksDB LOCK files under the given source directory.
pub fn find_lock_files(source: &Path) -> Vec<PathBuf> {
    WalkDir::new(source)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && e.file_name() == "LOCK")
        .map(|e| e.path().to_path_buf())
        .collect()
}

/// Check if any RocksDB LOCK file is held by another process.
/// Returns the list of actively locked file paths.
pub fn check_node_running(source: &Path) -> Vec<PathBuf> {
    let lock_files = find_lock_files(source);
    let mut locked = Vec::new();

    for lock_path in &lock_files {
        if is_file_locked(lock_path) {
            locked.push(lock_path.clone());
        }
    }

    locked
}

/// Try to acquire an exclusive non-blocking flock.
/// If it fails with EWOULDBLOCK, the file is locked by another process.
fn is_file_locked(path: &Path) -> bool {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return false,
    };

    let fd = file.as_raw_fd();
    let result = unsafe { libc_flock(fd, libc_LOCK_EX | libc_LOCK_NB) };

    if result == 0 {
        // We got the lock — release it immediately
        unsafe {
            libc_flock(fd, libc_LOCK_UN);
        }
        false
    } else {
        // Lock is held by another process
        true
    }
}

// Inline libc constants to avoid adding libc as a dependency
const libc_LOCK_EX: i32 = 2;
const libc_LOCK_NB: i32 = 4;
const libc_LOCK_UN: i32 = 8;

unsafe fn libc_flock(fd: i32, operation: i32) -> i32 {
    unsafe {
        extern "C" {
            fn flock(fd: i32, operation: i32) -> i32;
        }
        flock(fd, operation)
    }
}
