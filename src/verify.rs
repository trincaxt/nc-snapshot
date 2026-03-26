use crate::types::{VerifyMismatch, VerifyResult};
use blake3::Hasher;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

/// Verify an archive's integrity by checking BLAKE3 checksums from embedded manifest.
pub fn verify_archive(archive_path: &Path, json: bool) -> anyhow::Result<VerifyResult> {
    if !archive_path.exists() {
        anyhow::bail!("Archive not found: {}", archive_path.display());
    }

    if !json {
        eprintln!("🔍 Verifying: {}", archive_path.display());
    }

    let file = File::open(archive_path)?;
    let reader = BufReader::with_capacity(64 * 1024 * 1024, file);
    let decoder = zstd::Decoder::new(reader)?;
    let mut archive = tar::Archive::new(decoder);

    // First pass: find and parse manifest, collect file data
    let mut manifest: HashMap<String, String> = HashMap::new();
    let mut file_hashes: HashMap<String, String> = HashMap::new();
    let mut file_count = 0;
    let mut read_buf = vec![0u8; 256 * 1024];

    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        let path = entry.path()?.to_string_lossy().to_string();

        if path == "manifest.blake3" {
            // Parse manifest
            let mut content = String::new();
            entry.read_to_string(&mut content)?;
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Some((hash, file_path)) = line.split_once("  ") {
                    manifest.insert(file_path.to_string(), hash.to_string());
                }
            }
        } else {
            // Hash this file
            let mut hasher = Hasher::new();
            loop {
                let n = entry.read(&mut read_buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&read_buf[..n]);
            }
            let hash = hasher.finalize().to_hex().to_string();
            file_hashes.insert(path, hash);
            file_count += 1;
        }
    }

    if manifest.is_empty() {
        anyhow::bail!("No manifest.blake3 found in archive — cannot verify");
    }

    if !json {
        eprintln!("📋 Manifest: {} entries, Archive: {} files", manifest.len(), file_count);
    }

    // Compare hashes
    let mut files_ok = 0;
    let mut mismatches = Vec::new();

    let pb = if !json {
        let pb = ProgressBar::new(manifest.len() as u64);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} Verifying [{bar:40.cyan/blue}] {pos}/{len}",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        Some(pb)
    } else {
        None
    };

    for (path, expected_hash) in &manifest {
        match file_hashes.get(path) {
            Some(actual_hash) => {
                if actual_hash == expected_hash {
                    files_ok += 1;
                } else {
                    mismatches.push(VerifyMismatch {
                        path: path.clone(),
                        expected: expected_hash.clone(),
                        actual: actual_hash.clone(),
                    });
                }
            }
            None => {
                mismatches.push(VerifyMismatch {
                    path: path.clone(),
                    expected: expected_hash.clone(),
                    actual: "MISSING".to_string(),
                });
            }
        }
        if let Some(ref pb) = pb {
            pb.inc(1);
        }
    }

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    let result = VerifyResult {
        archive_path: archive_path.display().to_string(),
        files_checked: manifest.len(),
        files_ok,
        files_failed: mismatches.len(),
        mismatches,
    };

    if !json {
        if result.files_failed == 0 {
            eprintln!("✅ All {} files verified OK", result.files_ok);
        } else {
            eprintln!(
                "❌ {} files FAILED, {} OK",
                result.files_failed, result.files_ok
            );
            for m in &result.mismatches {
                eprintln!("   FAIL: {} (expected: {}, got: {})", m.path, m.expected, m.actual);
            }
        }
    }

    Ok(result)
}
