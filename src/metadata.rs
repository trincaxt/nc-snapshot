//! Blockchain metadata generation (100% Rust, zero C# bridge).
//!
//! Lê o header do bloco tip de um checkpoint RocksDB e gera o JSON de metadata
//! byte-idêntico ao que o bridge C# original produzia.

use anyhow::Context;
use std::path::Path;
use std::fs;
use crate::chain_reader;
use crate::types::BridgeResult;

/// 24 horas em segundos — usado pra calcular epoch do timestamp do bloco
const EPOCH_UNIT_SECONDS: i64 = 86400;

/// Gera o metadata JSON em Rust puro (byte-idêntico ao C# bridge).
pub fn generate_metadata_rust(
    checkpoint_base: &Path,
    apv: &str,
    block_before: i32,
    output_dir: &Path,
    _json_output: bool,
) -> anyhow::Result<(String, String, i32)> {
    // 1. Lê informações completas do header
    let header = chain_reader::read_block_header_from_checkpoint(checkpoint_base, block_before as u64)?;

    // 2. Calcula latest epoch (do timestamp do bloco)
    let timestamp_parsed = chrono::DateTime::parse_from_rfc3339(&header.timestamp)
        .context("Failed to parse timestamp")?;
    let latest_epoch = (timestamp_parsed.timestamp() / EPOCH_UNIT_SECONDS) as i32;

    // 3. Lê epochs do metadata anterior
    let metadata_dir = output_dir.join("metadata");
    let current_metadata_block_epoch = get_metadata_epoch(&metadata_dir, "BlockEpoch");
    let current_metadata_tx_epoch = get_metadata_epoch(&metadata_dir, "TxEpoch");
    let previous_metadata_block_epoch = get_metadata_epoch(&metadata_dir, "PreviousBlockEpoch");

    // 4. Calcula previous epochs
    let (previous_block_epoch, previous_tx_epoch) = if current_metadata_block_epoch == latest_epoch {
        (previous_metadata_block_epoch, previous_metadata_block_epoch)
    } else {
        (current_metadata_block_epoch, current_metadata_tx_epoch)
    };

    // 5. Calcula block/tx epochs
    let (block_epoch, tx_epoch) = if current_metadata_block_epoch == 0 && current_metadata_tx_epoch == 0 {
        (latest_epoch - 1, latest_epoch - 1)
    } else {
        (latest_epoch, latest_epoch)
    };

    // 6. Monta o metadata
    let metadata = crate::types::BlockMetadata {
        index: header.index,
        timestamp: header.timestamp.clone(),
        state_root_hash: hex::encode(header.state_root_hash),
        previous_hash: hex::encode(header.previous_hash),
        tx_hash: header.tx_hash.map(|h| hex::encode(h)),
        apv: apv.to_string(),
        block_epoch,
        tx_epoch,
        previous_block_epoch,
        previous_tx_epoch,
    };

    // 7. Serializa para JSON (sem formatação, igual ao C#)
    let metadata_json = serde_json::to_string(&metadata)
        .context("Failed to serialize metadata")?;

    // 8. Nome do arquivo partition
    let partition_filename = get_partition_base_filename(
        current_metadata_block_epoch,
        current_metadata_tx_epoch,
        latest_epoch,
    );

    Ok((metadata_json, partition_filename, latest_epoch))
}

/// Gera metadata 100% Rust puro (sem C# bridge).
pub fn fetch_metadata_hybrid(
    source: &Path,
    apv: &str,
    block_before: i32,
    _mode: &str,
    output_dir: &Path,
    json_output: bool,
) -> anyhow::Result<BridgeResult> {
    if !json_output {
        eprintln!("🟢 Fetching metadata from checkpoint (no LOCK conflicts)...");
    }

    let (metadata_json, partition_filename, latest_epoch) =
        generate_metadata_rust(source, apv, block_before, output_dir, json_output)?;

    let current_metadata_block_epoch = get_metadata_epoch(&output_dir.join("metadata"), "BlockEpoch");
    let previous_metadata_block_epoch = get_metadata_epoch(&output_dir.join("metadata"), "PreviousBlockEpoch");

    Ok(BridgeResult {
        success: true,
        error: None,
        partition_base_filename: partition_filename,
        state_base_filename: "state_latest".to_string(),
        latest_epoch,
        current_metadata_block_epoch,
        previous_metadata_block_epoch,
        stringfy_metadata: metadata_json,
    })
}

/// Lê o epoch do metadata anterior (metadata/*.json mais recente).
/// Retorna 0 se não houver metadata anterior.
pub fn get_metadata_epoch(metadata_dir: &Path, epoch_type: &str) -> i32 {
    if !metadata_dir.exists() {
        return 0;
    }

    match fs::read_dir(metadata_dir) {
        Ok(entries) => {
            let mut json_files: Vec<_> = entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == "json")
                        .unwrap_or(false)
                })
                .collect();

            if json_files.is_empty() {
                return 0;
            }

            // Ordena por mtime (mais recente primeiro)
            json_files.sort_by(|a, b| {
                let a_meta = a.metadata().ok();
                let b_meta = b.metadata().ok();
                let a_time = a_meta.and_then(|m| m.modified().ok());
                let b_time = b_meta.and_then(|m| m.modified().ok());
                b_time.cmp(&a_time)
            });

            // Lê o primeiro (mais recente)
            if let Some(file) = json_files.first() {
                if let Ok(content) = fs::read_to_string(file.path()) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                        if let Some(epoch) = json.get(epoch_type).and_then(|v| v.as_i64()) {
                            return epoch as i32;
                        }
                    }
                }
            }

            0
        }
        Err(_) => 0,
    }
}

/// Calcula o nome base do arquivo partition.
pub fn get_partition_base_filename(
    current_metadata_block_epoch: i32,
    current_metadata_tx_epoch: i32,
    latest_epoch: i32,
) -> String {
    if current_metadata_block_epoch == 0 && current_metadata_tx_epoch == 0 {
        format!("snapshot-{}-{}", latest_epoch - 1, latest_epoch - 1)
    } else {
        format!("snapshot-{}-{}", latest_epoch, latest_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_get_partition_base_filename_both_zero() {
        let name = get_partition_base_filename(0, 0, 10);
        assert_eq!(name, "snapshot-9-9");
    }

    #[test]
    fn test_get_partition_base_filename_current_match() {
        let name = get_partition_base_filename(10, 10, 10);
        assert_eq!(name, "snapshot-10-10");
    }

    #[test]
    fn test_get_partition_base_filename_no_previous() {
        let name = get_partition_base_filename(10, 10, 15);
        assert_eq!(name, "snapshot-15-15");
    }

    #[test]
    fn test_get_metadata_epoch_with_tempdir() {
        let dir = TempDir::new().unwrap();
        let meta_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&meta_dir).unwrap();

        // Write a metadata JSON
        let meta = serde_json::json!({
            "BlockEpoch": 20642,
            "TxEpoch": 20642,
            "PreviousBlockEpoch": 20641,
        });
        let meta_path = meta_dir.join("snapshot-20642-20642.json");
        std::fs::write(&meta_path, serde_json::to_string(&meta).unwrap()).unwrap();

        assert_eq!(get_metadata_epoch(&meta_dir, "BlockEpoch"), 20642);
        assert_eq!(get_metadata_epoch(&meta_dir, "TxEpoch"), 20642);
        assert_eq!(get_metadata_epoch(&meta_dir, "PreviousBlockEpoch"), 20641);
    }

    #[test]
    fn test_get_metadata_epoch_empty_dir() {
        let dir = TempDir::new().unwrap();
        assert_eq!(get_metadata_epoch(&dir.path().join("nonexistent"), "BlockEpoch"), 0);
    }
}
