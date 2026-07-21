//! Shared I/O utilities for the export binary format.
//!
//! Format: `[key:32b][val_len:4b LE][val:Nb]` repeated.
//!
//! Also provides `hex_to_hash32()` for hex→`[u8;32]` conversion.

/// 32-byte hash length (SHA-256 / BLAKE3 / Libplanet node key)
pub const HASH_LEN: usize = 32;

/// Lê um registro em `offset`: retorna (key, value, next_offset).
///
/// Formato: key(32) + value_len(4 LE) + value(value_len).
/// Retorna None se o record não couber completamente no arquivo.
#[inline]
pub fn read_record(data: &[u8], offset: usize) -> Option<([u8; HASH_LEN], &[u8], usize)> {
    if offset + 36 > data.len() {
        return None;
    }

    let key: [u8; HASH_LEN] = data[offset..offset + HASH_LEN].try_into().unwrap();
    let lo = offset + HASH_LEN;
    let vlen = u32::from_le_bytes([data[lo], data[lo + 1], data[lo + 2], data[lo + 3]]) as usize;
    let vo = lo + 4;

    if vo + vlen > data.len() {
        return None;
    }

    Some((key, &data[vo..vo + vlen], vo + vlen))
}

/// Converte hex string para `[u8; 32]`.
pub fn hex_to_hash32(hex: &str) -> anyhow::Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    if hex.len() != 64 {
        anyhow::bail!("Invalid hex length: expected 64, got {}", hex.len());
    }
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── read_record tests ────────────────────────────────────────

    #[test]
    fn test_read_record_empty() {
        let result = read_record(b"", 0);
        assert!(result.is_none());
    }

    #[test]
    fn test_read_record_incomplete_key() {
        let data = [0u8; 31];
        let result = read_record(&data, 0);
        assert!(result.is_none());
    }

    #[test]
    fn test_read_record_incomplete_len() {
        let data = [0u8; 33];
        let result = read_record(&data, 0);
        assert!(result.is_none());
    }

    #[test]
    fn test_read_record_incomplete_value() {
        let mut data = vec![0u8; 32 + 4 + 3];
        data[32..36].copy_from_slice(&5u32.to_le_bytes());
        let result = read_record(&data, 0);
        assert!(result.is_none());
    }

    #[test]
    fn test_read_record_single() {
        let key = [0xDE; 32];
        let value = b"hello";
        let mut data = key.to_vec();
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(value);

        let result = read_record(&data, 0);
        assert!(result.is_some());
        let (k, v, next) = result.unwrap();
        assert_eq!(k, key);
        assert_eq!(v, value);
        assert_eq!(next, 32 + 4 + 5); // key(32) + vlen(4) + val(5)
    }

    #[test]
    fn test_read_record_empty_value() {
        let key = [0x00; 32];
        let value = b"";
        let mut data = key.to_vec();
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());

        let result = read_record(&data, 0);
        assert!(result.is_some());
        let (k, v, next) = result.unwrap();
        assert_eq!(k, key);
        assert_eq!(v.len(), 0);
        assert_eq!(next, 32 + 4);
    }

    #[test]
    fn test_read_record_skip_value() {
        let key1 = [0x01; 32];
        let key2 = [0x02; 32];
        let mut data = key1.to_vec();
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"abc");
        data.extend_from_slice(&key2);
        data.extend_from_slice(&4u32.to_le_bytes());
        data.extend_from_slice(b"defg");

        let (_, _, next1) = read_record(&data, 0).unwrap();
        assert_eq!(next1, 32 + 4 + 3);

        let (k2, v2, next2) = read_record(&data, next1).unwrap();
        assert_eq!(k2, key2);
        assert_eq!(v2, b"defg");
        assert_eq!(next2, 32 + 4 + 3 + 32 + 4 + 4);
    }

    // ── hex_to_hash32 tests ──────────────────────────────────────

    #[test]
    fn test_hex_to_hash32_valid() {
        let hex = "ab".repeat(32);
        let result = hex_to_hash32(&hex);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 32);
    }

    #[test]
    fn test_hex_to_hash32_invalid_length() {
        let hex = "ab".repeat(31); // 62 chars
        let result = hex_to_hash32(&hex);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("64"));
    }

    #[test]
    fn test_hex_to_hash32_invalid_chars() {
        let hex = "gg".repeat(32);
        let result = hex_to_hash32(&hex);
        assert!(result.is_err());
    }

    #[test]
    fn test_hex_to_hash32_roundtrip() {
        let original = [0xDEu8; 32];
        let hex_str = hex::encode(original);
        let result = hex_to_hash32(&hex_str).unwrap();
        assert_eq!(result, original);
    }
}
