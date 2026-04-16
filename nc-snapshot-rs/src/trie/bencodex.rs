//! Bencodex decoder for Libplanet's node encoding.
//!
//! Bencodex is a superset of Bencode with additional types:
//! - Null:    `n`
//! - Bool:    `t` (true) / `f` (false)
//! - Integer: `i<number>e` (decimal, big-endian, arbitrary precision)
//! - Bytes:   `<len>:<data>` (raw bytes, length-prefixed)
//! - Text:    `u<len>:<data>` (UTF-8 string, length-prefixed with 'u' prefix)
//! - List:    `l<items>e`
//! - Dict:    `d<key-value pairs>e` (keys sorted: bytes before text, then lexicographic)
//!
//! Reference: https://bencodex.org/

use anyhow::{bail, Context, Result};
use num_bigint::BigInt;
use std::fmt;

/// A decoded Bencodex value.
#[derive(Clone, PartialEq, Eq)]
pub enum BencodexValue {
    Null,
    Bool(bool),
    Integer(BigInt),
    Bytes(Vec<u8>),
    Text(String),
    List(Vec<BencodexValue>),
    Dict(Vec<(BencodexKey, BencodexValue)>),
}

/// Dictionary keys in Bencodex are either Bytes or Text.
/// Bytes sort before Text; within the same type, lexicographic order.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BencodexKey {
    Bytes(Vec<u8>),
    Text(String),
}

impl fmt::Debug for BencodexValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BencodexValue::Null => write!(f, "Null"),
            BencodexValue::Bool(b) => write!(f, "Bool({b})"),
            BencodexValue::Integer(i) => write!(f, "Int({i})"),
            BencodexValue::Bytes(b) => {
                if b.len() <= 64 {
                    write!(f, "Bytes({})", hex::encode(b))
                } else {
                    write!(f, "Bytes({}... [{} bytes])", hex::encode(&b[..32]), b.len())
                }
            }
            BencodexValue::Text(s) => write!(f, "Text({s:?})"),
            BencodexValue::List(items) => {
                write!(f, "List[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{item:?}")?;
                }
                write!(f, "]")
            }
            BencodexValue::Dict(entries) => {
                write!(f, "Dict{{")?;
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{k:?}: {v:?}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl fmt::Debug for BencodexKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BencodexKey::Bytes(b) => write!(f, "b:{}", hex::encode(b)),
            BencodexKey::Text(s) => write!(f, "t:{s:?}"),
        }
    }
}

impl BencodexValue {
    /// Try to extract as a list.
    pub fn as_list(&self) -> Option<&[BencodexValue]> {
        match self {
            BencodexValue::List(items) => Some(items),
            _ => None,
        }
    }

    /// Try to extract as raw bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            BencodexValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to extract as text.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            BencodexValue::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Check if this is Null.
    pub fn is_null(&self) -> bool {
        matches!(self, BencodexValue::Null)
    }
}

/// Decoder state: tracks position in the byte slice.
struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn peek(&self) -> Result<u8> {
        if self.pos >= self.data.len() {
            bail!("Unexpected end of Bencodex data at position {}", self.pos);
        }
        Ok(self.data[self.pos])
    }

    fn advance(&mut self, n: usize) {
        self.pos += n;
    }

    fn read_byte(&mut self) -> Result<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            bail!(
                "Tried to read {} bytes at position {} but only {} remaining",
                n, self.pos, self.remaining()
            );
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    /// Parse a decimal integer until we hit a non-digit (or ':' for length prefix).
    fn read_decimal_usize(&mut self) -> Result<usize> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos == start {
            bail!("Expected decimal number at position {}", start);
        }
        let s = std::str::from_utf8(&self.data[start..self.pos])
            .context("Invalid UTF-8 in decimal number")?;
        s.parse::<usize>()
            .with_context(|| format!("Failed to parse decimal number: {s:?}"))
    }

    /// Decode one Bencodex value from current position.
    fn decode_value(&mut self) -> Result<BencodexValue> {
        let tag = self.peek()?;
        match tag {
            // Null
            b'n' => {
                self.advance(1);
                Ok(BencodexValue::Null)
            }
            // Bool true
            b't' => {
                self.advance(1);
                Ok(BencodexValue::Bool(true))
            }
            // Bool false
            b'f' => {
                self.advance(1);
                Ok(BencodexValue::Bool(false))
            }
            // Integer: i<number>e
            b'i' => {
                self.advance(1); // skip 'i'
                let start = self.pos;
                // Read until 'e', allowing leading '-'
                while self.pos < self.data.len() && self.data[self.pos] != b'e' {
                    self.pos += 1;
                }
                if self.pos >= self.data.len() {
                    bail!("Unterminated integer at position {}", start);
                }
                let num_str = std::str::from_utf8(&self.data[start..self.pos])
                    .context("Invalid UTF-8 in integer")?;
                let value: BigInt = num_str.parse()
                    .with_context(|| format!("Invalid integer: {num_str:?}"))?;
                self.advance(1); // skip 'e'
                Ok(BencodexValue::Integer(value))
            }
            // Text: u<len>:<data>
            b'u' => {
                self.advance(1); // skip 'u'
                let len = self.read_decimal_usize()?;
                let colon = self.read_byte()?;
                if colon != b':' {
                    bail!("Expected ':' after text length at position {}", self.pos - 1);
                }
                let raw = self.read_bytes(len)?;
                let text = std::str::from_utf8(raw)
                    .with_context(|| format!("Invalid UTF-8 in text of length {len}"))?;
                Ok(BencodexValue::Text(text.to_string()))
            }
            // List: l<items>e
            b'l' => {
                self.advance(1); // skip 'l'
                let mut items = Vec::new();
                while self.peek()? != b'e' {
                    items.push(self.decode_value()?);
                }
                self.advance(1); // skip 'e'
                Ok(BencodexValue::List(items))
            }
            // Dict: d<key-value>e
            b'd' => {
                self.advance(1); // skip 'd'
                let mut entries = Vec::new();
                while self.peek()? != b'e' {
                    let key = self.decode_key()?;
                    let value = self.decode_value()?;
                    entries.push((key, value));
                }
                self.advance(1); // skip 'e'
                Ok(BencodexValue::Dict(entries))
            }
            // Bytes: <len>:<data>
            b'0'..=b'9' => {
                let len = self.read_decimal_usize()?;
                let colon = self.read_byte()?;
                if colon != b':' {
                    bail!("Expected ':' after bytes length at position {}", self.pos - 1);
                }
                let raw = self.read_bytes(len)?;
                Ok(BencodexValue::Bytes(raw.to_vec()))
            }
            _ => {
                bail!(
                    "Unknown Bencodex tag byte 0x{:02x} ({:?}) at position {}",
                    tag,
                    tag as char,
                    self.pos
                );
            }
        }
    }

    /// Decode a dictionary key (must be Bytes or Text).
    fn decode_key(&mut self) -> Result<BencodexKey> {
        let tag = self.peek()?;
        match tag {
            // Text key
            b'u' => {
                self.advance(1);
                let len = self.read_decimal_usize()?;
                let colon = self.read_byte()?;
                if colon != b':' {
                    bail!("Expected ':' after text key length");
                }
                let raw = self.read_bytes(len)?;
                let text = std::str::from_utf8(raw)
                    .context("Invalid UTF-8 in dict text key")?;
                Ok(BencodexKey::Text(text.to_string()))
            }
            // Bytes key
            b'0'..=b'9' => {
                let len = self.read_decimal_usize()?;
                let colon = self.read_byte()?;
                if colon != b':' {
                    bail!("Expected ':' after bytes key length");
                }
                let raw = self.read_bytes(len)?;
                Ok(BencodexKey::Bytes(raw.to_vec()))
            }
            _ => {
                bail!(
                    "Invalid Bencodex dict key tag 0x{:02x} at position {}. Keys must be Bytes or Text.",
                    tag, self.pos
                );
            }
        }
    }
}

/// Decode a Bencodex-encoded byte slice into a `BencodexValue`.
///
/// # Example
/// ```
/// use nc_snapshot_rs::trie::bencodex::{decode, BencodexValue};
/// let data = b"n";
/// let val = decode(data).unwrap();
/// assert_eq!(val, BencodexValue::Null);
/// ```
pub fn decode(data: &[u8]) -> Result<BencodexValue> {
    if data.is_empty() {
        bail!("Empty Bencodex data");
    }
    let mut decoder = Decoder::new(data);
    let value = decoder.decode_value()
        .context("Failed to decode Bencodex value")?;

    // Ensure all data was consumed
    if decoder.pos != data.len() {
        tracing::warn!(
            "Bencodex decode: {} trailing bytes after value (total={}, consumed={})",
            data.len() - decoder.pos,
            data.len(),
            decoder.pos,
        );
    }

    Ok(value)
}

/// Encode a Bencodex value back to bytes. Useful for computing hashes.
pub fn encode(value: &BencodexValue) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_into(value, &mut buf);
    buf
}

fn encode_into(value: &BencodexValue, buf: &mut Vec<u8>) {
    match value {
        BencodexValue::Null => buf.push(b'n'),
        BencodexValue::Bool(true) => buf.push(b't'),
        BencodexValue::Bool(false) => buf.push(b'f'),
        BencodexValue::Integer(i) => {
            buf.push(b'i');
            buf.extend_from_slice(i.to_string().as_bytes());
            buf.push(b'e');
        }
        BencodexValue::Bytes(b) => {
            buf.extend_from_slice(b.len().to_string().as_bytes());
            buf.push(b':');
            buf.extend_from_slice(b);
        }
        BencodexValue::Text(s) => {
            buf.push(b'u');
            buf.extend_from_slice(s.len().to_string().as_bytes());
            buf.push(b':');
            buf.extend_from_slice(s.as_bytes());
        }
        BencodexValue::List(items) => {
            buf.push(b'l');
            for item in items {
                encode_into(item, buf);
            }
            buf.push(b'e');
        }
        BencodexValue::Dict(entries) => {
            buf.push(b'd');
            for (key, val) in entries {
                match key {
                    BencodexKey::Bytes(b) => {
                        buf.extend_from_slice(b.len().to_string().as_bytes());
                        buf.push(b':');
                        buf.extend_from_slice(b);
                    }
                    BencodexKey::Text(s) => {
                        buf.push(b'u');
                        buf.extend_from_slice(s.len().to_string().as_bytes());
                        buf.push(b':');
                        buf.extend_from_slice(s.as_bytes());
                    }
                }
                encode_into(val, buf);
            }
            buf.push(b'e');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    #[test]
    fn test_null() {
        let val = decode(b"n").unwrap();
        assert_eq!(val, BencodexValue::Null);
    }

    #[test]
    fn test_bool_true() {
        let val = decode(b"t").unwrap();
        assert_eq!(val, BencodexValue::Bool(true));
    }

    #[test]
    fn test_bool_false() {
        let val = decode(b"f").unwrap();
        assert_eq!(val, BencodexValue::Bool(false));
    }

    #[test]
    fn test_integer_positive() {
        let val = decode(b"i42e").unwrap();
        assert_eq!(val, BencodexValue::Integer(BigInt::from(42)));
    }

    #[test]
    fn test_integer_negative() {
        let val = decode(b"i-7e").unwrap();
        assert_eq!(val, BencodexValue::Integer(BigInt::from(-7)));
    }

    #[test]
    fn test_integer_zero() {
        let val = decode(b"i0e").unwrap();
        assert_eq!(val, BencodexValue::Integer(BigInt::from(0)));
    }

    #[test]
    fn test_bytes() {
        let val = decode(b"5:hello").unwrap();
        assert_eq!(val, BencodexValue::Bytes(b"hello".to_vec()));
    }

    #[test]
    fn test_bytes_empty() {
        let val = decode(b"0:").unwrap();
        assert_eq!(val, BencodexValue::Bytes(vec![]));
    }

    #[test]
    fn test_text() {
        let val = decode(b"u5:world").unwrap();
        assert_eq!(val, BencodexValue::Text("world".to_string()));
    }

    #[test]
    fn test_text_empty() {
        let val = decode(b"u0:").unwrap();
        assert_eq!(val, BencodexValue::Text(String::new()));
    }

    #[test]
    fn test_text_unicode() {
        // "cafe" in UTF-8 = 4 bytes, but "cafe\u{0301}" (cafe with accent) = 5 bytes
        let s = "caf\u{00e9}"; // 5 bytes in UTF-8
        let encoded = format!("u{}:{}", s.len(), s);
        let val = decode(encoded.as_bytes()).unwrap();
        assert_eq!(val, BencodexValue::Text(s.to_string()));
    }

    #[test]
    fn test_list() {
        // [null, true, 42]
        let val = decode(b"lnti42ee").unwrap();
        assert_eq!(
            val,
            BencodexValue::List(vec![
                BencodexValue::Null,
                BencodexValue::Bool(true),
                BencodexValue::Integer(BigInt::from(42)),
            ])
        );
    }

    #[test]
    fn test_list_empty() {
        let val = decode(b"le").unwrap();
        assert_eq!(val, BencodexValue::List(vec![]));
    }

    #[test]
    fn test_dict() {
        // { b"key": "value" }
        let val = decode(b"d3:keyu5:valuee").unwrap();
        assert_eq!(
            val,
            BencodexValue::Dict(vec![(
                BencodexKey::Bytes(b"key".to_vec()),
                BencodexValue::Text("value".to_string()),
            )])
        );
    }

    #[test]
    fn test_nested_list_of_bytes() {
        // This mimics a trie ShortNode: [path_bytes, child_hash]
        // l 4:\x01\x02\x03\x04 32:<32 bytes hash> e
        let mut input = Vec::new();
        input.push(b'l');
        // path: 4 bytes
        input.extend_from_slice(b"4:");
        input.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        // child hash: 32 bytes
        input.extend_from_slice(b"32:");
        let hash = [0xABu8; 32];
        input.extend_from_slice(&hash);
        input.push(b'e');

        let val = decode(&input).unwrap();
        match val {
            BencodexValue::List(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].as_bytes().unwrap(), &[0x01, 0x02, 0x03, 0x04]);
                assert_eq!(items[1].as_bytes().unwrap().len(), 32);
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn test_roundtrip() {
        let original = BencodexValue::List(vec![
            BencodexValue::Null,
            BencodexValue::Bool(true),
            BencodexValue::Integer(BigInt::from(999)),
            BencodexValue::Bytes(vec![1, 2, 3]),
            BencodexValue::Text("hello".into()),
            BencodexValue::Dict(vec![
                (BencodexKey::Bytes(b"a".to_vec()), BencodexValue::Null),
                (BencodexKey::Text("b".into()), BencodexValue::Bool(false)),
            ]),
        ]);
        let encoded = encode(&original);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_large_integer() {
        // Test big integer support
        let val = decode(b"i999999999999999999999999999999e").unwrap();
        if let BencodexValue::Integer(n) = val {
            assert!(n > BigInt::from(i64::MAX));
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_error_on_empty() {
        assert!(decode(b"").is_err());
    }

    #[test]
    fn test_error_on_invalid_tag() {
        assert!(decode(b"x").is_err());
    }
}
