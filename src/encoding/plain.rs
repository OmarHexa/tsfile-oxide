// Plain encoding: raw binary representation with no compression.
//
// ALGORITHM EXPLANATION:
// Plain encoding stores values in their native binary format:
// - Boolean: 1 byte (0x00 = false, 0x01 = true)
// - Int32: 4 bytes, little-endian two's complement
// - Int64: 8 bytes, little-endian two's complement
// - Float: 4 bytes, IEEE-754 single-precision, little-endian
// - Double: 8 bytes, IEEE-754 double-precision, little-endian
// - Text/String: 4-byte big-endian length prefix + UTF-8 bytes
//
// This is the simplest encoding with no compression. It's used when:
// 1. Data is already compressed at the compression layer
// 2. Data is incompressible (random, high-entropy)
// 3. Encoding overhead would exceed any compression gains
// 4. Fast encoding/decoding is prioritized over size
//
// C++ implementation: encoding/plain_encoder.h, encoding/plain_decoder.h
// These are template classes instantiated for each type.

use crate::error::{Result, TsFileError};
use std::io::{Read, Write};

/// Plain encoder — stores values in native binary format.
///
/// All integer and float types use little-endian byte order to match C++ on
/// little-endian systems (x86/x64). Boolean uses 1 byte. Text/String uses
/// big-endian 4-byte length prefix (matching Java TsFile convention) + UTF-8 bytes.
#[derive(Debug, Clone)]
pub struct PlainEncoder;

impl PlainEncoder {
    pub fn new() -> Self {
        Self
    }

    /// Encode a boolean value (1 byte: 0x00 or 0x01).
    pub fn encode_bool(&mut self, value: bool, out: &mut Vec<u8>) -> Result<()> {
        out.push(if value { 1 } else { 0 });
        Ok(())
    }

    /// Encode an i32 value (4 bytes, little-endian).
    pub fn encode_i32(&mut self, value: i32, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    /// Encode an i64 value (8 bytes, little-endian).
    pub fn encode_i64(&mut self, value: i64, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    /// Encode an f32 value (4 bytes, IEEE-754, little-endian).
    pub fn encode_f32(&mut self, value: f32, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    /// Encode an f64 value (8 bytes, IEEE-754, little-endian).
    pub fn encode_f64(&mut self, value: f64, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    /// Encode a byte slice (Text/String).
    ///
    /// Format: 4-byte big-endian length + raw bytes.
    /// This matches the Java TsFile convention used by the C++ implementation.
    /// Maximum length is 2^31 - 1 bytes (~2GB).
    pub fn encode_bytes(&mut self, value: &[u8], out: &mut Vec<u8>) -> Result<()> {
        // Length check
        if value.len() > i32::MAX as usize {
            return Err(TsFileError::InvalidArg(format!(
                "byte array too long: {} > {}",
                value.len(),
                i32::MAX
            )));
        }

        // Write length prefix (big-endian i32)
        let len = value.len() as i32;
        out.extend_from_slice(&len.to_be_bytes());

        // Write raw bytes
        out.extend_from_slice(value);
        Ok(())
    }

    /// Flush any buffered data.
    ///
    /// Plain encoding has no buffering, so this is a no-op.
    /// Included for API consistency with other encoders.
    pub fn flush(&mut self, _out: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    /// Reset encoder state.
    ///
    /// Plain encoding has no state, so this is a no-op.
    /// Included for API consistency with other encoders.
    pub fn reset(&mut self) {
        // No state to reset
    }

    /// Get the maximum encoded size for a value.
    ///
    /// Returns the fixed size for numeric types, or the maximum size for byte arrays.
    pub fn max_encoded_size(&self, value_size: usize) -> usize {
        // For byte arrays: 4-byte length prefix + value bytes
        4 + value_size
    }
}

impl Default for PlainEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Plain decoder — reads values from native binary format.
///
/// Mirrors PlainEncoder exactly. All reads use little-endian byte order for
/// numeric types, big-endian length prefix for byte arrays.
#[derive(Debug, Clone)]
pub struct PlainDecoder;

impl PlainDecoder {
    pub fn new() -> Self {
        Self
    }

    /// Decode a boolean value (1 byte: 0x00 = false, 0x01 = true).
    ///
    /// Returns an error if the byte is not 0 or 1 (corrupted data).
    pub fn decode_bool(&mut self, input: &mut impl Read) -> Result<bool> {
        let mut buf = [0u8; 1];
        input.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(TsFileError::Corrupted(format!(
                "invalid boolean byte: 0x{:02x}",
                buf[0]
            ))),
        }
    }

    /// Decode an i32 value (4 bytes, little-endian).
    pub fn decode_i32(&mut self, input: &mut impl Read) -> Result<i32> {
        let mut buf = [0u8; 4];
        input.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    /// Decode an i64 value (8 bytes, little-endian).
    pub fn decode_i64(&mut self, input: &mut impl Read) -> Result<i64> {
        let mut buf = [0u8; 8];
        input.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    /// Decode an f32 value (4 bytes, IEEE-754, little-endian).
    pub fn decode_f32(&mut self, input: &mut impl Read) -> Result<f32> {
        let mut buf = [0u8; 4];
        input.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }

    /// Decode an f64 value (8 bytes, IEEE-754, little-endian).
    pub fn decode_f64(&mut self, input: &mut impl Read) -> Result<f64> {
        let mut buf = [0u8; 8];
        input.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    /// Decode a byte slice (Text/String).
    ///
    /// Format: 4-byte big-endian length + raw bytes.
    /// Returns a newly allocated Vec<u8> containing the decoded bytes.
    pub fn decode_bytes(&mut self, input: &mut impl Read) -> Result<Vec<u8>> {
        // Read length prefix (big-endian i32)
        let mut len_buf = [0u8; 4];
        input.read_exact(&mut len_buf)?;
        let len = i32::from_be_bytes(len_buf);

        if len < 0 {
            return Err(TsFileError::Corrupted(format!(
                "negative byte array length: {len}"
            )));
        }

        // Read raw bytes
        let mut buf = vec![0u8; len as usize];
        input.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Reset decoder state.
    ///
    /// Plain decoding has no state, so this is a no-op.
    /// Included for API consistency with other decoders.
    pub fn reset(&mut self) {
        // No state to reset
    }
}

impl Default for PlainDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Cursor;

    // === Basic round-trip tests ===

    #[test]
    fn bool_round_trip() {
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        for value in [false, true] {
            let mut encoded = Vec::new();
            encoder.encode_bool(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 1);

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_bool(&mut cursor).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn i32_round_trip() {
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        let values = [0, 1, -1, i32::MIN, i32::MAX, 12345, -67890];
        for value in values {
            let mut encoded = Vec::new();
            encoder.encode_i32(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 4);

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_i32(&mut cursor).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn i64_round_trip() {
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        let values = [0, 1, -1, i64::MIN, i64::MAX, 1234567890123456789];
        for value in values {
            let mut encoded = Vec::new();
            encoder.encode_i64(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 8);

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_i64(&mut cursor).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn f32_round_trip() {
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        let values = [
            0.0f32,
            1.0,
            -1.0,
            f32::MIN,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ];
        for value in values {
            let mut encoded = Vec::new();
            encoder.encode_f32(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 4);

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_f32(&mut cursor).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn f32_nan_round_trip() {
        // NaN has special handling — NaN != NaN, so check is_nan()
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        let mut encoded = Vec::new();
        encoder.encode_f32(f32::NAN, &mut encoded).unwrap();

        let mut cursor = Cursor::new(encoded);
        let decoded = decoder.decode_f32(&mut cursor).unwrap();
        assert!(decoded.is_nan());
    }

    #[test]
    fn f64_round_trip() {
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        let values = [
            0.0f64,
            1.0,
            -1.0,
            f64::MIN,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for value in values {
            let mut encoded = Vec::new();
            encoder.encode_f64(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 8);

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_f64(&mut cursor).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn f64_nan_round_trip() {
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        let mut encoded = Vec::new();
        encoder.encode_f64(f64::NAN, &mut encoded).unwrap();

        let mut cursor = Cursor::new(encoded);
        let decoded = decoder.decode_f64(&mut cursor).unwrap();
        assert!(decoded.is_nan());
    }

    #[test]
    fn bytes_round_trip() {
        let mut encoder = PlainEncoder::new();
        let mut decoder = PlainDecoder::new();

        let test_cases = vec![
            b"".to_vec(),
            b"hello".to_vec(),
            b"The quick brown fox".to_vec(),
            vec![0, 1, 2, 255, 254],
            "UTF-8: 你好世界 🚀".as_bytes().to_vec(),
        ];

        for value in test_cases {
            let mut encoded = Vec::new();
            encoder.encode_bytes(&value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 4 + value.len()); // 4-byte length + data

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_bytes(&mut cursor).unwrap();
            assert_eq!(decoded, value);
        }
    }

    // === Edge case tests ===

    #[test]
    fn invalid_bool_byte() {
        let mut decoder = PlainDecoder::new();
        let mut cursor = Cursor::new(vec![2u8]); // Invalid boolean byte
        assert!(decoder.decode_bool(&mut cursor).is_err());
    }

    #[test]
    fn truncated_i32() {
        let mut decoder = PlainDecoder::new();
        let mut cursor = Cursor::new(vec![0, 1, 2]); // Only 3 bytes, need 4
        assert!(decoder.decode_i32(&mut cursor).is_err());
    }

    #[test]
    fn negative_length_bytes() {
        let mut decoder = PlainDecoder::new();
        // Encode -1 as big-endian i32
        let mut cursor = Cursor::new(vec![0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(decoder.decode_bytes(&mut cursor).is_err());
    }

    #[test]
    fn truncated_bytes() {
        let mut decoder = PlainDecoder::new();
        // Length says 10 bytes but only 5 bytes follow
        let mut data = vec![0, 0, 0, 10]; // big-endian i32 = 10
        data.extend_from_slice(&[1, 2, 3, 4, 5]); // Only 5 bytes
        let mut cursor = Cursor::new(data);
        assert!(decoder.decode_bytes(&mut cursor).is_err());
    }

    // === Property-based tests using proptest ===

    proptest! {
        #[test]
        fn prop_bool_round_trip(value: bool) {
            let mut encoder = PlainEncoder::new();
            let mut decoder = PlainDecoder::new();

            let mut encoded = Vec::new();
            encoder.encode_bool(value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_bool(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_i32_round_trip(value: i32) {
            let mut encoder = PlainEncoder::new();
            let mut decoder = PlainDecoder::new();

            let mut encoded = Vec::new();
            encoder.encode_i32(value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_i32(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_i64_round_trip(value: i64) {
            let mut encoder = PlainEncoder::new();
            let mut decoder = PlainDecoder::new();

            let mut encoded = Vec::new();
            encoder.encode_i64(value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_i64(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_f32_round_trip(value in prop::num::f32::NORMAL) {
            // NORMAL excludes NaN/Infinity which need special handling
            let mut encoder = PlainEncoder::new();
            let mut decoder = PlainDecoder::new();

            let mut encoded = Vec::new();
            encoder.encode_f32(value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_f32(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_f64_round_trip(value in prop::num::f64::NORMAL) {
            let mut encoder = PlainEncoder::new();
            let mut decoder = PlainDecoder::new();

            let mut encoded = Vec::new();
            encoder.encode_f64(value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_f64(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_bytes_round_trip(value: Vec<u8>) {
            // Limit size to avoid huge allocations in tests
            if value.len() > 1_000_000 {
                return Ok(());
            }

            let mut encoder = PlainEncoder::new();
            let mut decoder = PlainDecoder::new();

            let mut encoded = Vec::new();
            encoder.encode_bytes(&value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_bytes(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }
    }

    // === Byte order verification tests ===

    #[test]
    fn i32_little_endian() {
        // Verify little-endian byte order
        let mut encoder = PlainEncoder::new();
        let mut encoded = Vec::new();
        encoder.encode_i32(0x12345678, &mut encoded).unwrap();
        assert_eq!(encoded, vec![0x78, 0x56, 0x34, 0x12]); // little-endian
    }

    #[test]
    fn bytes_big_endian_length() {
        // Verify big-endian length prefix
        let mut encoder = PlainEncoder::new();
        let mut encoded = Vec::new();
        encoder.encode_bytes(b"hello", &mut encoded).unwrap();
        // Length 5 as big-endian i32: [0, 0, 0, 5]
        assert_eq!(&encoded[0..4], &[0, 0, 0, 5]);
        assert_eq!(&encoded[4..], b"hello");
    }
}
