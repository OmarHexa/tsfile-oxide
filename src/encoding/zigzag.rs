// ZigZag encoding: Variable-length integer encoding for signed integers.
//
// ALGORITHM EXPLANATION:
// ZigZag encoding maps signed integers to unsigned integers in a way that
// makes small-magnitude values (both positive and negative) encode compactly:
//
//   0 -> 0,  -1 -> 1,  1 -> 2,  -2 -> 3,  2 -> 4,  -3 -> 5,  3 -> 6, ...
//
// Formula:
//   zigzag(n) = (n << 1) ^ (n >> 31)   for i32
//   zigzag(n) = (n << 1) ^ (n >> 63)   for i64
//
// The transformed unsigned value is then encoded using varint (7 bits per byte
// with continuation bit). This is extremely effective for small integers:
//
//   Value    Plain (4 bytes)    ZigZag (varint)
//   -----    ---------------    ---------------
//   0        [00 00 00 00]      [00]           (1 byte)
//   1        [00 00 00 01]      [02]           (1 byte)
//   -1       [FF FF FF FF]      [01]           (1 byte)  <-- key win
//   127      [00 00 00 7F]      [FE 01]        (2 bytes)
//   -128     [FF FF FF 80]      [FF 01]        (2 bytes)  <-- also compact
//
// WHEN TO USE:
// - Small-magnitude integers (both positive and negative near zero)
// - Sensor data that hovers around zero (temperature deltas, error codes)
// - Delta encoding where deltas are typically small
// - NOT effective for: unsigned data, large positive-only values (use plain varint)
//
// This implementation wraps the zigzag+varint functions in serialize.rs which
// were already implemented for general serialization. The C++ encoding/
// directory has a separate ZigzagEncoder class, but the logic is identical.

use crate::error::Result;
use crate::serialize::{read_var_i32, read_var_i64, write_var_i32, write_var_i64};
use std::io::{Read, Write};

/// ZigZag encoder for i32 and i64 values.
///
/// This is a thin wrapper around the zigzag+varint serialization functions
/// in serialize.rs. The encoder has no state — each value is encoded independently.
#[derive(Debug, Clone)]
pub enum ZigzagEncoder {
    Int32,
    Int64,
}

impl ZigzagEncoder {
    /// Create a ZigZag encoder for i32 values.
    pub fn new_i32() -> Self {
        Self::Int32
    }

    /// Create a ZigZag encoder for i64 values.
    pub fn new_i64() -> Self {
        Self::Int64
    }

    /// Encode an i32 value using zigzag+varint encoding.
    ///
    /// Writes 1-5 bytes depending on magnitude. Small values near zero
    /// (both positive and negative) use 1-2 bytes.
    pub fn encode_i32(&mut self, value: i32, out: &mut Vec<u8>) -> Result<()> {
        write_var_i32(out, value)?;
        Ok(())
    }

    /// Encode an i64 value using zigzag+varint encoding.
    ///
    /// Writes 1-10 bytes depending on magnitude. Small values near zero
    /// (both positive and negative) use 1-2 bytes.
    pub fn encode_i64(&mut self, value: i64, out: &mut Vec<u8>) -> Result<()> {
        write_var_i64(out, value)?;
        Ok(())
    }

    /// Flush any buffered data.
    ///
    /// ZigZag encoding has no buffering, so this is a no-op.
    pub fn flush(&mut self, _out: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    /// Reset encoder state.
    ///
    /// ZigZag encoding has no state, so this is a no-op.
    pub fn reset(&mut self) {
        // No state to reset
    }
}

/// ZigZag decoder for i32 and i64 values.
///
/// This is a thin wrapper around the varint+zigzag deserialization functions
/// in serialize.rs. The decoder has no state — each value is decoded independently.
#[derive(Debug, Clone)]
pub enum ZigzagDecoder {
    Int32,
    Int64,
}

impl ZigzagDecoder {
    /// Create a ZigZag decoder for i32 values.
    pub fn new_i32() -> Self {
        Self::Int32
    }

    /// Create a ZigZag decoder for i64 values.
    pub fn new_i64() -> Self {
        Self::Int64
    }

    /// Decode an i32 value from zigzag+varint encoding.
    ///
    /// Reads 1-5 bytes from the input stream.
    pub fn decode_i32(&mut self, input: &mut impl Read) -> Result<i32> {
        read_var_i32(input)
    }

    /// Decode an i64 value from zigzag+varint encoding.
    ///
    /// Reads 1-10 bytes from the input stream.
    pub fn decode_i64(&mut self, input: &mut impl Read) -> Result<i64> {
        read_var_i64(input)
    }

    /// Reset decoder state.
    ///
    /// ZigZag decoding has no state, so this is a no-op.
    pub fn reset(&mut self) {
        // No state to reset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Cursor;

    // === Basic round-trip tests ===

    #[test]
    fn i32_small_values() {
        let mut encoder = ZigzagEncoder::new_i32();
        let mut decoder = ZigzagDecoder::new_i32();

        // Small values should encode to 1 byte
        for value in [-10, -5, -1, 0, 1, 5, 10] {
            let mut encoded = Vec::new();
            encoder.encode_i32(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 1, "value {} should encode to 1 byte", value);

            let mut cursor = Cursor::new(encoded);
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn i32_medium_values() {
        let mut encoder = ZigzagEncoder::new_i32();
        let mut decoder = ZigzagDecoder::new_i32();

        // Medium values should encode to 2-3 bytes
        for value in [-128, -127, -100, 100, 127, 128, 1000, -1000] {
            let mut encoded = Vec::new();
            encoder.encode_i32(value, &mut encoded).unwrap();
            assert!(
                encoded.len() <= 3,
                "value {} encoded to {} bytes",
                value,
                encoded.len()
            );

            let mut cursor = Cursor::new(encoded);
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn i32_extreme_values() {
        let mut encoder = ZigzagEncoder::new_i32();
        let mut decoder = ZigzagDecoder::new_i32();

        for value in [i32::MIN, i32::MAX] {
            let mut encoded = Vec::new();
            encoder.encode_i32(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 5); // Max 5 bytes for i32

            let mut cursor = Cursor::new(encoded);
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn i64_small_values() {
        let mut encoder = ZigzagEncoder::new_i64();
        let mut decoder = ZigzagDecoder::new_i64();

        for value in [-10i64, -1, 0, 1, 10] {
            let mut encoded = Vec::new();
            encoder.encode_i64(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 1);

            let mut cursor = Cursor::new(encoded);
            assert_eq!(decoder.decode_i64(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn i64_extreme_values() {
        let mut encoder = ZigzagEncoder::new_i64();
        let mut decoder = ZigzagDecoder::new_i64();

        for value in [i64::MIN, i64::MAX] {
            let mut encoded = Vec::new();
            encoder.encode_i64(value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 10); // Max 10 bytes for i64

            let mut cursor = Cursor::new(encoded);
            assert_eq!(decoder.decode_i64(&mut cursor).unwrap(), value);
        }
    }

    // === Property-based tests ===

    proptest! {
        #[test]
        fn prop_i32_round_trip(value: i32) {
            let mut encoder = ZigzagEncoder::new_i32();
            let mut decoder = ZigzagDecoder::new_i32();

            let mut encoded = Vec::new();
            encoder.encode_i32(value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_i32(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_i64_round_trip(value: i64) {
            let mut encoder = ZigzagEncoder::new_i64();
            let mut decoder = ZigzagDecoder::new_i64();

            let mut encoded = Vec::new();
            encoder.encode_i64(value, &mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            let decoded = decoder.decode_i64(&mut cursor).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_i32_small_values_are_compact(value in -100i32..=100i32) {
            let mut encoder = ZigzagEncoder::new_i32();
            let mut encoded = Vec::new();
            encoder.encode_i32(value, &mut encoded).unwrap();

            // Small values should encode to 1-2 bytes
            prop_assert!(encoded.len() <= 2);

            // Plain encoding would always use 4 bytes
            prop_assert!(encoded.len() < 4);
        }

        #[test]
        fn prop_i64_small_values_are_compact(value in -100i64..=100i64) {
            let mut encoder = ZigzagEncoder::new_i64();
            let mut encoded = Vec::new();
            encoder.encode_i64(value, &mut encoded).unwrap();

            // Small values should encode to 1-2 bytes
            prop_assert!(encoded.len() <= 2);

            // Plain encoding would always use 8 bytes
            prop_assert!(encoded.len() < 8);
        }
    }

    // === Compression efficiency test (informational) ===

    #[test]
    fn compression_efficiency_info() {
        let test_cases: Vec<(&str, Vec<i32>)> = vec![
            ("Small values near zero", (-10..=10).collect()),
            ("Medium values", vec![-1000, -500, -100, 0, 100, 500, 1000]),
            ("Large values", vec![i32::MIN, -1000000, 0, 1000000, i32::MAX]),
        ];

        for (name, values) in test_cases {
            let mut encoder = ZigzagEncoder::new_i32();
            let mut total_encoded = 0;
            let total_plain = values.len() * 4;

            for &value in &values {
                let mut encoded = Vec::new();
                encoder.encode_i32(value, &mut encoded).unwrap();
                total_encoded += encoded.len();
            }

            let ratio = total_plain as f64 / total_encoded as f64;
            println!(
                "{}: {} values, plain {} bytes, zigzag {} bytes (ratio {:.2}x)",
                name,
                values.len(),
                total_plain,
                total_encoded,
                ratio
            );
        }
    }

    // === Batch encoding test ===

    #[test]
    fn batch_encode_decode() {
        let values = vec![0, -1, 1, -2, 2, -127, 127, -128, 128, i32::MIN, i32::MAX];

        let mut encoder = ZigzagEncoder::new_i32();
        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i32(value, &mut encoded).unwrap();
        }

        let mut decoder = ZigzagDecoder::new_i32();
        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            let decoded = decoder.decode_i32(&mut cursor).unwrap();
            assert_eq!(decoded, expected);
        }
    }
}
