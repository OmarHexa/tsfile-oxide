// RLE (Run-Length Encoding): Encode consecutive runs of identical values.
//
// ALGORITHM EXPLANATION:
// RLE is effective for data with long runs of repeated values (e.g., sensor
// status flags that stay constant for extended periods). It stores each run
// as a (value, count) pair:
//
// Example: [5, 5, 5, 5, 7, 7, 9, 9, 9, 9, 9]
// RLE:     [(5, 4), (7, 2), (9, 5)]
// Encoding: value₁ count₁ value₂ count₂ value₃ count₃
//
// Format:
// - Values: encoded using plain little-endian binary (4 bytes for i32, 8 for i64)
// - Counts: encoded using variable-length encoding (varint) to save space
//
// RLE works for Int32 and Int64 types. For other types, use Plain or Gorilla.
//
// WHEN TO USE:
// - Data with long runs of identical values (status codes, flags, categories)
// - Compression ratio improves with longer runs
// - Worst case: alternating values → larger than plain encoding (value + 1-byte count each)
//
// C++ implementation: encoding/rle_encoder.h, encoding/rle_decoder.h

use crate::error::{Result, TsFileError};
use crate::serialize::{read_var_u32, write_var_u32};
use std::io::{Read, Write};

/// RLE encoder for i32 and i64 values.
///
/// Accumulates consecutive runs of identical values and encodes them as
/// (value, count) pairs. Flush must be called to emit the final run.
#[derive(Debug, Clone)]
pub enum RleEncoder {
    Int32(RleEncoderCore<i32>),
    Int64(RleEncoderCore<i64>),
}

impl RleEncoder {
    /// Create an RLE encoder for i32 values.
    pub fn new_i32() -> Self {
        Self::Int32(RleEncoderCore::new())
    }

    /// Create an RLE encoder for i64 values.
    pub fn new_i64() -> Self {
        Self::Int64(RleEncoderCore::new())
    }

    /// Encode an i32 value.
    pub fn encode_i32(&mut self, value: i32, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int32(core) => core.encode(value, out),
            Self::Int64(_) => Err(TsFileError::InvalidArg(
                "RLE encoder is i64, cannot encode i32".into(),
            )),
        }
    }

    /// Encode an i64 value.
    pub fn encode_i64(&mut self, value: i64, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int64(core) => core.encode(value, out),
            Self::Int32(_) => Err(TsFileError::InvalidArg(
                "RLE encoder is i32, cannot encode i64".into(),
            )),
        }
    }

    /// Flush any pending run.
    ///
    /// MUST be called after the last value to emit the final run.
    pub fn flush(&mut self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int32(core) => core.flush(out),
            Self::Int64(core) => core.flush(out),
        }
    }

    /// Reset encoder state.
    pub fn reset(&mut self) {
        match self {
            Self::Int32(core) => core.reset(),
            Self::Int64(core) => core.reset(),
        }
    }
}

/// Core RLE encoder logic, generic over i32/i64.
#[derive(Debug, Clone)]
struct RleEncoderCore<T: Copy + Eq> {
    /// Current run value (None if no run in progress).
    current_value: Option<T>,
    /// Count of consecutive occurrences of current_value.
    run_count: u32,
}

impl<T: Copy + Eq> RleEncoderCore<T> {
    fn new() -> Self {
        Self {
            current_value: None,
            run_count: 0,
        }
    }

    fn encode(&mut self, value: T, out: &mut Vec<u8>) -> Result<()>
    where
        T: ToBytes,
    {
        match self.current_value {
            None => {
                // Start new run
                self.current_value = Some(value);
                self.run_count = 1;
            }
            Some(current) if current == value => {
                // Continue current run
                self.run_count += 1;
            }
            Some(current) => {
                // Different value — flush current run and start new one
                current.write_bytes(out)?;
                write_var_u32(out, self.run_count)?;

                self.current_value = Some(value);
                self.run_count = 1;
            }
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut Vec<u8>) -> Result<()>
    where
        T: ToBytes,
    {
        if let Some(value) = self.current_value {
            value.write_bytes(out)?;
            write_var_u32(out, self.run_count)?;
            self.current_value = None;
            self.run_count = 0;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.current_value = None;
        self.run_count = 0;
    }
}

/// RLE decoder for i32 and i64 values.
///
/// Reads (value, count) pairs and yields each value 'count' times.
/// Maintains state to handle batch decoding.
#[derive(Debug, Clone)]
pub enum RleDecoder {
    Int32(RleDecoderCore<i32>),
    Int64(RleDecoderCore<i64>),
}

impl RleDecoder {
    /// Create an RLE decoder for i32 values.
    pub fn new_i32() -> Self {
        Self::Int32(RleDecoderCore::new())
    }

    /// Create an RLE decoder for i64 values.
    pub fn new_i64() -> Self {
        Self::Int64(RleDecoderCore::new())
    }

    /// Decode an i32 value.
    ///
    /// Returns Ok(value) if a value is available, or reads the next (value, count)
    /// pair from the input if the current run is exhausted.
    pub fn decode_i32(&mut self, input: &mut impl Read) -> Result<i32> {
        match self {
            Self::Int32(core) => core.decode(input),
            Self::Int64(_) => Err(TsFileError::InvalidArg(
                "RLE decoder is i64, cannot decode i32".into(),
            )),
        }
    }

    /// Decode an i64 value.
    pub fn decode_i64(&mut self, input: &mut impl Read) -> Result<i64> {
        match self {
            Self::Int64(core) => core.decode(input),
            Self::Int32(_) => Err(TsFileError::InvalidArg(
                "RLE decoder is i32, cannot decode i64".into(),
            )),
        }
    }

    /// Reset decoder state.
    pub fn reset(&mut self) {
        match self {
            Self::Int32(core) => core.reset(),
            Self::Int64(core) => core.reset(),
        }
    }
}

/// Core RLE decoder logic, generic over i32/i64.
#[derive(Debug, Clone)]
struct RleDecoderCore<T: Copy> {
    /// Current run value (None if no run loaded).
    current_value: Option<T>,
    /// Remaining count for current run.
    remaining_count: u32,
}

impl<T: Copy> RleDecoderCore<T> {
    fn new() -> Self {
        Self {
            current_value: None,
            remaining_count: 0,
        }
    }

    fn decode(&mut self, input: &mut impl Read) -> Result<T>
    where
        T: FromBytes,
    {
        // If current run exhausted, read next (value, count) pair
        if self.remaining_count == 0 {
            self.current_value = Some(T::read_bytes(input)?);
            self.remaining_count = read_var_u32(input)?;

            if self.remaining_count == 0 {
                return Err(TsFileError::Corrupted("RLE run count is zero".into()));
            }
        }

        // Yield current value and decrement count
        self.remaining_count -= 1;
        Ok(self.current_value.unwrap())
    }

    fn reset(&mut self) {
        self.current_value = None;
        self.remaining_count = 0;
    }
}

// ---------------------------------------------------------------------------
// Helper traits for generic encoding/decoding
// ---------------------------------------------------------------------------

trait ToBytes {
    fn write_bytes(&self, out: &mut Vec<u8>) -> Result<()>;
}

trait FromBytes: Sized {
    fn read_bytes(input: &mut impl Read) -> Result<Self>;
}

impl ToBytes for i32 {
    fn write_bytes(&self, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl FromBytes for i32 {
    fn read_bytes(input: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; 4];
        input.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }
}

impl ToBytes for i64 {
    fn write_bytes(&self, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl FromBytes for i64 {
    fn read_bytes(input: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; 8];
        input.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Cursor;

    // === Basic round-trip tests ===

    #[test]
    fn i32_single_run() {
        let mut encoder = RleEncoder::new_i32();
        let mut decoder = RleDecoder::new_i32();

        let mut encoded = Vec::new();
        for _ in 0..100 {
            encoder.encode_i32(42, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        // Should encode as (42, 100) → 4 bytes value + ~1 byte count = ~5 bytes
        assert!(encoded.len() < 10);

        let mut cursor = Cursor::new(encoded);
        for _ in 0..100 {
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), 42);
        }
    }

    #[test]
    fn i64_single_run() {
        let mut encoder = RleEncoder::new_i64();
        let mut decoder = RleDecoder::new_i64();

        let mut encoded = Vec::new();
        for _ in 0..50 {
            encoder.encode_i64(123456789, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        // Should encode as (123456789, 50) → 8 bytes value + ~1 byte count = ~9 bytes
        assert!(encoded.len() < 15);

        let mut cursor = Cursor::new(encoded);
        for _ in 0..50 {
            assert_eq!(decoder.decode_i64(&mut cursor).unwrap(), 123456789);
        }
    }

    #[test]
    fn i32_multiple_runs() {
        let mut encoder = RleEncoder::new_i32();
        let mut decoder = RleDecoder::new_i32();

        let values = vec![
            5, 5, 5, 5,      // run of 4
            7, 7,            // run of 2
            9, 9, 9, 9, 9,   // run of 5
        ];

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i32(value, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        // Should encode 3 runs: (5,4) (7,2) (9,5)
        // Each run: 4 bytes value + 1 byte count = ~15 bytes total
        assert!(encoded.len() < 20);

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn i32_alternating_values() {
        // Worst case for RLE: alternating values
        let mut encoder = RleEncoder::new_i32();
        let mut decoder = RleDecoder::new_i32();

        let values: Vec<i32> = (0..10).map(|i| if i % 2 == 0 { 1 } else { 2 }).collect();

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i32(value, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        // Each value becomes a run of 1: 10 values → 10 * (4 + 1) = 50 bytes
        // Worse than plain encoding (10 * 4 = 40 bytes), but RLE still works correctly
        assert!(encoded.len() >= 40);

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn i32_empty() {
        let mut encoder = RleEncoder::new_i32();
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();
        assert!(encoded.is_empty());
    }

    #[test]
    fn i32_single_value() {
        let mut encoder = RleEncoder::new_i32();
        let mut decoder = RleDecoder::new_i32();

        let mut encoded = Vec::new();
        encoder.encode_i32(99, &mut encoded).unwrap();
        encoder.flush(&mut encoded).unwrap();

        let mut cursor = Cursor::new(encoded);
        assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), 99);
    }

    // === Edge case tests ===

    #[test]
    fn type_mismatch_encode() {
        let mut encoder = RleEncoder::new_i32();
        let mut encoded = Vec::new();
        assert!(encoder.encode_i64(123, &mut encoded).is_err());
    }

    #[test]
    fn type_mismatch_decode() {
        let mut decoder = RleDecoder::new_i32();
        let mut cursor = Cursor::new(vec![0, 0, 0, 0, 1]); // dummy data
        assert!(decoder.decode_i64(&mut cursor).is_err());
    }

    #[test]
    fn zero_run_count() {
        // Corrupted data: value followed by run count of 0
        let mut decoder = RleDecoder::new_i32();
        let mut data = Vec::new();
        data.extend_from_slice(&42i32.to_le_bytes()); // value = 42
        data.push(0); // run count = 0 (invalid)

        let mut cursor = Cursor::new(data);
        assert!(decoder.decode_i32(&mut cursor).is_err());
    }

    #[test]
    fn truncated_value() {
        let mut decoder = RleDecoder::new_i32();
        let mut cursor = Cursor::new(vec![1, 2]); // Only 2 bytes, need 4 for i32
        assert!(decoder.decode_i32(&mut cursor).is_err());
    }

    #[test]
    fn truncated_count() {
        let mut decoder = RleDecoder::new_i32();
        let mut data = Vec::new();
        data.extend_from_slice(&42i32.to_le_bytes()); // value = 42
        // No count bytes follow (truncated)

        let mut cursor = Cursor::new(data);
        assert!(decoder.decode_i32(&mut cursor).is_err());
    }

    // === Property-based tests ===

    proptest! {
        #[test]
        fn prop_i32_round_trip(values: Vec<i32>) {
            let mut encoder = RleEncoder::new_i32();
            let mut decoder = RleDecoder::new_i32();

            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_i32(value, &mut encoded).unwrap();
            }
            encoder.flush(&mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            for &expected in &values {
                let decoded = decoder.decode_i32(&mut cursor).unwrap();
                prop_assert_eq!(decoded, expected);
            }
        }

        #[test]
        fn prop_i64_round_trip(values: Vec<i64>) {
            let mut encoder = RleEncoder::new_i64();
            let mut decoder = RleDecoder::new_i64();

            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_i64(value, &mut encoded).unwrap();
            }
            encoder.flush(&mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            for &expected in &values {
                let decoded = decoder.decode_i64(&mut cursor).unwrap();
                prop_assert_eq!(decoded, expected);
            }
        }

        #[test]
        fn prop_i32_compression_ratio(run_length in 10u32..1000u32) {
            // Test that RLE compresses repeated values well
            let mut encoder = RleEncoder::new_i32();

            let mut encoded = Vec::new();
            for _ in 0..run_length {
                encoder.encode_i32(42, &mut encoded).unwrap();
            }
            encoder.flush(&mut encoded).unwrap();

            // Plain encoding: run_length * 4 bytes
            // RLE encoding: 4 bytes value + varint count (~1-2 bytes for <16384)
            let plain_size = run_length as usize * 4;
            prop_assert!(encoded.len() < plain_size);
        }
    }

    // === Compression ratio test (informational) ===

    #[test]
    fn compression_ratio_info() {
        let test_cases = vec![
            ("Single run (100 values)", vec![42; 100]),
            (
                "Multiple runs",
                vec![1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 4, 5, 5, 5, 5],
            ),
            (
                "Alternating (worst case)",
                (0..20).map(|i| i % 2).collect(),
            ),
        ];

        for (name, values) in test_cases {
            let mut encoder = RleEncoder::new_i32();
            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_i32(value, &mut encoded).unwrap();
            }
            encoder.flush(&mut encoded).unwrap();

            let plain_size = values.len() * 4;
            let ratio = plain_size as f64 / encoded.len() as f64;
            println!(
                "{}: {} values, plain {} bytes, RLE {} bytes (ratio {:.2}x)",
                name,
                values.len(),
                plain_size,
                encoded.len(),
                ratio
            );
        }
    }
}
