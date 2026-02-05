// TS2DIFF encoding: Delta-of-delta encoding for time series data.
//
// ALGORITHM EXPLANATION:
// TS2DIFF (Time Series 2 Delta) exploits the property that many time series
// have regular intervals or slowly changing deltas. Instead of storing raw
// values or deltas, it stores delta-of-deltas, which are often very small.
//
// Example for timestamps at regular intervals:
//   Timestamps:     1000, 1010, 1020, 1030, 1040, 1050
//   Deltas:               10,   10,   10,   10,   10
//   Delta-of-deltas:       0,    0,    0,    0
//
// Encoding process:
// 1. Store first value as-is (32 or 64 bits)
// 2. Calculate delta₁ = value₁ - value₀
// 3. Store delta₁ using zigzag+varint
// 4. For subsequent values:
//    - Calculate delta_n = value_n - value_(n-1)
//    - Calculate delta_of_delta = delta_n - delta_(n-1)
//    - Store delta_of_delta using zigzag+varint
//
// Example encoding:
//   Values:         1000, 1010, 1020, 1030, 1050, 1080
//   First value:    1000 (4 bytes)
//   Delta₁:         10 (zigzag 20 -> 1 byte)
//   Delta-of-delta: 0, 0, 20, 30 (each 1-2 bytes)
//   Total: ~4 + 1 + 4 = 9 bytes vs 24 bytes plain
//
// For floating-point values:
// - Convert f32 to i32 bits, f64 to i64 bits
// - Apply same delta-of-delta logic on bit patterns
// - This works because slowly changing floats have slowly changing bit patterns
//
// WHEN TO USE:
// - Monotonically increasing timestamps with regular intervals
// - Sensor readings with slow, steady changes
// - Sequential IDs or counters
// - NOT effective for: random data, large jumps, oscillating values
//
// C++ implementation: encoding/ts2diff_encoder.h (template for i32/i64/f32/f64)

use crate::error::Result;
use crate::serialize::{read_var_i32, read_var_i64, write_var_i32, write_var_i64};
use std::io::Read;

/// TS2DIFF encoder for i32, i64, f32, and f64 values.
///
/// Maintains state for the previous value and previous delta.
/// Uses zigzag+varint encoding for deltas and delta-of-deltas.
#[derive(Debug, Clone)]
pub enum Ts2DiffEncoder {
    Int32(Ts2DiffCore<i32>),
    Int64(Ts2DiffCore<i64>),
    Float(Ts2DiffCore<i32>),   // f32 as i32 bits
    Double(Ts2DiffCore<i64>),  // f64 as i64 bits
}

impl Ts2DiffEncoder {
    pub fn new_i32() -> Self {
        Self::Int32(Ts2DiffCore::new())
    }

    pub fn new_i64() -> Self {
        Self::Int64(Ts2DiffCore::new())
    }

    pub fn new_f32() -> Self {
        Self::Float(Ts2DiffCore::new())
    }

    pub fn new_f64() -> Self {
        Self::Double(Ts2DiffCore::new())
    }

    pub fn encode_i32(&mut self, value: i32, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int32(core) => core.encode(value, out),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF encoder type mismatch: expected i32".into(),
            )),
        }
    }

    pub fn encode_i64(&mut self, value: i64, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int64(core) => core.encode(value, out),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF encoder type mismatch: expected i64".into(),
            )),
        }
    }

    pub fn encode_f32(&mut self, value: f32, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Float(core) => core.encode(value.to_bits() as i32, out),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF encoder type mismatch: expected f32".into(),
            )),
        }
    }

    pub fn encode_f64(&mut self, value: f64, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Double(core) => core.encode(value.to_bits() as i64, out),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF encoder type mismatch: expected f64".into(),
            )),
        }
    }

    pub fn flush(&mut self, _out: &mut Vec<u8>) -> Result<()> {
        // TS2DIFF has no buffering, all values are written immediately
        Ok(())
    }

    pub fn reset(&mut self) {
        match self {
            Self::Int32(core) | Self::Float(core) => core.reset(),
            Self::Int64(core) | Self::Double(core) => core.reset(),
        }
    }
}

/// TS2DIFF decoder for i32, i64, f32, and f64 values.
#[derive(Debug, Clone)]
pub enum Ts2DiffDecoder {
    Int32(Ts2DiffCore<i32>),
    Int64(Ts2DiffCore<i64>),
    Float(Ts2DiffCore<i32>),
    Double(Ts2DiffCore<i64>),
}

impl Ts2DiffDecoder {
    pub fn new_i32() -> Self {
        Self::Int32(Ts2DiffCore::new())
    }

    pub fn new_i64() -> Self {
        Self::Int64(Ts2DiffCore::new())
    }

    pub fn new_f32() -> Self {
        Self::Float(Ts2DiffCore::new())
    }

    pub fn new_f64() -> Self {
        Self::Double(Ts2DiffCore::new())
    }

    pub fn decode_i32(&mut self, input: &mut impl Read) -> Result<i32> {
        match self {
            Self::Int32(core) => core.decode(input),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF decoder type mismatch: expected i32".into(),
            )),
        }
    }

    pub fn decode_i64(&mut self, input: &mut impl Read) -> Result<i64> {
        match self {
            Self::Int64(core) => core.decode(input),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF decoder type mismatch: expected i64".into(),
            )),
        }
    }

    pub fn decode_f32(&mut self, input: &mut impl Read) -> Result<f32> {
        match self {
            Self::Float(core) => Ok(f32::from_bits(core.decode(input)? as u32)),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF decoder type mismatch: expected f32".into(),
            )),
        }
    }

    pub fn decode_f64(&mut self, input: &mut impl Read) -> Result<f64> {
        match self {
            Self::Double(core) => Ok(f64::from_bits(core.decode(input)? as u64)),
            _ => Err(crate::error::TsFileError::InvalidArg(
                "TS2DIFF decoder type mismatch: expected f64".into(),
            )),
        }
    }

    pub fn reset(&mut self) {
        match self {
            Self::Int32(core) | Self::Float(core) => core.reset(),
            Self::Int64(core) | Self::Double(core) => core.reset(),
        }
    }
}

/// Core TS2DIFF encoder/decoder logic, generic over i32/i64.
///
/// Maintains state for previous value and previous delta.
/// Uses trait-based abstraction for i32/i64 operations.
#[derive(Debug, Clone)]
struct Ts2DiffCore<T: Ts2DiffValue> {
    prev_value: Option<T>,
    prev_delta: Option<T>,
}

impl<T: Ts2DiffValue> Ts2DiffCore<T> {
    fn new() -> Self {
        Self {
            prev_value: None,
            prev_delta: None,
        }
    }

    fn encode(&mut self, value: T, out: &mut Vec<u8>) -> Result<()> {
        match self.prev_value {
            None => {
                // First value: write as-is (plain binary)
                value.write_plain(out)?;
                self.prev_value = Some(value);
            }
            Some(prev) => {
                let delta = value.sub(prev);

                match self.prev_delta {
                    None => {
                        // First delta: write using zigzag+varint
                        delta.write_varint(out)?;
                        self.prev_delta = Some(delta);
                    }
                    Some(prev_delta) => {
                        // Delta-of-delta: encode difference
                        let delta_of_delta = delta.sub(prev_delta);
                        delta_of_delta.write_varint(out)?;
                        self.prev_delta = Some(delta);
                    }
                }

                self.prev_value = Some(value);
            }
        }
        Ok(())
    }

    fn decode(&mut self, input: &mut impl Read) -> Result<T> {
        match self.prev_value {
            None => {
                // First value: read plain
                let value = T::read_plain(input)?;
                self.prev_value = Some(value);
                Ok(value)
            }
            Some(prev) => {
                match self.prev_delta {
                    None => {
                        // First delta: read varint
                        let delta = T::read_varint(input)?;
                        let value = prev.add(delta);
                        self.prev_delta = Some(delta);
                        self.prev_value = Some(value);
                        Ok(value)
                    }
                    Some(prev_delta) => {
                        // Delta-of-delta: decode and reconstruct
                        let delta_of_delta = T::read_varint(input)?;
                        let delta = prev_delta.add(delta_of_delta);
                        let value = prev.add(delta);
                        self.prev_delta = Some(delta);
                        self.prev_value = Some(value);
                        Ok(value)
                    }
                }
            }
        }
    }

    fn reset(&mut self) {
        self.prev_value = None;
        self.prev_delta = None;
    }
}

/// Trait for types that can be used with TS2DIFF encoding.
trait Ts2DiffValue: Copy {
    fn write_plain(&self, out: &mut Vec<u8>) -> Result<()>;
    fn read_plain(input: &mut impl Read) -> Result<Self>;
    fn write_varint(&self, out: &mut Vec<u8>) -> Result<()>;
    fn read_varint(input: &mut impl Read) -> Result<Self>;
    fn sub(self, other: Self) -> Self;
    fn add(self, other: Self) -> Self;
}

impl Ts2DiffValue for i32 {
    fn write_plain(&self, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }

    fn read_plain(input: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; 4];
        input.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    fn write_varint(&self, out: &mut Vec<u8>) -> Result<()> {
        write_var_i32(out, *self)?;
        Ok(())
    }

    fn read_varint(input: &mut impl Read) -> Result<Self> {
        read_var_i32(input)
    }

    fn sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
    }

    fn add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

impl Ts2DiffValue for i64 {
    fn write_plain(&self, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }

    fn read_plain(input: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; 8];
        input.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    fn write_varint(&self, out: &mut Vec<u8>) -> Result<()> {
        write_var_i64(out, *self)?;
        Ok(())
    }

    fn read_varint(input: &mut impl Read) -> Result<Self> {
        read_var_i64(input)
    }

    fn sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
    }

    fn add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Cursor;

    // === Basic round-trip tests ===

    #[test]
    fn i32_monotonic_sequence() {
        let mut encoder = Ts2DiffEncoder::new_i32();
        let mut decoder = Ts2DiffDecoder::new_i32();

        // Regular intervals: delta = 10, delta-of-delta = 0
        let values: Vec<i32> = (0..10).map(|i| 1000 + i * 10).collect();

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i32(value, &mut encoded).unwrap();
        }

        // Should compress well: 4 bytes (first) + 1 byte (delta) + 9*1 byte (dod=0)
        let plain_size = values.len() * 4;
        assert!(
            encoded.len() < plain_size / 2,
            "encoded {} bytes vs {} plain",
            encoded.len(),
            plain_size
        );

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn i64_timestamps() {
        let mut encoder = Ts2DiffEncoder::new_i64();
        let mut decoder = Ts2DiffDecoder::new_i64();

        // Simulated timestamps (milliseconds since epoch)
        let base = 1700000000000i64;
        let values: Vec<i64> = (0..20).map(|i| base + i * 1000).collect();

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i64(value, &mut encoded).unwrap();
        }

        // Should compress well due to constant delta
        let plain_size = values.len() * 8;
        assert!(encoded.len() < plain_size / 2);

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_i64(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn f32_slowly_changing() {
        let mut encoder = Ts2DiffEncoder::new_f32();
        let mut decoder = Ts2DiffDecoder::new_f32();

        let values: Vec<f32> = (0..10).map(|i| 20.0 + i as f32 * 0.1).collect();

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_f32(value, &mut encoded).unwrap();
        }

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            let decoded = decoder.decode_f32(&mut cursor).unwrap();
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn f64_round_trip() {
        let mut encoder = Ts2DiffEncoder::new_f64();
        let mut decoder = Ts2DiffDecoder::new_f64();

        let values = vec![3.14159, 3.14260, 3.14361, 3.14462];

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_f64(value, &mut encoded).unwrap();
        }

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_f64(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn i32_single_value() {
        let mut encoder = Ts2DiffEncoder::new_i32();
        let mut decoder = Ts2DiffDecoder::new_i32();

        let mut encoded = Vec::new();
        encoder.encode_i32(42, &mut encoded).unwrap();
        assert_eq!(encoded.len(), 4); // Just the plain value

        let mut cursor = Cursor::new(encoded);
        assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), 42);
    }

    #[test]
    fn i32_variable_deltas() {
        let mut encoder = Ts2DiffEncoder::new_i32();
        let mut decoder = Ts2DiffDecoder::new_i32();

        // Deltas: 10, 20, 30 → delta-of-deltas: 10, 10
        let values = vec![100, 110, 130, 160];

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i32(value, &mut encoded).unwrap();
        }

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), expected);
        }
    }

    // === Property-based tests ===

    proptest! {
        #[test]
        fn prop_i32_round_trip(values: Vec<i32>) {
            if values.is_empty() {
                return Ok(());
            }

            let mut encoder = Ts2DiffEncoder::new_i32();
            let mut decoder = Ts2DiffDecoder::new_i32();

            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_i32(value, &mut encoded).unwrap();
            }

            let mut cursor = Cursor::new(encoded);
            for &expected in &values {
                let decoded = decoder.decode_i32(&mut cursor).unwrap();
                prop_assert_eq!(decoded, expected);
            }
        }

        #[test]
        fn prop_i64_round_trip(values: Vec<i64>) {
            if values.is_empty() {
                return Ok(());
            }

            let mut encoder = Ts2DiffEncoder::new_i64();
            let mut decoder = Ts2DiffDecoder::new_i64();

            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_i64(value, &mut encoded).unwrap();
            }

            let mut cursor = Cursor::new(encoded);
            for &expected in &values {
                let decoded = decoder.decode_i64(&mut cursor).unwrap();
                prop_assert_eq!(decoded, expected);
            }
        }

        #[test]
        fn prop_f32_round_trip(values: Vec<f32>) {
            if values.is_empty() {
                return Ok(());
            }

            // Filter out NaN
            let values: Vec<f32> = values.into_iter().filter(|v| !v.is_nan()).collect();
            if values.is_empty() {
                return Ok(());
            }

            let mut encoder = Ts2DiffEncoder::new_f32();
            let mut decoder = Ts2DiffDecoder::new_f32();

            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_f32(value, &mut encoded).unwrap();
            }

            let mut cursor = Cursor::new(encoded);
            for &expected in &values {
                let decoded = decoder.decode_f32(&mut cursor).unwrap();
                prop_assert_eq!(decoded, expected);
            }
        }
    }

    // === Compression ratio test ===

    #[test]
    fn compression_ratio_info() {
        println!("\nTS2DIFF compression ratios:");

        // Test 1: Regular timestamps
        let regular: Vec<i64> = (0..100).map(|i| 1000000 + i * 1000).collect();
        test_compression_i64("Regular timestamps (1s intervals)", &regular);

        // Test 2: Slowly changing deltas
        let slow: Vec<i32> = (0..100).map(|i| 1000 + i * i).collect();
        test_compression_i32("Slowly changing deltas", &slow);

        // Test 3: Constant value (best case)
        let constant: Vec<i32> = vec![42; 100];
        test_compression_i32("Constant value", &constant);
    }

    fn test_compression_i32(name: &str, values: &[i32]) {
        let mut encoder = Ts2DiffEncoder::new_i32();
        let mut encoded = Vec::new();
        for &value in values {
            encoder.encode_i32(value, &mut encoded).unwrap();
        }

        let plain_size = values.len() * 4;
        let ratio = plain_size as f64 / encoded.len() as f64;
        println!(
            "  {}: {} values, plain {} bytes, ts2diff {} bytes (ratio {:.2}x)",
            name,
            values.len(),
            plain_size,
            encoded.len(),
            ratio
        );
    }

    fn test_compression_i64(name: &str, values: &[i64]) {
        let mut encoder = Ts2DiffEncoder::new_i64();
        let mut encoded = Vec::new();
        for &value in values {
            encoder.encode_i64(value, &mut encoded).unwrap();
        }

        let plain_size = values.len() * 8;
        let ratio = plain_size as f64 / encoded.len() as f64;
        println!(
            "  {}: {} values, plain {} bytes, ts2diff {} bytes (ratio {:.2}x)",
            name,
            values.len(),
            plain_size,
            encoded.len(),
            ratio
        );
    }
}
