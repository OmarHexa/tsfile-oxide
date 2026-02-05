// Gorilla encoding: XOR-based compression for time series data.
//
// ALGORITHM EXPLANATION:
// Gorilla was developed by Facebook for time series compression. It's extremely
// effective for slowly-changing floating-point data (sensor readings, metrics).
// Paper: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
//
// The algorithm exploits two observations:
// 1. Values in time series often change slowly or stay constant
// 2. XOR of consecutive values often has many leading/trailing zeros
//
// Encoding process:
// 1. Store first value as-is (32 or 64 bits)
// 2. For each subsequent value:
//    - XOR with previous value
//    - If XOR = 0 (value unchanged): store control bit '0' (1 bit)
//    - If XOR ≠ 0:
//      a) store control bit '1' (1 bit)
//      b) count leading zeros (L) and meaningful bits (M)
//      c) if L and M match previous block:
//         - store control bit '0' (1 bit)
//         - store M bits of XOR value
//      d) else:
//         - store control bit '1' (1 bit)
//         - store L (5 bits for f32/i32, 6 bits for f64/i64)
//         - store M-1 (6 bits for f32/i32, 6 bits for f64/i64)
//         - store M bits of XOR value
//
// Example (f32):
//   Value     XOR          Leading  Trailing  Encoding
//   72.5      (first)      -        -         32 bits
//   72.5      0x00000000   -        -         1 bit (control '0')
//   72.6      0x00051EB8   12       3         1 + 1 + 5 + 6 + 17 = 30 bits (first diff)
//   72.6      0x00000000   -        -         1 bit (control '0')
//   72.7      0x00051EB8   12       3         1 + 1 + 17 = 19 bits (L+M match)
//
// WHEN TO USE:
// - Floating-point time series with slow changes (sensor data, stock prices)
// - Integer sequences with small deltas
// - NOT effective for: random data, large jumps, non-numeric data
//
// C++ implementation: encoding/gorilla_encoder.h (template for u32/u64)

use crate::error::{Result, TsFileError};
use std::io::Read;

/// Gorilla encoder for i32, i64, f32, and f64 values.
///
/// Uses XOR-based compression with bit-level control codes. Maintains state
/// for the previous value and previous leading/trailing zero counts.
#[derive(Debug, Clone)]
pub enum GorillaEncoder {
    Int32(GorillaCore<u32>),
    Int64(GorillaCore<u64>),
    Float(GorillaCore<u32>),   // f32 as u32 bits
    Double(GorillaCore<u64>),  // f64 as u64 bits
}

impl GorillaEncoder {
    pub fn new_i32() -> Self {
        Self::Int32(GorillaCore::new(32))
    }

    pub fn new_i64() -> Self {
        Self::Int64(GorillaCore::new(64))
    }

    pub fn new_f32() -> Self {
        Self::Float(GorillaCore::new(32))
    }

    pub fn new_f64() -> Self {
        Self::Double(GorillaCore::new(64))
    }

    pub fn encode_i32(&mut self, value: i32, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int32(core) => core.encode(value as u32, out),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla encoder type mismatch: expected i32".into(),
            )),
        }
    }

    pub fn encode_i64(&mut self, value: i64, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int64(core) => core.encode(value as u64, out),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla encoder type mismatch: expected i64".into(),
            )),
        }
    }

    pub fn encode_f32(&mut self, value: f32, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Float(core) => core.encode(value.to_bits(), out),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla encoder type mismatch: expected f32".into(),
            )),
        }
    }

    pub fn encode_f64(&mut self, value: f64, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Double(core) => core.encode(value.to_bits(), out),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla encoder type mismatch: expected f64".into(),
            )),
        }
    }

    /// Flush any buffered bits to the output.
    ///
    /// MUST be called after the last value to ensure partial bytes are written.
    pub fn flush(&mut self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Int32(core) | Self::Float(core) => core.flush(out),
            Self::Int64(core) | Self::Double(core) => core.flush(out),
        }
    }

    pub fn reset(&mut self) {
        match self {
            Self::Int32(core) | Self::Float(core) => core.reset(),
            Self::Int64(core) | Self::Double(core) => core.reset(),
        }
    }
}

/// Gorilla decoder for i32, i64, f32, and f64 values.
#[derive(Debug, Clone)]
pub enum GorillaDecoder {
    Int32(GorillaCore<u32>),
    Int64(GorillaCore<u64>),
    Float(GorillaCore<u32>),
    Double(GorillaCore<u64>),
}

impl GorillaDecoder {
    pub fn new_i32() -> Self {
        Self::Int32(GorillaCore::new(32))
    }

    pub fn new_i64() -> Self {
        Self::Int64(GorillaCore::new(64))
    }

    pub fn new_f32() -> Self {
        Self::Float(GorillaCore::new(32))
    }

    pub fn new_f64() -> Self {
        Self::Double(GorillaCore::new(64))
    }

    pub fn decode_i32(&mut self, input: &mut impl Read) -> Result<i32> {
        match self {
            Self::Int32(core) => Ok(core.decode(input)? as i32),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla decoder type mismatch: expected i32".into(),
            )),
        }
    }

    pub fn decode_i64(&mut self, input: &mut impl Read) -> Result<i64> {
        match self {
            Self::Int64(core) => Ok(core.decode(input)? as i64),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla decoder type mismatch: expected i64".into(),
            )),
        }
    }

    pub fn decode_f32(&mut self, input: &mut impl Read) -> Result<f32> {
        match self {
            Self::Float(core) => Ok(f32::from_bits(core.decode(input)?)),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla decoder type mismatch: expected f32".into(),
            )),
        }
    }

    pub fn decode_f64(&mut self, input: &mut impl Read) -> Result<f64> {
        match self {
            Self::Double(core) => Ok(f64::from_bits(core.decode(input)?)),
            _ => Err(TsFileError::InvalidArg(
                "Gorilla decoder type mismatch: expected f64".into(),
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

/// Core Gorilla encoder/decoder logic, generic over u32/u64.
///
/// Bit-level operations for XOR-based compression. Uses a bit buffer
/// to accumulate partial bytes before writing to output.
#[derive(Debug, Clone)]
struct GorillaCore<T: GorillaBits> {
    /// Previous value for XOR computation.
    prev_value: Option<T>,
    /// Number of leading zeros in previous XOR.
    prev_leading_zeros: u32,
    /// Number of trailing zeros in previous XOR.
    prev_trailing_zeros: u32,
    /// Bit buffer for accumulating partial bytes.
    bit_buffer: u64,
    /// Number of valid bits in bit_buffer (0-63).
    bits_in_buffer: u8,
    /// Total bit width (32 or 64).
    value_bits: u32,
}

impl<T: GorillaBits> GorillaCore<T> {
    fn new(value_bits: u32) -> Self {
        Self {
            prev_value: None,
            prev_leading_zeros: 0,
            prev_trailing_zeros: 0,
            bit_buffer: 0,
            bits_in_buffer: 0,
            value_bits,
        }
    }

    fn encode(&mut self, value: T, out: &mut Vec<u8>) -> Result<()> {
        match self.prev_value {
            None => {
                // First value: write full value
                self.write_bits(value.to_u64(), self.value_bits, out)?;
                self.prev_value = Some(value);
            }
            Some(prev) => {
                let xor_val = value.xor(prev);

                if xor_val == T::ZERO {
                    // Value unchanged: write control bit '0'
                    self.write_bits(0, 1, out)?;
                } else {
                    // Value changed: write control bit '1' + XOR encoding
                    self.write_bits(1, 1, out)?;

                    let leading = xor_val.leading_zeros();
                    let trailing = xor_val.trailing_zeros();
                    let meaningful_bits = self.value_bits - leading - trailing;

                    // Check if we can reuse previous block parameters
                    // Condition: meaningful bits fit within previous window
                    let can_use_prev_block = self.prev_leading_zeros > 0 || self.prev_trailing_zeros > 0; // Have previous block
                    let fits_in_prev_window = leading >= self.prev_leading_zeros
                        && trailing >= self.prev_trailing_zeros;

                    if can_use_prev_block && fits_in_prev_window {
                        // Use previous block: control bit '0' + meaningful bits
                        self.write_bits(0, 1, out)?;
                        let shift = self.prev_trailing_zeros;
                        let bits_to_write = self.value_bits - self.prev_leading_zeros - self.prev_trailing_zeros;
                        let mask = if bits_to_write >= 64 {
                            u64::MAX
                        } else {
                            (1u64 << bits_to_write) - 1
                        };
                        let value_bits = (xor_val.to_u64() >> shift) & mask;
                        self.write_bits(value_bits, bits_to_write, out)?;
                    } else {
                        // New block: control bit '1' + leading + length + meaningful bits
                        self.write_bits(1, 1, out)?;

                        // Write leading zeros count (5 bits for 32-bit, 6 bits for 64-bit)
                        let leading_bits = if self.value_bits == 32 { 5 } else { 6 };
                        self.write_bits(leading as u64, leading_bits, out)?;

                        // Write meaningful bits length - 1 (6 bits), ensure at least 1 bit
                        let meaningful_to_write = meaningful_bits.max(1);
                        self.write_bits((meaningful_to_write - 1) as u64, 6, out)?;

                        // Write meaningful bits
                        let shift = trailing;
                        let mask = if meaningful_to_write >= 64 {
                            u64::MAX
                        } else {
                            (1u64 << meaningful_to_write) - 1
                        };
                        let value_bits = (xor_val.to_u64() >> shift) & mask;
                        self.write_bits(value_bits, meaningful_to_write, out)?;

                        self.prev_leading_zeros = leading;
                        self.prev_trailing_zeros = trailing;
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
                // First value: read full value
                let value = T::from_u64(self.read_bits(self.value_bits, input)?);
                self.prev_value = Some(value);
                Ok(value)
            }
            Some(prev) => {
                // Read control bit
                let control = self.read_bits(1, input)?;

                if control == 0 {
                    // Value unchanged
                    Ok(prev)
                } else {
                    // Read second control bit
                    let control2 = self.read_bits(1, input)?;

                    let xor_val = if control2 == 0 {
                        // Use previous block
                        let bits_to_read = self.value_bits - self.prev_leading_zeros - self.prev_trailing_zeros;
                        let value_bits = self.read_bits(bits_to_read, input)?;
                        T::from_u64(value_bits << self.prev_trailing_zeros)
                    } else {
                        // New block: read leading + length + meaningful bits
                        let leading_bits = if self.value_bits == 32 { 5 } else { 6 };
                        let leading = self.read_bits(leading_bits, input)? as u32;
                        let meaningful_bits = self.read_bits(6, input)? as u32 + 1;

                        let value_bits = self.read_bits(meaningful_bits, input)?;
                        let trailing = self.value_bits - leading - meaningful_bits;

                        self.prev_leading_zeros = leading;
                        self.prev_trailing_zeros = trailing;

                        T::from_u64(value_bits << trailing)
                    };

                    let value = prev.xor(T::from_u64(xor_val.to_u64()));
                    self.prev_value = Some(value);
                    Ok(value)
                }
            }
        }
    }

    /// Write `count` bits from `value` to the output.
    fn write_bits(&mut self, value: u64, count: u32, out: &mut Vec<u8>) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        if count > 64 {
            return Err(TsFileError::InvalidArg(format!(
                "cannot write more than 64 bits at once: {}",
                count
            )));
        }

        // Mask to count bits (avoid overflow for count >= 64)
        let mask = if count >= 64 { u64::MAX } else { (1u64 << count) - 1 };
        let masked_value = value & mask;

        // Add bits to buffer (avoid shift overflow for count >= 64)
        if count >= 64 {
            // If adding 64 bits, buffer should be empty
            self.bit_buffer = masked_value;
        } else {
            self.bit_buffer = (self.bit_buffer << count) | masked_value;
        }
        self.bits_in_buffer += count as u8;

        // Flush complete bytes
        while self.bits_in_buffer >= 8 {
            self.bits_in_buffer -= 8;
            let byte = (self.bit_buffer >> self.bits_in_buffer) as u8;
            out.push(byte);
            let mask = if self.bits_in_buffer == 0 {
                0
            } else {
                (1u64 << self.bits_in_buffer) - 1
            };
            self.bit_buffer &= mask;
        }

        Ok(())
    }

    /// Read `count` bits from the input.
    fn read_bits(&mut self, count: u32, input: &mut impl Read) -> Result<u64> {
        if count == 0 {
            return Ok(0);
        }
        if count > 64 {
            return Err(TsFileError::InvalidArg(format!(
                "cannot read more than 64 bits at once: {}",
                count
            )));
        }

        // Refill buffer if needed
        while (self.bits_in_buffer as u32) < count {
            let mut byte = [0u8; 1];
            input.read_exact(&mut byte)?;
            self.bit_buffer = (self.bit_buffer << 8) | (byte[0] as u64);
            self.bits_in_buffer += 8;
        }

        // Extract bits (avoid shift overflow for count >= 64)
        self.bits_in_buffer -= count as u8;
        let mask = if count >= 64 { u64::MAX } else { (1u64 << count) - 1 };
        let value = (self.bit_buffer >> self.bits_in_buffer) & mask;
        let buffer_mask = if self.bits_in_buffer == 0 {
            0
        } else {
            (1u64 << self.bits_in_buffer) - 1
        };
        self.bit_buffer &= buffer_mask;

        Ok(value)
    }

    fn flush(&mut self, out: &mut Vec<u8>) -> Result<()> {
        if self.bits_in_buffer > 0 {
            // Pad remaining bits to complete a byte
            let padding = 8 - self.bits_in_buffer;
            self.bit_buffer <<= padding;
            out.push(self.bit_buffer as u8);
            self.bits_in_buffer = 0;
            self.bit_buffer = 0;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.prev_value = None;
        self.prev_leading_zeros = 0;
        self.prev_trailing_zeros = 0;
        self.bit_buffer = 0;
        self.bits_in_buffer = 0;
    }
}

/// Trait for types that can be used with Gorilla encoding.
trait GorillaBits: Copy + Eq + std::fmt::Debug {
    const ZERO: Self;
    fn leading_zeros(self) -> u32;
    fn trailing_zeros(self) -> u32;
    fn xor(self, other: Self) -> Self;
    fn to_u64(self) -> u64;
    fn from_u64(value: u64) -> Self;
}

impl GorillaBits for u32 {
    const ZERO: Self = 0;
    fn leading_zeros(self) -> u32 {
        self.leading_zeros()
    }
    fn trailing_zeros(self) -> u32 {
        self.trailing_zeros()
    }
    fn xor(self, other: Self) -> Self {
        self ^ other
    }
    fn to_u64(self) -> u64 {
        self as u64
    }
    fn from_u64(value: u64) -> Self {
        value as u32
    }
}

impl GorillaBits for u64 {
    const ZERO: Self = 0;
    fn leading_zeros(self) -> u32 {
        self.leading_zeros()
    }
    fn trailing_zeros(self) -> u32 {
        self.trailing_zeros()
    }
    fn xor(self, other: Self) -> Self {
        self ^ other
    }
    fn to_u64(self) -> u64 {
        self
    }
    fn from_u64(value: u64) -> Self {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Cursor;

    // === Basic round-trip tests ===

    #[test]
    fn f32_constant_values() {
        let mut encoder = GorillaEncoder::new_f32();
        let mut decoder = GorillaDecoder::new_f32();

        // Constant values compress extremely well
        let mut encoded = Vec::new();
        for _ in 0..100 {
            encoder.encode_f32(72.5, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        // First value: 32 bits, rest: 1 bit each = 32 + 99 = 131 bits = ~17 bytes
        assert!(encoded.len() < 20, "encoded {} bytes", encoded.len());

        let mut cursor = Cursor::new(encoded);
        for _ in 0..100 {
            assert_eq!(decoder.decode_f32(&mut cursor).unwrap(), 72.5);
        }
    }

    #[test]
    fn f32_slowly_changing() {
        let mut encoder = GorillaEncoder::new_f32();
        let mut decoder = GorillaDecoder::new_f32();

        let values = vec![72.0, 72.1, 72.2, 72.3, 72.4, 72.5];

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_f32(value, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        // Gorilla may not always compress better than plain for small datasets
        // The key is that decoding works correctly
        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_f32(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn i32_round_trip() {
        let mut encoder = GorillaEncoder::new_i32();
        let mut decoder = GorillaDecoder::new_i32();

        let values = vec![100, 101, 102, 103, 100, 100, 104];

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i32(value, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_i32(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn i64_round_trip() {
        let mut encoder = GorillaEncoder::new_i64();
        let mut decoder = GorillaDecoder::new_i64();

        let values: Vec<i64> = vec![
            1000000000,
            1000000001,
            1000000001,
            1000000002,
            1000000000,
        ];

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_i64(value, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_i64(&mut cursor).unwrap(), expected);
        }
    }

    #[test]
    fn f64_round_trip() {
        let mut encoder = GorillaEncoder::new_f64();
        let mut decoder = GorillaDecoder::new_f64();

        let values = vec![3.14159, 3.14160, 3.14161, 3.14159];

        let mut encoded = Vec::new();
        for &value in &values {
            encoder.encode_f64(value, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        let mut cursor = Cursor::new(encoded);
        for &expected in &values {
            assert_eq!(decoder.decode_f64(&mut cursor).unwrap(), expected);
        }
    }

    // === Property-based tests ===

    proptest! {
        #[test]
        fn prop_f32_round_trip(values: Vec<f32>) {
            if values.is_empty() {
                return Ok(());
            }

            // Filter out NaN (NaN != NaN breaks equality)
            let values: Vec<f32> = values.into_iter().filter(|v| !v.is_nan()).collect();
            if values.is_empty() {
                return Ok(());
            }

            let mut encoder = GorillaEncoder::new_f32();
            let mut decoder = GorillaDecoder::new_f32();

            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_f32(value, &mut encoded).unwrap();
            }
            encoder.flush(&mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            for &expected in &values {
                let decoded = decoder.decode_f32(&mut cursor).unwrap();
                prop_assert_eq!(decoded, expected);
            }
        }

        #[test]
        fn prop_i32_round_trip(values: Vec<i32>) {
            if values.is_empty() {
                return Ok(());
            }

            let mut encoder = GorillaEncoder::new_i32();
            let mut decoder = GorillaDecoder::new_i32();

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
    }

    // === Compression ratio test ===

    #[test]
    fn compression_ratio_info() {
        println!("\nGorilla compression ratios:");

        // Test 1: Constant values (best case)
        let constant: Vec<f32> = vec![42.0; 100];
        test_compression("Constant values (100x 42.0)", &constant);

        // Test 2: Slowly changing
        let slowly: Vec<f32> = (0..100).map(|i| 20.0 + (i as f32) * 0.1).collect();
        test_compression("Slowly changing (20.0 to 30.0)", &slowly);

        // Test 3: Random (worst case)
        let random: Vec<f32> = vec![1.0, 99.9, 2.5, 88.1, 3.7, 77.3, 4.2, 66.6];
        test_compression("Random values", &random);
    }

    fn test_compression(name: &str, values: &[f32]) {
        let mut encoder = GorillaEncoder::new_f32();
        let mut encoded = Vec::new();
        for &value in values {
            encoder.encode_f32(value, &mut encoded).unwrap();
        }
        encoder.flush(&mut encoded).unwrap();

        let plain_size = values.len() * 4;
        let ratio = plain_size as f64 / encoded.len() as f64;
        println!(
            "  {}: {} values, plain {} bytes, gorilla {} bytes (ratio {:.2}x)",
            name,
            values.len(),
            plain_size,
            encoded.len(),
            ratio
        );
    }
}
