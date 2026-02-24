// Unified encoder interface: enum dispatch for all encoding algorithms.
//
// DESIGN PATTERN:
// The C++ implementation uses a factory pattern with EncoderFactory::alloc_encoder()
// returning Encoder* (abstract base class). Callers store `unique_ptr<Encoder>` and
// use virtual dispatch through vtables.
//
// In Rust we use enum dispatch instead: the Encoder enum has a variant for each
// algorithm, and `match` statements provide exhaustive dispatch at compile time.
// This eliminates heap allocation and vtable overhead while maintaining type safety.
//
// BENEFITS OF ENUM DISPATCH:
// - Zero cost: match compiles to jump table or conditional branches (no vtable)
// - Exhaustiveness: compiler enforces handling of all variants
// - No heap allocation: encoders live on stack or inline in structs
// - Type safety: impossible to call wrong method for wrong type
//
// USAGE:
//   let mut encoder = Encoder::new_gorilla_f32();
//   encoder.encode_f32(value, &mut output)?;
//   encoder.flush(&mut output)?;

use crate::encoding::{dictionary, gorilla, plain, rle, sprintz, ts2diff, zigzag};
use crate::error::{Result, TsFileError};
use crate::types::{TSDataType, TSEncoding};

/// Unified encoder enum for all encoding algorithms.
///
/// Each variant wraps a specific encoder type. The enum provides a common
/// interface via match dispatch, eliminating the need for trait objects.
#[derive(Debug, Clone)]
pub enum Encoder {
    Plain(plain::PlainEncoder),
    Rle(rle::RleEncoder),
    Zigzag(zigzag::ZigzagEncoder),
    Dictionary(dictionary::DictionaryEncoder),
    Gorilla(gorilla::GorillaEncoder),
    Ts2Diff(ts2diff::Ts2DiffEncoder),
    Sprintz(sprintz::SprintzEncoder),
}

impl Encoder {
    /// Create an encoder for the given data type and encoding algorithm.
    ///
    /// This replaces the C++ EncoderFactory pattern with a simple constructor.
    pub fn new(data_type: TSDataType, encoding: TSEncoding) -> Result<Self> {
        match (data_type, encoding) {
            // Plain: all types
            (TSDataType::Boolean, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainEncoder::new())),
            (TSDataType::Int32, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainEncoder::new())),
            (TSDataType::Int64, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainEncoder::new())),
            (TSDataType::Float, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainEncoder::new())),
            (TSDataType::Double, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainEncoder::new())),
            (TSDataType::Text, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainEncoder::new())),

            // RLE: Int32, Int64
            (TSDataType::Int32, TSEncoding::Rle) => Ok(Self::Rle(rle::RleEncoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Rle) => Ok(Self::Rle(rle::RleEncoder::new_i64())),

            // Zigzag: Int32, Int64
            (TSDataType::Int32, TSEncoding::Zigzag) => Ok(Self::Zigzag(zigzag::ZigzagEncoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Zigzag) => Ok(Self::Zigzag(zigzag::ZigzagEncoder::new_i64())),

            // Dictionary: Text only
            (TSDataType::Text, TSEncoding::Dictionary) => Ok(Self::Dictionary(dictionary::DictionaryEncoder::new())),

            // Gorilla: Int32, Int64, Float, Double
            (TSDataType::Int32, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaEncoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaEncoder::new_i64())),
            (TSDataType::Float, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaEncoder::new_f32())),
            (TSDataType::Double, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaEncoder::new_f64())),

            // TS2DIFF: Int32, Int64, Float, Double
            (TSDataType::Int32, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffEncoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffEncoder::new_i64())),
            (TSDataType::Float, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffEncoder::new_f32())),
            (TSDataType::Double, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffEncoder::new_f64())),

            // Sprintz: Int32, Int64, Float, Double
            (TSDataType::Int32, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzEncoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzEncoder::new_i64())),
            (TSDataType::Float, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzEncoder::new_f32())),
            (TSDataType::Double, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzEncoder::new_f64())),

            // Unsupported combinations
            (dt, enc) => Err(TsFileError::Unsupported(format!(
                "encoding {:?} not supported for data type {:?}",
                enc, dt
            ))),
        }
    }

    // Type-specific encode methods

    pub fn encode_bool(&mut self, value: bool, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.encode_bool(value, out),
            _ => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Boolean,
                actual: TSDataType::Int32, // Placeholder
            }),
        }
    }

    pub fn encode_i32(&mut self, value: i32, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.encode_i32(value, out),
            Self::Rle(enc) => enc.encode_i32(value, out),
            Self::Zigzag(enc) => enc.encode_i32(value, out),
            Self::Gorilla(enc) => enc.encode_i32(value, out),
            Self::Ts2Diff(enc) => enc.encode_i32(value, out),
            Self::Sprintz(enc) => enc.encode_i32(value, out),
            Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Int32,
                actual: TSDataType::Text,
            }),
        }
    }

    pub fn encode_i64(&mut self, value: i64, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.encode_i64(value, out),
            Self::Rle(enc) => enc.encode_i64(value, out),
            Self::Zigzag(enc) => enc.encode_i64(value, out),
            Self::Gorilla(enc) => enc.encode_i64(value, out),
            Self::Ts2Diff(enc) => enc.encode_i64(value, out),
            Self::Sprintz(enc) => enc.encode_i64(value, out),
            Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Int64,
                actual: TSDataType::Text,
            }),
        }
    }

    pub fn encode_f32(&mut self, value: f32, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.encode_f32(value, out),
            Self::Gorilla(enc) => enc.encode_f32(value, out),
            Self::Ts2Diff(enc) => enc.encode_f32(value, out),
            Self::Sprintz(enc) => enc.encode_f32(value, out),
            Self::Rle(_) | Self::Zigzag(_) | Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Float,
                actual: TSDataType::Int32,
            }),
        }
    }

    pub fn encode_f64(&mut self, value: f64, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.encode_f64(value, out),
            Self::Gorilla(enc) => enc.encode_f64(value, out),
            Self::Ts2Diff(enc) => enc.encode_f64(value, out),
            Self::Sprintz(enc) => enc.encode_f64(value, out),
            Self::Rle(_) | Self::Zigzag(_) | Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Double,
                actual: TSDataType::Int32,
            }),
        }
    }

    pub fn encode_string(&mut self, value: &str, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.encode_bytes(value.as_bytes(), out),
            Self::Dictionary(enc) => enc.encode_string(value),
            _ => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Text,
                actual: TSDataType::Int32,
            }),
        }
    }

    pub fn encode_bytes(&mut self, value: &[u8], out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.encode_bytes(value, out),
            Self::Dictionary(enc) => enc.encode_bytes(value),
            _ => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Text,
                actual: TSDataType::Int32,
            }),
        }
    }

    /// Flush any buffered data.
    ///
    /// Must be called after encoding the last value to ensure all data is written.
    pub fn flush(&mut self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(enc) => enc.flush(out),
            Self::Rle(enc) => enc.flush(out),
            Self::Zigzag(enc) => enc.flush(out),
            Self::Dictionary(enc) => enc.flush(out),
            Self::Gorilla(enc) => enc.flush(out),
            Self::Ts2Diff(enc) => enc.flush(out),
            Self::Sprintz(enc) => enc.flush(out),
        }
    }

    /// Reset encoder state for reuse.
    pub fn reset(&mut self) {
        match self {
            Self::Plain(enc) => enc.reset(),
            Self::Rle(enc) => enc.reset(),
            Self::Zigzag(enc) => enc.reset(),
            Self::Dictionary(enc) => enc.reset(),
            Self::Gorilla(enc) => enc.reset(),
            Self::Ts2Diff(enc) => enc.reset(),
            Self::Sprintz(enc) => enc.reset(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use crate::encoding::decoder::Decoder;

    #[test]
    fn test_factory_plain_i32() {
        let mut encoder = Encoder::new(TSDataType::Int32, TSEncoding::Plain).unwrap();
        let mut out = Vec::new();
        encoder.encode_i32(42, &mut out).unwrap();
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn test_factory_gorilla_f32() {
        let mut encoder = Encoder::new(TSDataType::Float, TSEncoding::Gorilla).unwrap();
        let mut out = Vec::new();
        encoder.encode_f32(3.14, &mut out).unwrap();
        encoder.flush(&mut out).unwrap();
        assert!(!out.is_empty());
    }

    #[test]
    fn test_unsupported_combination() {
        let result = Encoder::new(TSDataType::Boolean, TSEncoding::Gorilla);
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_all_algorithms() {
        // Test each algorithm with appropriate data type
        let test_cases = vec![
            (TSDataType::Int32, TSEncoding::Plain, vec![1, 2, 3, 4, 5]),
            (TSDataType::Int32, TSEncoding::Rle, vec![1, 1, 1, 2, 2, 2]),
            (TSDataType::Int32, TSEncoding::Zigzag, vec![-5, -1, 0, 1, 5]),
            (TSDataType::Int32, TSEncoding::Gorilla, vec![100, 101, 102, 103, 104]),
            (TSDataType::Int32, TSEncoding::Ts2Diff, vec![10, 11, 12, 13, 14]),
            (TSDataType::Int32, TSEncoding::Sprintz, vec![0, 1, 2, 3, 4]),
        ];

        for (data_type, encoding, values) in test_cases {
            let mut encoder = Encoder::new(data_type, encoding).unwrap();
            let mut decoder = Decoder::new(data_type, encoding).unwrap();

            let mut encoded = Vec::new();
            for &value in &values {
                encoder.encode_i32(value, &mut encoded).unwrap();
            }
            encoder.flush(&mut encoded).unwrap();

            let mut cursor = Cursor::new(encoded);
            for &expected in &values {
                let decoded = decoder.decode_i32(&mut cursor).unwrap();
                assert_eq!(decoded, expected, "failed for {:?}/{:?}", data_type, encoding);
            }
        }
    }
}
