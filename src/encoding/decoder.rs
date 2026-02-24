// Unified decoder interface: enum dispatch for all decoding algorithms.
//
// DESIGN PATTERN:
// Similar to Encoder, this replaces the C++ DecoderFactory + virtual dispatch
// pattern with enum-based compile-time dispatch. See encoder.rs for detailed
// explanation of the design rationale.
//
// USAGE:
//   let mut decoder = Decoder::new(TSDataType::Float, TSEncoding::Gorilla)?;
//   let value = decoder.decode_f32(&mut input)?;

use crate::encoding::{dictionary, gorilla, plain, rle, sprintz, ts2diff, zigzag};
use crate::error::{Result, TsFileError};
use crate::types::{TSDataType, TSEncoding};
use std::io::Read;

/// Unified decoder enum for all decoding algorithms.
///
/// Each variant wraps a specific decoder type. The enum provides a common
/// interface via match dispatch.
#[derive(Debug, Clone)]
pub enum Decoder {
    Plain(plain::PlainDecoder),
    Rle(rle::RleDecoder),
    Zigzag(zigzag::ZigzagDecoder),
    Dictionary(dictionary::DictionaryDecoder),
    Gorilla(gorilla::GorillaDecoder),
    Ts2Diff(ts2diff::Ts2DiffDecoder),
    Sprintz(sprintz::SprintzDecoder),
}

impl Decoder {
    /// Create a decoder for the given data type and encoding algorithm.
    ///
    /// Must match the encoder that produced the data.
    pub fn new(data_type: TSDataType, encoding: TSEncoding) -> Result<Self> {
        match (data_type, encoding) {
            // Plain: all types
            (TSDataType::Boolean, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainDecoder::new())),
            (TSDataType::Int32, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainDecoder::new())),
            (TSDataType::Int64, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainDecoder::new())),
            (TSDataType::Float, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainDecoder::new())),
            (TSDataType::Double, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainDecoder::new())),
            (TSDataType::Text, TSEncoding::Plain) => Ok(Self::Plain(plain::PlainDecoder::new())),

            // RLE: Int32, Int64
            (TSDataType::Int32, TSEncoding::Rle) => Ok(Self::Rle(rle::RleDecoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Rle) => Ok(Self::Rle(rle::RleDecoder::new_i64())),

            // Zigzag: Int32, Int64
            (TSDataType::Int32, TSEncoding::Zigzag) => Ok(Self::Zigzag(zigzag::ZigzagDecoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Zigzag) => Ok(Self::Zigzag(zigzag::ZigzagDecoder::new_i64())),

            // Dictionary: Text only
            (TSDataType::Text, TSEncoding::Dictionary) => Ok(Self::Dictionary(dictionary::DictionaryDecoder::new())),

            // Gorilla: Int32, Int64, Float, Double
            (TSDataType::Int32, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaDecoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaDecoder::new_i64())),
            (TSDataType::Float, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaDecoder::new_f32())),
            (TSDataType::Double, TSEncoding::Gorilla) => Ok(Self::Gorilla(gorilla::GorillaDecoder::new_f64())),

            // TS2DIFF: Int32, Int64, Float, Double
            (TSDataType::Int32, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffDecoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffDecoder::new_i64())),
            (TSDataType::Float, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffDecoder::new_f32())),
            (TSDataType::Double, TSEncoding::Ts2Diff) => Ok(Self::Ts2Diff(ts2diff::Ts2DiffDecoder::new_f64())),

            // Sprintz: Int32, Int64, Float, Double
            (TSDataType::Int32, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzDecoder::new_i32())),
            (TSDataType::Int64, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzDecoder::new_i64())),
            (TSDataType::Float, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzDecoder::new_f32())),
            (TSDataType::Double, TSEncoding::Sprintz) => Ok(Self::Sprintz(sprintz::SprintzDecoder::new_f64())),

            // Unsupported combinations
            (dt, enc) => Err(TsFileError::Unsupported(format!(
                "encoding {:?} not supported for data type {:?}",
                enc, dt
            ))),
        }
    }

    // Type-specific decode methods

    pub fn decode_bool(&mut self, input: &mut impl Read) -> Result<bool> {
        match self {
            Self::Plain(dec) => dec.decode_bool(input),
            _ => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Boolean,
                actual: TSDataType::Int32, // Placeholder
            }),
        }
    }

    pub fn decode_i32(&mut self, input: &mut impl Read) -> Result<i32> {
        match self {
            Self::Plain(dec) => dec.decode_i32(input),
            Self::Rle(dec) => dec.decode_i32(input),
            Self::Zigzag(dec) => dec.decode_i32(input),
            Self::Gorilla(dec) => dec.decode_i32(input),
            Self::Ts2Diff(dec) => dec.decode_i32(input),
            Self::Sprintz(dec) => dec.decode_i32(input),
            Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Int32,
                actual: TSDataType::Text,
            }),
        }
    }

    pub fn decode_i64(&mut self, input: &mut impl Read) -> Result<i64> {
        match self {
            Self::Plain(dec) => dec.decode_i64(input),
            Self::Rle(dec) => dec.decode_i64(input),
            Self::Zigzag(dec) => dec.decode_i64(input),
            Self::Gorilla(dec) => dec.decode_i64(input),
            Self::Ts2Diff(dec) => dec.decode_i64(input),
            Self::Sprintz(dec) => dec.decode_i64(input),
            Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Int64,
                actual: TSDataType::Text,
            }),
        }
    }

    pub fn decode_f32(&mut self, input: &mut impl Read) -> Result<f32> {
        match self {
            Self::Plain(dec) => dec.decode_f32(input),
            Self::Gorilla(dec) => dec.decode_f32(input),
            Self::Ts2Diff(dec) => dec.decode_f32(input),
            Self::Sprintz(dec) => dec.decode_f32(input),
            Self::Rle(_) | Self::Zigzag(_) | Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Float,
                actual: TSDataType::Int32,
            }),
        }
    }

    pub fn decode_f64(&mut self, input: &mut impl Read) -> Result<f64> {
        match self {
            Self::Plain(dec) => dec.decode_f64(input),
            Self::Gorilla(dec) => dec.decode_f64(input),
            Self::Ts2Diff(dec) => dec.decode_f64(input),
            Self::Sprintz(dec) => dec.decode_f64(input),
            Self::Rle(_) | Self::Zigzag(_) | Self::Dictionary(_) => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Double,
                actual: TSDataType::Int32,
            }),
        }
    }

    pub fn decode_string(&mut self, input: &mut impl Read) -> Result<String> {
        match self {
            Self::Plain(dec) => {
                let bytes = dec.decode_bytes(input)?;
                String::from_utf8(bytes).map_err(|e| {
                    TsFileError::InvalidArg(format!("invalid UTF-8 in string: {}", e))
                })
            }
            Self::Dictionary(dec) => dec.decode_string(input),
            _ => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Text,
                actual: TSDataType::Int32,
            }),
        }
    }

    pub fn decode_bytes(&mut self, input: &mut impl Read) -> Result<Vec<u8>> {
        match self {
            Self::Plain(dec) => dec.decode_bytes(input),
            Self::Dictionary(dec) => dec.decode_bytes(input),
            _ => Err(TsFileError::TypeMismatch {
                expected: TSDataType::Text,
                actual: TSDataType::Int32,
            }),
        }
    }

    /// Reset decoder state for reuse.
    pub fn reset(&mut self) {
        match self {
            Self::Plain(dec) => dec.reset(),
            Self::Rle(dec) => dec.reset(),
            Self::Zigzag(dec) => dec.reset(),
            Self::Dictionary(dec) => dec.reset(),
            Self::Gorilla(dec) => dec.reset(),
            Self::Ts2Diff(dec) => dec.reset(),
            Self::Sprintz(dec) => dec.reset(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_plain_i32() {
        let decoder = Decoder::new(TSDataType::Int32, TSEncoding::Plain).unwrap();
        assert!(matches!(decoder, Decoder::Plain(_)));
    }

    #[test]
    fn test_factory_gorilla_f64() {
        let decoder = Decoder::new(TSDataType::Double, TSEncoding::Gorilla).unwrap();
        assert!(matches!(decoder, Decoder::Gorilla(_)));
    }

    #[test]
    fn test_unsupported_combination() {
        let result = Decoder::new(TSDataType::Boolean, TSEncoding::Gorilla);
        assert!(result.is_err());
    }
}
