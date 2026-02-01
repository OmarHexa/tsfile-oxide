// C++ defines TSDataType, TSEncoding, CompressionType as plain enums in
// db_common.h. We use #[repr(u8)] to lock discriminant values to match
// the on-disk format exactly — this is critical for binary compatibility.
//
// C++ relies on implicit integer conversion for serialization (just cast
// the enum to int). In Rust we implement TryFrom<u8> explicitly so that
// deserialization is checked — an invalid byte produces an error rather
// than undefined behavior.

use crate::error::{TsFileError, Result};

/// Data types supported by TsFile. Discriminant values must match C++ db_common.h.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TSDataType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float = 3,
    Double = 4,
    Text = 5,
}

impl TryFrom<u8> for TSDataType {
    type Error = TsFileError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Boolean),
            1 => Ok(Self::Int32),
            2 => Ok(Self::Int64),
            3 => Ok(Self::Float),
            4 => Ok(Self::Double),
            5 => Ok(Self::Text),
            _ => Err(TsFileError::InvalidArg(format!(
                "unknown TSDataType discriminant: {value}"
            ))),
        }
    }
}

/// Encoding algorithms. Discriminant values must match C++ db_common.h.
///
/// Note: discriminants are non-contiguous (0,1,2,4,8,16,32) — this matches
/// the C++ enum where some values were reserved or removed over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TSEncoding {
    Plain = 0,
    Dictionary = 1,
    Rle = 2,
    Ts2Diff = 4,
    Gorilla = 8,
    Zigzag = 16,
    Sprintz = 32,
}

impl TryFrom<u8> for TSEncoding {
    type Error = TsFileError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Dictionary),
            2 => Ok(Self::Rle),
            4 => Ok(Self::Ts2Diff),
            8 => Ok(Self::Gorilla),
            16 => Ok(Self::Zigzag),
            32 => Ok(Self::Sprintz),
            _ => Err(TsFileError::InvalidArg(format!(
                "unknown TSEncoding discriminant: {value}"
            ))),
        }
    }
}

/// Compression types. Discriminant values must match C++ db_common.h.
///
/// Note: Lz4 = 7 (not 4) — values 4-6 are used by other compression types
/// in the broader IoTDB ecosystem but not supported in this implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CompressionType {
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Lzo = 3,
    Lz4 = 7,
}

impl TryFrom<u8> for CompressionType {
    type Error = TsFileError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Uncompressed),
            1 => Ok(Self::Snappy),
            2 => Ok(Self::Gzip),
            3 => Ok(Self::Lzo),
            7 => Ok(Self::Lz4),
            _ => Err(TsFileError::InvalidArg(format!(
                "unknown CompressionType discriminant: {value}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verify discriminant values match C++ db_common.h. These are baked into the
    // on-disk format — changing them would break binary compatibility.

    #[test]
    fn data_type_discriminants() {
        assert_eq!(TSDataType::Boolean as u8, 0);
        assert_eq!(TSDataType::Int32 as u8, 1);
        assert_eq!(TSDataType::Int64 as u8, 2);
        assert_eq!(TSDataType::Float as u8, 3);
        assert_eq!(TSDataType::Double as u8, 4);
        assert_eq!(TSDataType::Text as u8, 5);
    }

    #[test]
    fn encoding_discriminants() {
        assert_eq!(TSEncoding::Plain as u8, 0);
        assert_eq!(TSEncoding::Dictionary as u8, 1);
        assert_eq!(TSEncoding::Rle as u8, 2);
        assert_eq!(TSEncoding::Ts2Diff as u8, 4);
        assert_eq!(TSEncoding::Gorilla as u8, 8);
        assert_eq!(TSEncoding::Zigzag as u8, 16);
        assert_eq!(TSEncoding::Sprintz as u8, 32);
    }

    #[test]
    fn compression_discriminants() {
        assert_eq!(CompressionType::Uncompressed as u8, 0);
        assert_eq!(CompressionType::Snappy as u8, 1);
        assert_eq!(CompressionType::Gzip as u8, 2);
        assert_eq!(CompressionType::Lzo as u8, 3);
        assert_eq!(CompressionType::Lz4 as u8, 7);
    }

    #[test]
    fn data_type_try_from_valid() {
        for byte in 0..=5u8 {
            assert!(TSDataType::try_from(byte).is_ok());
        }
    }

    #[test]
    fn data_type_try_from_invalid() {
        assert!(TSDataType::try_from(6).is_err());
        assert!(TSDataType::try_from(255).is_err());
    }

    #[test]
    fn data_type_round_trip() {
        let types = [
            TSDataType::Boolean,
            TSDataType::Int32,
            TSDataType::Int64,
            TSDataType::Float,
            TSDataType::Double,
            TSDataType::Text,
        ];
        for dt in types {
            assert_eq!(TSDataType::try_from(dt as u8).unwrap(), dt);
        }
    }

    #[test]
    fn encoding_try_from_valid() {
        let valid = [0, 1, 2, 4, 8, 16, 32];
        for byte in valid {
            assert!(TSEncoding::try_from(byte).is_ok());
        }
    }

    #[test]
    fn encoding_try_from_invalid_gaps() {
        // Values 3, 5, 6, 7 are gaps in the encoding enum
        for byte in [3, 5, 6, 7, 9, 15, 17, 31, 33, 255] {
            assert!(TSEncoding::try_from(byte).is_err());
        }
    }

    #[test]
    fn encoding_round_trip() {
        let encodings = [
            TSEncoding::Plain,
            TSEncoding::Dictionary,
            TSEncoding::Rle,
            TSEncoding::Ts2Diff,
            TSEncoding::Gorilla,
            TSEncoding::Zigzag,
            TSEncoding::Sprintz,
        ];
        for enc in encodings {
            assert_eq!(TSEncoding::try_from(enc as u8).unwrap(), enc);
        }
    }

    #[test]
    fn compression_try_from_valid() {
        let valid = [0, 1, 2, 3, 7];
        for byte in valid {
            assert!(CompressionType::try_from(byte).is_ok());
        }
    }

    #[test]
    fn compression_try_from_invalid_gaps() {
        // Values 4, 5, 6 are gaps (used by other IoTDB compression types)
        for byte in [4, 5, 6, 8, 255] {
            assert!(CompressionType::try_from(byte).is_err());
        }
    }

    #[test]
    fn compression_round_trip() {
        let types = [
            CompressionType::Uncompressed,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lzo,
            CompressionType::Lz4,
        ];
        for ct in types {
            assert_eq!(CompressionType::try_from(ct as u8).unwrap(), ct);
        }
    }
}
