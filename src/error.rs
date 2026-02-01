// C++ uses 54 integer error codes (errno_define.h) propagated via
// `int ret = E_OK; if (RET_FAIL(op())) return ret;`. In Rust we model
// errors as an enum and propagate with the `?` operator, giving us
// exhaustive matching and zero-cost propagation without manual checks.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TsFileError {
    // This is a macro that generates a Display implementation for TsFileError
    #[error("I/O error: {0}")]
    // This is a macro that generates a From implementation for TsFileError
    Io(#[from] std::io::Error),

    #[error("out of memory")]
    OutOfMemory,

    #[error("corrupted tsfile: {0}")]
    Corrupted(String),

    #[error("type mismatch: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        expected: crate::types::TSDataType,
        actual: crate::types::TSDataType,
    },

    #[error("out of order timestamp: {ts} <= {last}")]
    OutOfOrder { ts: i64, last: i64 },

    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArg(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, TsFileError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts_via_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        // From trait on T allows automatic Into<T> conversion
        let ts_err: TsFileError = io_err.into();
        assert!(matches!(ts_err, TsFileError::Io(_)));
        assert!(ts_err.to_string().contains("file missing"));
    }

    #[test]
    fn result_alias_works_with_question_mark() {
        fn inner() -> Result<i32> {
            let mut reader: &[u8] = &[];
            // Reading from empty slice produces io::Error, auto-converted via ?
            let mut buf = [0u8; 1];
            std::io::Read::read_exact(&mut reader, &mut buf)?;
            Ok(42)
        }
        assert!(inner().is_err());
    }

    #[test]
    fn display_messages_are_readable() {
        let err = TsFileError::OutOfMemory;
        assert_eq!(err.to_string(), "out of memory");

        let err = TsFileError::Corrupted("bad magic".into());
        assert_eq!(err.to_string(), "corrupted tsfile: bad magic");

        let err = TsFileError::OutOfOrder { ts: 5, last: 10 };
        assert_eq!(err.to_string(), "out of order timestamp: 5 <= 10");

        let err = TsFileError::TypeMismatch {
            expected: crate::types::TSDataType::Int32,
            actual: crate::types::TSDataType::Float,
        };
        assert!(err.to_string().contains("Int32"));
        assert!(err.to_string().contains("Float"));
    }

    #[test]
    fn error_variants_are_distinct() {
        // Ensure different variants don't accidentally match
        let e1 = TsFileError::Encoding("enc".into());
        let e2 = TsFileError::Compression("comp".into());
        assert_ne!(e1.to_string(), e2.to_string());
    }
}
