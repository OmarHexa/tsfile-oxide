// C++ uses void* with manual type tags and union-based dispatch for
// dynamic values. In Rust we use an enum — the compiler enforces that
// every match arm handles all variants, preventing the class of bugs
// where a type tag is checked incorrectly or forgotten.
//
// TsValue is used at API boundaries where the type isn't known statically
// (e.g., TsRecord data points, filter evaluation). On hot paths (encoders,
// page writers), we use statically-typed methods instead to avoid match
// overhead.

use crate::types::TSDataType;

/// A dynamically-typed time-series value.
///
/// Used at API boundaries (TsRecord, filter evaluation) where the data type
/// isn't known at compile time. For bulk operations the writer/reader use
/// statically-typed methods instead.
#[derive(Debug, Clone, PartialEq)]
pub enum TsValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    /// Text/String/Blob data stored as raw bytes.
    Text(Vec<u8>),
    String(String),
}

impl TsValue {
    /// Returns the TSDataType that corresponds to this value's variant.
    pub fn data_type(&self) -> TSDataType {
        match self {
            TsValue::Boolean(_) => TSDataType::Boolean,
            TsValue::Int32(_) => TSDataType::Int32,
            TsValue::Int64(_) => TSDataType::Int64,
            TsValue::Float(_) => TSDataType::Float,
            TsValue::Double(_) => TSDataType::Double,
            TsValue::Text(_) => TSDataType::Text,
            TsValue::String(_) => TSDataType::String,
        }
    }

    /// Attempt to extract a bool. Returns None if variant doesn't match.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            TsValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    /// Attempt to extract an i32. Returns None if variant doesn't match.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            TsValue::Int32(v) => Some(*v),
            _ => None,
        }
    }

    /// Attempt to extract an i64. Returns None if variant doesn't match.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            TsValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Attempt to extract an f32. Returns None if variant doesn't match.
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            TsValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    /// Attempt to extract an f64. Returns None if variant doesn't match.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            TsValue::Double(v) => Some(*v),
            _ => None,
        }
    }

    /// Attempt to extract text bytes. Returns None if variant doesn't match.
    pub fn as_text(&self) -> Option<&[u8]> {
        match self {
            TsValue::Text(v) => Some(v),
            _ => None,
        }
    }

    /// Attempt to extract a string. Returns None if variant doesn't match.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            TsValue::String(v) => Some(v),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_matches_variant() {
        assert_eq!(TsValue::Boolean(true).data_type(), TSDataType::Boolean);
        assert_eq!(TsValue::Int32(0).data_type(), TSDataType::Int32);
        assert_eq!(TsValue::Int64(0).data_type(), TSDataType::Int64);
        assert_eq!(TsValue::Float(0.0).data_type(), TSDataType::Float);
        assert_eq!(TsValue::Double(0.0).data_type(), TSDataType::Double);
        assert_eq!(TsValue::Text(vec![]).data_type(), TSDataType::Text);
        assert_eq!(
            TsValue::String("".to_string()).data_type(),
            TSDataType::String
        );
    }

    #[test]
    fn as_accessors_return_some_for_matching_type() {
        assert_eq!(TsValue::Boolean(true).as_bool(), Some(true));
        assert_eq!(TsValue::Int32(42).as_i32(), Some(42));
        assert_eq!(TsValue::Int64(-1).as_i64(), Some(-1));
        assert_eq!(TsValue::Float(3.14).as_f32(), Some(3.14));
        assert_eq!(TsValue::Double(2.718).as_f64(), Some(2.718));
        assert_eq!(
            TsValue::Text(b"hello".to_vec()).as_text(),
            Some(b"hello".as_slice())
        );
        assert_eq!(
            TsValue::String("hello".to_string()).as_string(),
            Some("hello")
        );
    }

    #[test]
    fn as_accessors_return_none_for_wrong_type() {
        let val = TsValue::Int32(42);
        assert_eq!(val.as_bool(), None);
        assert_eq!(val.as_i64(), None);
        assert_eq!(val.as_f32(), None);
        assert_eq!(val.as_f64(), None);
        assert_eq!(val.as_text(), None);
    }

    #[test]
    fn equality() {
        assert_eq!(TsValue::Int32(1), TsValue::Int32(1));
        assert_ne!(TsValue::Int32(1), TsValue::Int32(2));
        // Different variants with same inner value are not equal
        assert_ne!(TsValue::Int32(1), TsValue::Int64(1));
    }

    #[test]
    fn clone_preserves_value() {
        let original = TsValue::Text(b"data".to_vec());
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }
}
