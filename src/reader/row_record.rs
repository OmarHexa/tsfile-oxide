// C++ RowRecord is a (timestamp, Vec<Field>) pair where each Field is a
// typed tagged union. In Rust we already have TsValue for dynamic typing;
// Option<TsValue> captures the per-column null slot used by aligned chunks.

use crate::types::TSDataType;
use crate::value::TsValue;

#[derive(Debug, Clone, PartialEq)]
pub struct RowRecord {
    pub timestamp: i64,
    pub values: Vec<Option<TsValue>>,
}

impl RowRecord {
    pub fn new(timestamp: i64, values: Vec<Option<TsValue>>) -> Self {
        Self { timestamp, values }
    }

    /// Returns the number of value slots (including nulls).
    pub fn num_columns(&self) -> usize { self.values.len() }

    /// Returns true if the slot at `col` is null / missing. Out-of-range
    /// indices are treated as null rather than panicking, matching the
    /// C++ RowRecord behaviour where querying a missing column returns
    /// an absent/unknown value.
    pub fn is_null(&self, col: usize) -> bool {
        self.values.get(col).is_none_or(|v| v.is_none())
    }

    /// Returns the data type of the slot at `col`, or `None` if the slot
    /// is null or the index is out of range.
    pub fn data_type(&self, col: usize) -> Option<TSDataType> {
        self.values.get(col)?.as_ref().map(|v| v.data_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_sets_fields() {
        let r = RowRecord::new(42, vec![Some(TsValue::Int64(7))]);
        assert_eq!(r.timestamp, 42);
        assert_eq!(r.num_columns(), 1);
    }

    #[test]
    fn is_null_detects_missing_slots() {
        let r = RowRecord::new(0, vec![Some(TsValue::Int32(1)), None]);
        assert!(!r.is_null(0));
        assert!(r.is_null(1));
        assert!(r.is_null(99)); // out of range = null
    }

    #[test]
    fn data_type_returns_some_for_present_slot() {
        let r = RowRecord::new(0, vec![Some(TsValue::Double(1.0)), None]);
        assert_eq!(r.data_type(0), Some(TSDataType::Double));
        assert_eq!(r.data_type(1), None);
    }
}
