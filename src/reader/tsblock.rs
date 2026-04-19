// C++ TsBlock (common/row_record.h neighbours) is a columnar batch: one
// i64 time vector plus N typed value columns, optionally with null bitmaps.
// Rust mirrors the shape with an enum over the 7 supported data types.
// nulls are `None` for non-aligned chunks (writer never emits nulls there)
// and `Some` for aligned value chunks where `ValuePageWriter` already
// tracks a bitmap on the write side.

use crate::bitmap::BitMap;
use crate::types::TSDataType;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TsBlock {
    pub times: Vec<i64>,
    pub columns: Vec<Column>,
    pub column_meta: Arc<[ColumnMeta]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: TSDataType,
}

#[derive(Debug, Clone)]
pub enum Column {
    Boolean { values: Vec<bool>,    nulls: Option<BitMap> },
    Int32   { values: Vec<i32>,     nulls: Option<BitMap> },
    Int64   { values: Vec<i64>,     nulls: Option<BitMap> },
    Float   { values: Vec<f32>,     nulls: Option<BitMap> },
    Double  { values: Vec<f64>,     nulls: Option<BitMap> },
    Text    { values: Vec<Vec<u8>>, nulls: Option<BitMap> },
    String  { values: Vec<String>,  nulls: Option<BitMap> },
}

impl Column {
    /// Number of logical rows in this column (equal to `values.len()`).
    pub fn len(&self) -> usize {
        match self {
            Column::Boolean { values, .. } => values.len(),
            Column::Int32   { values, .. } => values.len(),
            Column::Int64   { values, .. } => values.len(),
            Column::Float   { values, .. } => values.len(),
            Column::Double  { values, .. } => values.len(),
            Column::Text    { values, .. } => values.len(),
            Column::String  { values, .. } => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Returns true if the slot at `row` is null (aligned chunks only).
    ///
    /// # Panics
    /// Panics if `row >= self.len()`, consistent with `BitMap::get`.
    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Column::Boolean { nulls, .. }
            | Column::Int32   { nulls, .. }
            | Column::Int64   { nulls, .. }
            | Column::Float   { nulls, .. }
            | Column::Double  { nulls, .. }
            | Column::Text    { nulls, .. }
            | Column::String  { nulls, .. } => nulls.as_ref().is_some_and(|b| b.get(row)),
        }
    }

    pub fn data_type(&self) -> TSDataType {
        match self {
            Column::Boolean { .. } => TSDataType::Boolean,
            Column::Int32   { .. } => TSDataType::Int32,
            Column::Int64   { .. } => TSDataType::Int64,
            Column::Float   { .. } => TSDataType::Float,
            Column::Double  { .. } => TSDataType::Double,
            Column::Text    { .. } => TSDataType::Text,
            Column::String  { .. } => TSDataType::String,
        }
    }
}

impl TsBlock {
    pub fn new(times: Vec<i64>, columns: Vec<Column>, column_meta: Arc<[ColumnMeta]>) -> Self {
        debug_assert_eq!(columns.len(), column_meta.len(), "columns and column_meta length mismatch");
        for c in &columns {
            debug_assert_eq!(c.len(), times.len(), "column length != times length");
        }
        Self { times, columns, column_meta }
    }

    pub fn num_rows(&self) -> usize { self.times.len() }
    pub fn num_columns(&self) -> usize { self.columns.len() }
    pub fn is_empty(&self) -> bool { self.times.is_empty() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(name: &str, dt: TSDataType) -> ColumnMeta {
        ColumnMeta { name: name.into(), data_type: dt }
    }

    #[test]
    fn block_construction_validates_lengths_debug() {
        let cols = vec![Column::Int64 { values: vec![1, 2, 3], nulls: None }];
        let cm: Arc<[ColumnMeta]> = Arc::from(vec![meta("t", TSDataType::Int64)]);
        let b = TsBlock::new(vec![10, 20, 30], cols, cm);
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.num_columns(), 1);
    }

    #[test]
    fn column_null_bit_reports_true() {
        let mut nulls = BitMap::new(4);
        nulls.set(2);
        let c = Column::Int32 { values: vec![0, 1, 0, 3], nulls: Some(nulls) };
        assert!(!c.is_null(0));
        assert!(!c.is_null(1));
        assert!(c.is_null(2));
        assert!(!c.is_null(3));
    }

    #[test]
    fn column_without_null_bitmap_never_null() {
        let c = Column::Double { values: vec![1.0, 2.0], nulls: None };
        assert!(!c.is_null(0));
        assert!(!c.is_null(1));
    }

    #[test]
    fn column_len_and_data_type_match_variant() {
        let c = Column::Float { values: vec![0.0; 7], nulls: None };
        assert_eq!(c.len(), 7);
        assert_eq!(c.data_type(), TSDataType::Float);
    }

    #[test]
    fn empty_block_reports_zero() {
        let cm: Arc<[ColumnMeta]> = Arc::from(Vec::<ColumnMeta>::new());
        let b = TsBlock::new(Vec::new(), Vec::new(), cm);
        assert!(b.is_empty());
        assert_eq!(b.num_rows(), 0);
    }
}
