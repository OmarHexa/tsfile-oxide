// Tablet is the bulk-write API for TsFile — a columnar batch of rows for one
// device / table. C++ Tablet uses a void* value_matrix with manual union-based
// type dispatch and a bool** bitMaps for null tracking. In Rust, ColumnData
// holds a typed Vec per column, eliminating unsafe casts while keeping the
// same columnar layout.

use crate::bitmap::BitMap;
use crate::error::{Result, TsFileError};
use crate::schema::MeasurementSchema;
use crate::types::TSDataType;

// ---------------------------------------------------------------------------
// ColumnData
// ---------------------------------------------------------------------------

/// Typed column storage for one measurement in a Tablet.
///
/// C++ stores values as a `void*` matrix with union-based type puns; callers
/// must cast manually and the type is tracked in the schema. In Rust the enum
/// makes the type explicit and pattern-matching replaces unsafe casts.
#[derive(Debug, Clone)]
pub enum ColumnData {
    Boolean(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    Text(Vec<Vec<u8>>),
}

impl ColumnData {
    /// Create an empty column of the given data type with `capacity` slots.
    pub fn new(data_type: TSDataType, capacity: usize) -> Self {
        match data_type {
            TSDataType::Boolean => ColumnData::Boolean(Vec::with_capacity(capacity)),
            TSDataType::Int32 => ColumnData::Int32(Vec::with_capacity(capacity)),
            TSDataType::Int64 => ColumnData::Int64(Vec::with_capacity(capacity)),
            TSDataType::Float => ColumnData::Float(Vec::with_capacity(capacity)),
            TSDataType::Double => ColumnData::Double(Vec::with_capacity(capacity)),
            TSDataType::Text | TSDataType::String => {
                ColumnData::Text(Vec::with_capacity(capacity))
            }
        }
    }

    /// Number of values stored in this column.
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Boolean(v) => v.len(),
            ColumnData::Int32(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float(v) => v.len(),
            ColumnData::Double(v) => v.len(),
            ColumnData::Text(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> TSDataType {
        match self {
            ColumnData::Boolean(_) => TSDataType::Boolean,
            ColumnData::Int32(_) => TSDataType::Int32,
            ColumnData::Int64(_) => TSDataType::Int64,
            ColumnData::Float(_) => TSDataType::Float,
            ColumnData::Double(_) => TSDataType::Double,
            ColumnData::Text(_) => TSDataType::Text,
        }
    }
}

// ---------------------------------------------------------------------------
// Tablet
// ---------------------------------------------------------------------------

/// Columnar batch write buffer for one device or table.
///
/// Rows share a single `timestamps` vector; each column maps to one measurement
/// schema. A `bitmaps` entry per column tracks which rows have null values
/// (set bit = null). For non-aligned writes, null rows are simply skipped.
///
/// Usage:
/// ```rust,ignore
/// let mut tablet = Tablet::new("root.sg1.d1", schemas, 1000);
/// tablet.add_timestamp(0, 1_000_000)?;
/// tablet.add_value_i32(0, 0, 42)?;    // (row=0, col=0, value=42)
/// tablet.add_timestamp(1, 2_000_000)?;
/// tablet.add_value_f32(1, 1, 3.14)?;  // (row=1, col=1, value=3.14)
/// writer.write_tablet(&device_id, &tablet)?;
/// ```
pub struct Tablet {
    /// Device path (tree model) or table name (table model).
    pub device_name: String,
    /// One schema per column, in column order.
    pub schemas: Vec<MeasurementSchema>,
    /// Timestamps for each row in insertion order.
    pub timestamps: Vec<i64>,
    /// Columnar value data, one `ColumnData` per schema entry.
    pub columns: Vec<ColumnData>,
    /// Null bitmaps: `bitmaps[c].get(r)` == true means row r of column c is null.
    pub bitmaps: Vec<BitMap>,
    /// Number of rows currently in the tablet.
    pub row_count: usize,
}

impl Tablet {
    /// Create a Tablet for `device_name` with the given schemas and a
    /// pre-allocated `capacity` for all internal vectors.
    pub fn new(device_name: impl Into<String>, schemas: Vec<MeasurementSchema>, capacity: usize) -> Self {
        let columns: Vec<ColumnData> = schemas
            .iter()
            .map(|s| ColumnData::new(s.data_type, capacity))
            .collect();
        let bitmaps = vec![BitMap::new(capacity); schemas.len()];
        Self {
            device_name: device_name.into(),
            schemas,
            timestamps: Vec::with_capacity(capacity),
            columns,
            bitmaps,
            row_count: 0,
        }
    }

    /// Append a timestamp for a new row, returning the row index.
    ///
    /// Each call to `add_timestamp` creates a new row. Column values for that
    /// row must be provided separately via `add_value_*` before the next
    /// `add_timestamp`, or left as null (the default).
    pub fn add_timestamp(&mut self, row: usize, timestamp: i64) -> Result<()> {
        if row != self.row_count {
            return Err(TsFileError::InvalidArg(format!(
                "add_timestamp: expected row {}, got {row}",
                self.row_count
            )));
        }
        self.timestamps.push(timestamp);
        self.row_count += 1;
        Ok(())
    }

    /// Mark a cell (row, col) as null.
    pub fn mark_null(&mut self, row: usize, col: usize) -> Result<()> {
        self.check_bounds(row, col)?;
        self.bitmaps[col].set(row);
        Ok(())
    }

    /// Write a boolean value at (row, col).
    pub fn add_value_bool(&mut self, row: usize, col: usize, value: bool) -> Result<()> {
        self.check_bounds(row, col)?;
        if let ColumnData::Boolean(v) = &mut self.columns[col] {
            // Extend if needed (caller may add in any order before finalizing row)
            if v.len() <= row {
                v.resize(row + 1, false);
            }
            v[row] = value;
        } else {
            return Err(TsFileError::TypeMismatch {
                expected: TSDataType::Boolean,
                actual: self.columns[col].data_type(),
            });
        }
        Ok(())
    }

    /// Write an i32 value at (row, col).
    pub fn add_value_i32(&mut self, row: usize, col: usize, value: i32) -> Result<()> {
        self.check_bounds(row, col)?;
        if let ColumnData::Int32(v) = &mut self.columns[col] {
            if v.len() <= row {
                v.resize(row + 1, 0);
            }
            v[row] = value;
        } else {
            return Err(TsFileError::TypeMismatch {
                expected: TSDataType::Int32,
                actual: self.columns[col].data_type(),
            });
        }
        Ok(())
    }

    /// Write an i64 value at (row, col).
    pub fn add_value_i64(&mut self, row: usize, col: usize, value: i64) -> Result<()> {
        self.check_bounds(row, col)?;
        if let ColumnData::Int64(v) = &mut self.columns[col] {
            if v.len() <= row {
                v.resize(row + 1, 0);
            }
            v[row] = value;
        } else {
            return Err(TsFileError::TypeMismatch {
                expected: TSDataType::Int64,
                actual: self.columns[col].data_type(),
            });
        }
        Ok(())
    }

    /// Write an f32 value at (row, col).
    pub fn add_value_f32(&mut self, row: usize, col: usize, value: f32) -> Result<()> {
        self.check_bounds(row, col)?;
        if let ColumnData::Float(v) = &mut self.columns[col] {
            if v.len() <= row {
                v.resize(row + 1, 0.0);
            }
            v[row] = value;
        } else {
            return Err(TsFileError::TypeMismatch {
                expected: TSDataType::Float,
                actual: self.columns[col].data_type(),
            });
        }
        Ok(())
    }

    /// Write an f64 value at (row, col).
    pub fn add_value_f64(&mut self, row: usize, col: usize, value: f64) -> Result<()> {
        self.check_bounds(row, col)?;
        if let ColumnData::Double(v) = &mut self.columns[col] {
            if v.len() <= row {
                v.resize(row + 1, 0.0);
            }
            v[row] = value;
        } else {
            return Err(TsFileError::TypeMismatch {
                expected: TSDataType::Double,
                actual: self.columns[col].data_type(),
            });
        }
        Ok(())
    }

    /// Write a text (byte slice) value at (row, col).
    pub fn add_value_text(&mut self, row: usize, col: usize, value: Vec<u8>) -> Result<()> {
        self.check_bounds(row, col)?;
        if let ColumnData::Text(v) = &mut self.columns[col] {
            if v.len() <= row {
                v.resize(row + 1, Vec::new());
            }
            v[row] = value;
        } else {
            return Err(TsFileError::TypeMismatch {
                expected: TSDataType::Text,
                actual: self.columns[col].data_type(),
            });
        }
        Ok(())
    }

    /// Returns true if cell (row, col) is null.
    pub fn is_null(&self, row: usize, col: usize) -> bool {
        self.bitmaps[col].get(row)
    }

    /// Clear all rows, resetting the tablet for reuse.
    pub fn reset(&mut self) {
        self.timestamps.clear();
        self.row_count = 0;
        for col in &mut self.columns {
            match col {
                ColumnData::Boolean(v) => v.clear(),
                ColumnData::Int32(v) => v.clear(),
                ColumnData::Int64(v) => v.clear(),
                ColumnData::Float(v) => v.clear(),
                ColumnData::Double(v) => v.clear(),
                ColumnData::Text(v) => v.clear(),
            }
        }
        for bm in &mut self.bitmaps {
            bm.clear_all();
        }
    }

    fn check_bounds(&self, row: usize, col: usize) -> Result<()> {
        if col >= self.schemas.len() {
            return Err(TsFileError::InvalidArg(format!(
                "column index {col} out of range (tablet has {} columns)",
                self.schemas.len()
            )));
        }
        if row >= self.row_count {
            return Err(TsFileError::InvalidArg(format!(
                "row {row} out of range (tablet has {} rows)",
                self.row_count
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::MeasurementSchema;
    use crate::types::{CompressionType, TSEncoding};

    fn make_schemas() -> Vec<MeasurementSchema> {
        vec![
            MeasurementSchema::new(
                "temperature".into(),
                TSDataType::Float,
                TSEncoding::Gorilla,
                CompressionType::Lz4,
            ),
            MeasurementSchema::new(
                "humidity".into(),
                TSDataType::Int32,
                TSEncoding::Ts2Diff,
                CompressionType::Lz4,
            ),
        ]
    }

    #[test]
    fn create_and_add_rows() {
        let mut tablet = Tablet::new("root.sg1.d1", make_schemas(), 10);
        tablet.add_timestamp(0, 1_000_000).unwrap();
        tablet.add_value_f32(0, 0, 25.5).unwrap();
        tablet.add_value_i32(0, 1, 60).unwrap();

        tablet.add_timestamp(1, 2_000_000).unwrap();
        tablet.add_value_f32(1, 0, 26.0).unwrap();
        tablet.add_value_i32(1, 1, 65).unwrap();

        assert_eq!(tablet.row_count, 2);
        assert_eq!(tablet.timestamps[0], 1_000_000);
        assert_eq!(tablet.timestamps[1], 2_000_000);
    }

    #[test]
    fn type_mismatch_errors() {
        let mut tablet = Tablet::new("d1", make_schemas(), 5);
        tablet.add_timestamp(0, 1000).unwrap();
        // column 0 is Float, writing i32 should fail
        assert!(tablet.add_value_i32(0, 0, 42).is_err());
    }

    #[test]
    fn null_tracking() {
        let mut tablet = Tablet::new("d1", make_schemas(), 5);
        tablet.add_timestamp(0, 1000).unwrap();
        tablet.mark_null(0, 0).unwrap();
        assert!(tablet.is_null(0, 0));
        assert!(!tablet.is_null(0, 1));
    }

    #[test]
    fn reset_clears_rows() {
        let mut tablet = Tablet::new("d1", make_schemas(), 5);
        tablet.add_timestamp(0, 1000).unwrap();
        tablet.add_value_f32(0, 0, 1.0).unwrap();
        tablet.reset();
        assert_eq!(tablet.row_count, 0);
        assert!(tablet.timestamps.is_empty());
    }

    #[test]
    fn out_of_order_timestamp_index_errors() {
        let mut tablet = Tablet::new("d1", make_schemas(), 5);
        tablet.add_timestamp(0, 1000).unwrap();
        // must provide row 1 next, not row 0 again
        assert!(tablet.add_timestamp(0, 2000).is_err());
    }

    #[test]
    fn column_data_new() {
        for dt in [
            TSDataType::Boolean,
            TSDataType::Int32,
            TSDataType::Int64,
            TSDataType::Float,
            TSDataType::Double,
            TSDataType::Text,
        ] {
            let col = ColumnData::new(dt, 16);
            assert_eq!(col.data_type(), dt);
            assert_eq!(col.len(), 0);
        }
    }
}
