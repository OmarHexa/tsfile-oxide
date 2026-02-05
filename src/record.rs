// C++ TsRecord / DataPoint (record.h) provides a single-row write
// container — one timestamp plus a list of measurement-to-value pairs.
// C++ uses void* with manual type dispatch and union-based DataPoint;
// in Rust we use the TsValue enum for type safety at the API boundary.
//
// C++ DataPoint has `bool isnull` + a union where the value is undefined
// when isnull=true. In Rust we use Option<TsValue>: None = null, which
// makes it impossible to accidentally read a garbage value from a null
// point — the compiler enforces the check via pattern matching.
//
// TsRecord is used by TsFileWriter::write_record() for row-at-a-time
// writes. For bulk writes (typical hot path), Tablet is more efficient
// since it amortizes the per-row overhead across many rows. TsRecord is
// the simple "quick start" API.

use crate::error::{Result, TsFileError};
use crate::types::TSDataType;
use crate::value::TsValue;

/// A single typed data point: measurement name + optional value.
///
/// C++ DataPoint holds `bool isnull`, a measurement name, and a union
/// value where the union contents are undefined when isnull=true. In Rust
/// we replace this with Option<TsValue> — None represents null, and the
/// compiler prevents reading the value of a null point. This eliminates
/// the class of bugs where C++ code forgets to check isnull before
/// accessing u_.float_val_ etc.
///
/// Null data points are important for:
/// - Sparse data: not every measurement has a value at every timestamp
/// - Aligned timeseries: value columns can have nulls where data is missing
#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    pub measurement_name: String,
    /// None = null (C++ isnull=true), Some = has value (C++ isnull=false).
    pub value: Option<TsValue>,
}

impl DataPoint {
    pub fn new(measurement_name: String, value: TsValue) -> Self {
        Self {
            measurement_name,
            value: Some(value),
        }
    }

    /// Create a null data point — the measurement exists but has no value
    /// at this timestamp. This is the Rust equivalent of C++ `isnull = true`.
    pub fn null(name: impl Into<String>) -> Self {
        Self {
            measurement_name: name.into(),
            value: None,
        }
    }

    /// Returns true if this data point is null (no value).
    pub fn is_null(&self) -> bool {
        self.value.is_none()
    }

    pub fn boolean(name: impl Into<String>, value: bool) -> Self {
        Self::new(name.into(), TsValue::Boolean(value))
    }

    pub fn int32(name: impl Into<String>, value: i32) -> Self {
        Self::new(name.into(), TsValue::Int32(value))
    }

    pub fn int64(name: impl Into<String>, value: i64) -> Self {
        Self::new(name.into(), TsValue::Int64(value))
    }

    pub fn float(name: impl Into<String>, value: f32) -> Self {
        Self::new(name.into(), TsValue::Float(value))
    }

    pub fn double(name: impl Into<String>, value: f64) -> Self {
        Self::new(name.into(), TsValue::Double(value))
    }

    pub fn text(name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::new(name.into(), TsValue::Text(value.into()))
    }

    /// Returns the data type of this data point's value, or None if null.
    /// For null points the type is not stored in the DataPoint itself —
    /// it must be looked up from the measurement schema.
    pub fn data_type(&self) -> Option<TSDataType> {
        self.value.as_ref().map(|v| v.data_type())
    }
}

/// A single-row record: device name + timestamp + list of data points.
///
/// C++ TsRecord stores a device_id string, timestamp, and vector of
/// DataPoint. This is the row-at-a-time write API used by
/// TsFileWriter::write_record(). For batch writes, Tablet is the
/// preferred container.
///
/// Usage:
/// ```
/// use tsfile_oxide::record::TsRecord;
///
/// let mut record = TsRecord::new("root.sg1.d1", 1000);
/// record.add_f32("temperature", 25.5);
/// record.add_i32("humidity", 80);
/// record.add_null("pressure"); // null: measurement exists but no value
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TsRecord {
    pub device_id: String,
    pub timestamp: i64,
    pub data_points: Vec<DataPoint>,
}

impl TsRecord {
    pub fn new(device_id: impl Into<String>, timestamp: i64) -> Self {
        Self {
            device_id: device_id.into(),
            timestamp,
            data_points: Vec::new(),
        }
    }

    /// Add a pre-constructed data point to this record.
    pub fn add_point(&mut self, point: DataPoint) {
        self.data_points.push(point);
    }

    /// Add a null data point — the measurement exists in the schema but
    /// has no value at this timestamp.
    pub fn add_null(&mut self, name: impl Into<String>) {
        self.add_point(DataPoint::null(name));
    }

    pub fn add_bool(&mut self, name: impl Into<String>, value: bool) {
        self.add_point(DataPoint::boolean(name, value));
    }

    pub fn add_i32(&mut self, name: impl Into<String>, value: i32) {
        self.add_point(DataPoint::int32(name, value));
    }

    pub fn add_i64(&mut self, name: impl Into<String>, value: i64) {
        self.add_point(DataPoint::int64(name, value));
    }

    pub fn add_f32(&mut self, name: impl Into<String>, value: f32) {
        self.add_point(DataPoint::float(name, value));
    }

    pub fn add_f64(&mut self, name: impl Into<String>, value: f64) {
        self.add_point(DataPoint::double(name, value));
    }

    pub fn add_text(&mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) {
        self.add_point(DataPoint::text(name, value));
    }

    /// Find a data point by measurement name.
    pub fn find_point(&self, name: &str) -> Option<&DataPoint> {
        self.data_points
            .iter()
            .find(|dp| dp.measurement_name == name)
    }

    /// Returns the number of data points in this record (including nulls).
    pub fn point_count(&self) -> usize {
        self.data_points.len()
    }

    /// Validate that all non-null data points match the expected types.
    ///
    /// This is called by TsFileWriter before writing to catch type
    /// mismatches early. Null data points are skipped — they have no
    /// value to type-check. Only validates points whose measurement
    /// names appear in the expected list.
    pub fn validate_types(&self, expected: &[(String, TSDataType)]) -> Result<()> {
        for dp in &self.data_points {
            // Null points have no type to validate
            let actual_type = match dp.data_type() {
                Some(dt) => dt,
                None => continue,
            };
            if let Some((_, expected_type)) =
                expected.iter().find(|(n, _)| n == &dp.measurement_name)
            {
                if actual_type != *expected_type {
                    return Err(TsFileError::TypeMismatch {
                        expected: *expected_type,
                        actual: actual_type,
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // DataPoint
    // -----------------------------------------------------------------------

    #[test]
    fn data_point_typed_constructors() {
        assert_eq!(DataPoint::boolean("b", true).data_type(), Some(TSDataType::Boolean));
        assert_eq!(DataPoint::int32("i", 42).data_type(), Some(TSDataType::Int32));
        assert_eq!(DataPoint::int64("l", 999).data_type(), Some(TSDataType::Int64));
        assert_eq!(DataPoint::float("f", 3.14).data_type(), Some(TSDataType::Float));
        assert_eq!(DataPoint::double("d", 2.718).data_type(), Some(TSDataType::Double));
        assert_eq!(DataPoint::text("t", b"hello".to_vec()).data_type(), Some(TSDataType::Text));
    }

    #[test]
    fn data_point_values() {
        let dp = DataPoint::float("temperature", 25.5);
        assert_eq!(dp.measurement_name, "temperature");
        assert_eq!(dp.value, Some(TsValue::Float(25.5)));
        assert!(!dp.is_null());
    }

    #[test]
    fn data_point_generic_constructor() {
        let dp = DataPoint::new(
            "status".to_string(),
            TsValue::Boolean(true),
        );
        assert_eq!(dp.measurement_name, "status");
        assert_eq!(dp.data_type(), Some(TSDataType::Boolean));
        assert!(!dp.is_null());
    }

    #[test]
    fn data_point_null() {
        let dp = DataPoint::null("temperature");
        assert_eq!(dp.measurement_name, "temperature");
        assert!(dp.is_null());
        assert_eq!(dp.value, None);
        assert_eq!(dp.data_type(), None);
    }

    #[test]
    fn data_point_clone_equality() {
        let dp1 = DataPoint::int32("col", 42);
        let dp2 = dp1.clone();
        assert_eq!(dp1, dp2);
    }

    #[test]
    fn data_point_null_equality() {
        let dp1 = DataPoint::null("col");
        let dp2 = DataPoint::null("col");
        assert_eq!(dp1, dp2);

        // Null != non-null
        let dp3 = DataPoint::int32("col", 0);
        assert_ne!(dp1, dp3);
    }

    // -----------------------------------------------------------------------
    // TsRecord
    // -----------------------------------------------------------------------

    #[test]
    fn ts_record_construction() {
        let record = TsRecord::new("root.sg1.d1", 1000);
        assert_eq!(record.device_id, "root.sg1.d1");
        assert_eq!(record.timestamp, 1000);
        assert_eq!(record.point_count(), 0);
    }

    #[test]
    fn ts_record_add_typed_values() {
        let mut record = TsRecord::new("root.sg1.d1", 1000);
        record.add_bool("status", true);
        record.add_i32("count", 42);
        record.add_i64("bigcount", 99999);
        record.add_f32("temperature", 25.5);
        record.add_f64("pressure", 1013.25);
        record.add_text("message", b"ok".to_vec());

        assert_eq!(record.point_count(), 6);
    }

    #[test]
    fn ts_record_add_null() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temperature", 25.5);
        record.add_null("humidity");
        record.add_i32("count", 10);

        assert_eq!(record.point_count(), 3);

        let null_point = record.find_point("humidity").unwrap();
        assert!(null_point.is_null());

        let value_point = record.find_point("temperature").unwrap();
        assert!(!value_point.is_null());
    }

    #[test]
    fn ts_record_find_point() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temperature", 25.5);
        record.add_i32("humidity", 80);

        let found = record.find_point("temperature").unwrap();
        assert_eq!(found.value, Some(TsValue::Float(25.5)));

        let found = record.find_point("humidity").unwrap();
        assert_eq!(found.value, Some(TsValue::Int32(80)));

        assert!(record.find_point("nonexistent").is_none());
    }

    #[test]
    fn ts_record_add_point_directly() {
        let mut record = TsRecord::new("root.d1", 500);
        record.add_point(DataPoint::float("temp", 20.0));
        assert_eq!(record.point_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Type validation
    // -----------------------------------------------------------------------

    #[test]
    fn validate_types_success() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temperature", 25.5);
        record.add_i32("humidity", 80);

        let expected = vec![
            ("temperature".to_string(), TSDataType::Float),
            ("humidity".to_string(), TSDataType::Int32),
        ];

        assert!(record.validate_types(&expected).is_ok());
    }

    #[test]
    fn validate_types_mismatch() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temperature", 25.5);

        let expected = vec![
            ("temperature".to_string(), TSDataType::Int32), // wrong type
        ];

        let err = record.validate_types(&expected).unwrap_err();
        match err {
            TsFileError::TypeMismatch { expected, actual } => {
                assert_eq!(expected, TSDataType::Int32);
                assert_eq!(actual, TSDataType::Float);
            }
            _ => panic!("expected TypeMismatch error"),
        }
    }

    #[test]
    fn validate_types_null_points_skipped() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_null("temperature"); // null — no type to validate

        let expected = vec![
            ("temperature".to_string(), TSDataType::Int32),
        ];

        // Null point should not trigger a type mismatch
        assert!(record.validate_types(&expected).is_ok());
    }

    #[test]
    fn validate_types_extra_point_ignored() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temperature", 25.5);
        record.add_i32("extra_col", 99); // not in expected list

        let expected = vec![
            ("temperature".to_string(), TSDataType::Float),
        ];

        // Extra point not in expected list is ignored
        assert!(record.validate_types(&expected).is_ok());
    }

    #[test]
    fn validate_types_empty_record() {
        let record = TsRecord::new("root.d1", 1000);
        let expected = vec![
            ("temperature".to_string(), TSDataType::Float),
        ];
        assert!(record.validate_types(&expected).is_ok());
    }

    #[test]
    fn validate_types_empty_expected() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temperature", 25.5);
        assert!(record.validate_types(&[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // Clone and equality
    // -----------------------------------------------------------------------

    #[test]
    fn ts_record_clone_equality() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temp", 25.5);
        record.add_i32("hum", 80);

        let cloned = record.clone();
        assert_eq!(record, cloned);
    }

    #[test]
    fn ts_record_inequality() {
        let mut r1 = TsRecord::new("root.d1", 1000);
        r1.add_i32("col", 1);

        let mut r2 = TsRecord::new("root.d1", 1000);
        r2.add_i32("col", 2);

        assert_ne!(r1, r2);
    }

    #[test]
    fn ts_record_mixed_null_and_values() {
        let mut record = TsRecord::new("root.d1", 1000);
        record.add_f32("temperature", 25.5);
        record.add_null("humidity");
        record.add_null("pressure");
        record.add_i32("count", 10);

        assert_eq!(record.point_count(), 4);

        // Verify nulls
        assert!(record.find_point("humidity").unwrap().is_null());
        assert!(record.find_point("pressure").unwrap().is_null());
        assert!(!record.find_point("temperature").unwrap().is_null());
        assert!(!record.find_point("count").unwrap().is_null());
    }
}
