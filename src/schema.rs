// C++ schema types (MeasurementSchema, MeasurementSchemaGroup, TableSchema)
// use raw pointers and manual lifecycle. In Rust, owned values and Vec
// handle memory automatically. ColumnCategory distinguishes TAG (identity)
// from FIELD (measurement) columns in the table data model.
//
// C++ MeasurementSchemaGroup groups measurements per device and tracks
// alignment (shared time column vs independent). In Rust we use BTreeMap
// to keep measurements sorted by name — this matches the on-disk ordering
// requirement without a separate sort step.
//
// C++ TableSchema uses a position index (HashMap<name, index>) for fast
// column lookup. We store an ordered Vec and do linear scan for lookups —
// column counts per table are small (typically < 100) so this is fine.

use crate::error::{Result, TsFileError};
use crate::serialize;
use crate::types::{CompressionType, TSDataType, TSEncoding};
use std::collections::BTreeMap;
use std::io::{Read, Write};

/// Distinguishes TAG columns (identity/key) from FIELD columns
/// (measurement/value) in the table data model.
///
/// C++ defines this in utils/db_utils.h. The discriminant values match the
/// on-disk format used in table schema serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ColumnCategory {
    Tag = 0,
    Field = 1,
}

impl TryFrom<u8> for ColumnCategory {
    type Error = TsFileError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Tag),
            1 => Ok(Self::Field),
            _ => Err(TsFileError::InvalidArg(format!(
                "Unknown ColumnCategory discriminant: {value}"
            ))),
        }
    }
}

/// Schema for a single measurement (column) within a device.
///
/// C++ MeasurementSchema holds name + data type + encoding + compression,
/// allocated via raw pointer with manual lifecycle. In Rust it's a plain
/// owned struct — no allocation tracking or explicit delete needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasurementSchema {
    pub measurement_name: String,
    pub data_type: TSDataType,
    pub encoding: TSEncoding,
    pub compression: CompressionType,
}

impl MeasurementSchema {
    pub fn new(
        measurement_name: String,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> Self {
        Self {
            measurement_name,
            data_type,
            encoding,
            compression,
        }
    }

    /// Serialize: name (length-prefixed string) + data_type (u8) + encoding (u8) + compression (u8).
    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_string(w, &self.measurement_name)?;
        serialize::write_u8(w, self.data_type as u8)?;
        serialize::write_u8(w, self.encoding as u8)?;
        serialize::write_u8(w, self.compression as u8)?;
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let measurement_name = serialize::read_string(r)?;
        let data_type = TSDataType::try_from(serialize::read_u8(r)?)?;
        let encoding = TSEncoding::try_from(serialize::read_u8(r)?)?;
        let compression = CompressionType::try_from(serialize::read_u8(r)?)?;
        Ok(Self {
            measurement_name,
            data_type,
            encoding,
            compression,
        })
    }
}

/// Schema for a column in the table data model.
///
/// Extends the measurement concept with a ColumnCategory to distinguish
/// TAG (identity) columns from FIELD (measurement) columns. In the C++
/// implementation, ColumnSchema is a separate class from MeasurementSchema;
/// we keep that distinction here for API clarity.
/// Defined in utils/db_utils.h
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub column_name: String,
    pub data_type: TSDataType,
    pub category: ColumnCategory,
}

impl ColumnSchema {
    pub fn new(column_name: String, data_type: TSDataType, category: ColumnCategory) -> Self {
        Self {
            column_name,
            data_type,
            category,
        }
    }

    /// Serialize: name (string) + data_type (u8) + category (u8).
    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_string(w, &self.column_name)?;
        serialize::write_u8(w, self.data_type as u8)?;
        serialize::write_u8(w, self.category as u8)?;
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let column_name = serialize::read_string(r)?;
        let data_type = TSDataType::try_from(serialize::read_u8(r)?)?;
        let category = ColumnCategory::try_from(serialize::read_u8(r)?)?;
        Ok(Self {
            column_name,
            data_type,
            category,
        })
    }
}

/// Table-level schema: table name + ordered list of column schemas.
///
/// C++ TableSchema holds column schemas with a position index
/// (HashMap<name, index>) for fast name-to-position lookup. We store
/// the ordered Vec and scan linearly — column counts per table are
/// typically small (< 100), so the O(n) scan is negligible vs the
/// complexity of maintaining a separate index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub table_name: String,
    pub column_schemas: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(table_name: String, column_schemas: Vec<ColumnSchema>) -> Self {
        Self {
            table_name,
            column_schemas,
        }
    }

    /// Find a column schema by name. Returns (index, schema) if found.
    pub fn find_column(&self, name: &str) -> Option<(usize, &ColumnSchema)> {
        self.column_schemas
            .iter()
            .enumerate()
            .find(|(_, cs)| cs.column_name == name)
    }

    /// Returns references to all TAG columns.
    pub fn tag_columns(&self) -> Vec<&ColumnSchema> {
        self.column_schemas
            .iter()
            .filter(|cs| cs.category == ColumnCategory::Tag)
            .collect()
    }

    /// Returns references to all FIELD columns.
    pub fn field_columns(&self) -> Vec<&ColumnSchema> {
        self.column_schemas
            .iter()
            .filter(|cs| cs.category == ColumnCategory::Field)
            .collect()
    }

    /// Serialize: table_name (string) + column_count (i32) + column_schemas[].
    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_string(w, &self.table_name)?;
        serialize::write_i32(w, self.column_schemas.len() as i32)?;
        for cs in &self.column_schemas {
            cs.serialize_to(w)?;
        }
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let table_name = serialize::read_string(r)?;
        let count = serialize::read_i32(r)? as usize;
        let mut column_schemas = Vec::with_capacity(count);
        for _ in 0..count {
            column_schemas.push(ColumnSchema::deserialize_from(r)?);
        }
        Ok(Self {
            table_name,
            column_schemas,
        })
    }
}

/// Groups measurement schemas for a single device.
///
/// C++ MeasurementSchemaGroup tracks whether the device's measurements
/// are aligned (shared time column) or non-aligned (independent time
/// columns per measurement). In Rust we use BTreeMap to keep measurements
/// sorted by name, matching the on-disk ordering requirement for the
/// metadata index tree.
#[derive(Debug, Clone)]
pub struct MeasurementSchemaGroup {
    pub measurement_schemas: BTreeMap<String, MeasurementSchema>,
    pub is_aligned: bool,
}

impl MeasurementSchemaGroup {
    pub fn new(is_aligned: bool) -> Self {
        Self {
            measurement_schemas: BTreeMap::new(),
            is_aligned,
        }
    }

    /// Register a measurement schema. Replaces any existing schema with
    /// the same measurement name.
    pub fn add_measurement(&mut self, schema: MeasurementSchema) {
        self.measurement_schemas
            .insert(schema.measurement_name.clone(), schema);
    }

    /// Look up a measurement schema by name.
    pub fn get_measurement(&self, name: &str) -> Option<&MeasurementSchema> {
        self.measurement_schemas.get(name)
    }

    /// Returns the number of registered measurements.
    pub fn measurement_count(&self) -> usize {
        self.measurement_schemas.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // --- ColumnCategory ---

    #[test]
    fn column_category_discriminants() {
        assert_eq!(ColumnCategory::Tag as u8, 0);
        assert_eq!(ColumnCategory::Field as u8, 1);
    }

    #[test]
    fn column_category_try_from_valid() {
        assert_eq!(ColumnCategory::try_from(0).unwrap(), ColumnCategory::Tag);
        assert_eq!(ColumnCategory::try_from(1).unwrap(), ColumnCategory::Field);
    }

    #[test]
    fn column_category_try_from_invalid() {
        assert!(ColumnCategory::try_from(2).is_err());
        assert!(ColumnCategory::try_from(255).is_err());
    }

    // --- MeasurementSchema ---

    #[test]
    fn measurement_schema_construction() {
        let ms = MeasurementSchema::new(
            "temperature".to_string(),
            TSDataType::Float,
            TSEncoding::Gorilla,
            CompressionType::Snappy,
        );
        assert_eq!(ms.measurement_name, "temperature");
        assert_eq!(ms.data_type, TSDataType::Float);
        assert_eq!(ms.encoding, TSEncoding::Gorilla);
        assert_eq!(ms.compression, CompressionType::Snappy);
    }

    #[test]
    fn measurement_schema_serialize_round_trip() {
        let original = MeasurementSchema::new(
            "temperature".to_string(),
            TSDataType::Float,
            TSEncoding::Gorilla,
            CompressionType::Lz4,
        );

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = MeasurementSchema::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn measurement_schema_serialize_all_types() {
        let cases = [
            (
                TSDataType::Boolean,
                TSEncoding::Plain,
                CompressionType::Uncompressed,
            ),
            (TSDataType::Int32, TSEncoding::Rle, CompressionType::Snappy),
            (
                TSDataType::Int64,
                TSEncoding::Ts2Diff,
                CompressionType::Gzip,
            ),
            (TSDataType::Float, TSEncoding::Gorilla, CompressionType::Lzo),
            (
                TSDataType::Double,
                TSEncoding::Sprintz,
                CompressionType::Lz4,
            ),
            (TSDataType::Text, TSEncoding::Plain, CompressionType::Snappy),
        ];

        for (dt, enc, comp) in cases {
            let original = MeasurementSchema::new(format!("col_{:?}", dt), dt, enc, comp);
            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = MeasurementSchema::deserialize_from(&mut cursor).unwrap();
            assert_eq!(original, decoded);
        }
    }

    // --- ColumnSchema ---

    #[test]
    fn column_schema_construction() {
        let cs = ColumnSchema::new(
            "region".to_string(),
            TSDataType::String,
            ColumnCategory::Tag,
        );
        assert_eq!(cs.column_name, "region");
        assert_eq!(cs.data_type, TSDataType::String);
        assert_eq!(cs.category, ColumnCategory::Tag);
    }

    #[test]
    fn column_schema_serialize_round_trip() {
        let cases = [
            ColumnSchema::new("region".into(), TSDataType::String, ColumnCategory::Tag),
            ColumnSchema::new(
                "temperature".into(),
                TSDataType::Float,
                ColumnCategory::Field,
            ),
        ];

        for original in &cases {
            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = ColumnSchema::deserialize_from(&mut cursor).unwrap();
            assert_eq!(original, &decoded);
        }
    }

    // --- TableSchema ---

    #[test]
    fn table_schema_construction() {
        let ts = TableSchema::new(
            "weather".to_string(),
            vec![
                ColumnSchema::new("region".into(), TSDataType::String, ColumnCategory::Tag),
                ColumnSchema::new(
                    "temperature".into(),
                    TSDataType::Float,
                    ColumnCategory::Field,
                ),
                ColumnSchema::new("humidity".into(), TSDataType::Double, ColumnCategory::Field),
            ],
        );
        assert_eq!(ts.table_name, "weather");
        assert_eq!(ts.column_schemas.len(), 3);
    }

    #[test]
    fn table_schema_find_column() {
        let ts = TableSchema::new(
            "sensors".to_string(),
            vec![
                ColumnSchema::new("device".into(), TSDataType::String, ColumnCategory::Tag),
                ColumnSchema::new("value".into(), TSDataType::Float, ColumnCategory::Field),
            ],
        );

        let (idx, col) = ts.find_column("value").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.data_type, TSDataType::Float);
        assert_eq!(col.category, ColumnCategory::Field);

        assert!(ts.find_column("nonexistent").is_none());
    }

    #[test]
    fn table_schema_tag_and_field_columns() {
        let ts = TableSchema::new(
            "metrics".to_string(),
            vec![
                ColumnSchema::new("host".into(), TSDataType::String, ColumnCategory::Tag),
                ColumnSchema::new("region".into(), TSDataType::String, ColumnCategory::Tag),
                ColumnSchema::new("cpu".into(), TSDataType::Float, ColumnCategory::Field),
                ColumnSchema::new("mem".into(), TSDataType::Double, ColumnCategory::Field),
            ],
        );

        let tags = ts.tag_columns();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].column_name, "host");
        assert_eq!(tags[1].column_name, "region");

        let fields = ts.field_columns();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].column_name, "cpu");
        assert_eq!(fields[1].column_name, "mem");
    }

    #[test]
    fn table_schema_serialize_round_trip() {
        let original = TableSchema::new(
            "weather".to_string(),
            vec![
                ColumnSchema::new("region".into(), TSDataType::String, ColumnCategory::Tag),
                ColumnSchema::new("plant_id".into(), TSDataType::Int32, ColumnCategory::Tag),
                ColumnSchema::new(
                    "temperature".into(),
                    TSDataType::Float,
                    ColumnCategory::Field,
                ),
                ColumnSchema::new("humidity".into(), TSDataType::Double, ColumnCategory::Field),
            ],
        );

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TableSchema::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn table_schema_serialize_empty_columns() {
        let original = TableSchema::new("empty".to_string(), vec![]);

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TableSchema::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    // --- MeasurementSchemaGroup ---

    #[test]
    fn schema_group_non_aligned() {
        let mut group = MeasurementSchemaGroup::new(false);
        assert!(!group.is_aligned);
        assert_eq!(group.measurement_count(), 0);

        group.add_measurement(MeasurementSchema::new(
            "temperature".into(),
            TSDataType::Float,
            TSEncoding::Gorilla,
            CompressionType::Snappy,
        ));
        group.add_measurement(MeasurementSchema::new(
            "humidity".into(),
            TSDataType::Double,
            TSEncoding::Gorilla,
            CompressionType::Lz4,
        ));

        assert_eq!(group.measurement_count(), 2);
        assert!(group.get_measurement("temperature").is_some());
        assert!(group.get_measurement("humidity").is_some());
        assert!(group.get_measurement("nonexistent").is_none());
    }

    #[test]
    fn schema_group_aligned() {
        let group = MeasurementSchemaGroup::new(true);
        assert!(group.is_aligned);
    }

    #[test]
    fn schema_group_measurements_sorted() {
        let mut group = MeasurementSchemaGroup::new(false);
        // Insert out of order — BTreeMap keeps them sorted
        group.add_measurement(MeasurementSchema::new(
            "z_col".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        ));
        group.add_measurement(MeasurementSchema::new(
            "a_col".into(),
            TSDataType::Int64,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        ));
        group.add_measurement(MeasurementSchema::new(
            "m_col".into(),
            TSDataType::Float,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        ));

        let names: Vec<&str> = group
            .measurement_schemas
            .keys()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(names, vec!["a_col", "m_col", "z_col"]);
    }

    #[test]
    fn schema_group_replace_duplicate() {
        let mut group = MeasurementSchemaGroup::new(false);
        group.add_measurement(MeasurementSchema::new(
            "col".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        ));
        // Replace with different type
        group.add_measurement(MeasurementSchema::new(
            "col".into(),
            TSDataType::Float,
            TSEncoding::Gorilla,
            CompressionType::Snappy,
        ));

        assert_eq!(group.measurement_count(), 1);
        let schema = group.get_measurement("col").unwrap();
        assert_eq!(schema.data_type, TSDataType::Float);
        assert_eq!(schema.encoding, TSEncoding::Gorilla);
    }
}
