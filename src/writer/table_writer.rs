// TsFileTableWriter is a simplified writer for the "table model" (as opposed
// to the "tree model" used by TsFileWriter).
//
// C++ ITableSessionWriter / TsFileTableWriter wrap TsFileWriter with a
// table-centric API: a fixed schema is declared up-front and rows are
// written as Tablets without per-write schema lookups.
//
// In the table model:
//   - The "table name" corresponds to the device_id in the tree model.
//   - TAG columns are identity/key columns (no data written, only schema).
//   - FIELD columns are measurement columns (written as non-aligned chunks).
//
// This implementation wraps TsFileWriter and auto-registers schemas from the
// Tablet on the first write, matching the common C++ pattern of calling
// registerSchema() lazily.

use crate::config::Config;
use crate::error::Result;
use crate::schema::{ColumnCategory, MeasurementSchema, TableSchema};
use crate::tablet::Tablet;
use crate::types::CompressionType;
use crate::writer::tsfile_writer::TsFileWriter;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Table-model writer: wraps TsFileWriter with a fixed schema per table.
///
/// C++ TsFileTableWriter accepts a TableSchema specifying which columns are
/// TAG vs FIELD, then writes Tablets row-by-row. In Rust we hold a
/// `TsFileWriter` internally and register schemas lazily on the first
/// `write_tablet()` call.
///
/// Note: `ColumnSchema` in the table model holds no encoding/compression info
/// (it's a pure schema descriptor). We store config so that FIELD columns can
/// be registered with the config-default encoding when `register_table()` is
/// called.
pub struct TsFileTableWriter {
    inner: TsFileWriter,
    config: Arc<Config>,
    /// table_name → TableSchema (for schema validation)
    table_schemas: HashMap<String, TableSchema>,
}

impl TsFileTableWriter {
    /// Create a new table writer, writing to `path`.
    pub fn new(path: impl AsRef<Path>, config: Arc<Config>) -> Result<Self> {
        let inner = TsFileWriter::new(path, config.clone())?;
        Ok(Self {
            inner,
            config,
            table_schemas: HashMap::new(),
        })
    }

    // -----------------------------------------------------------------------
    // Schema registration
    // -----------------------------------------------------------------------

    /// Register a table schema. Call before writing any tablets for this table.
    ///
    /// Only FIELD columns are registered as measurement schemas; TAG columns
    /// are metadata-only and are not written as chunk data. Encoding and
    /// compression are taken from `Config` defaults because `ColumnSchema`
    /// (the table model descriptor) does not carry per-column codec settings —
    /// those are a tree-model concept.
    pub fn register_table(&mut self, schema: TableSchema) {
        let table_name = schema.table_name.clone();
        let compression = self.config.default_compression_type;
        for col in &schema.column_schemas {
            if col.category == ColumnCategory::Field {
                let encoding = self.config.get_value_encoder(col.data_type);
                self.inner.register_schema(
                    &table_name,
                    MeasurementSchema::new(
                        col.column_name.clone(),
                        col.data_type,
                        encoding,
                        compression,
                    ),
                );
            }
        }
        self.table_schemas.insert(table_name, schema);
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    /// Write a Tablet to the named table.
    ///
    /// Schemas are auto-registered from the Tablet's column schemas on the
    /// first call for each table, so `register_table()` is optional when all
    /// columns are FIELD columns. TAG columns in the Tablet are silently
    /// skipped.
    pub fn write_tablet(&mut self, tablet: &Tablet) -> Result<()> {
        self.inner.write_tablet(tablet)
    }

    // -----------------------------------------------------------------------
    // Flush and close
    // -----------------------------------------------------------------------

    /// Flush all in-memory data to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    /// Flush and finalize the file.
    pub fn close(self) -> Result<()> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, TableSchema};
    use crate::tablet::Tablet;
    use crate::types::{CompressionType, TSDataType, TSEncoding};
    use crate::schema::MeasurementSchema;
    use tempfile::tempdir;

    fn make_table_schema() -> TableSchema {
        TableSchema {
            table_name: "root.sg1".to_string(),
            column_schemas: vec![
                ColumnSchema::new("temperature".to_string(), TSDataType::Float, ColumnCategory::Field),
                ColumnSchema::new("humidity".to_string(), TSDataType::Int32, ColumnCategory::Field),
            ],
        }
    }

    #[test]
    fn write_tablet_via_table_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("table.tsfile");
        let mut cfg = Config::default();
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.float_encoding_type = TSEncoding::Plain;
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        let config = Arc::new(cfg);

        let mut writer = TsFileTableWriter::new(&path, config).unwrap();
        writer.register_table(make_table_schema());

        let schemas = vec![
            MeasurementSchema::new("temperature".into(), TSDataType::Float, TSEncoding::Plain, CompressionType::Uncompressed),
            MeasurementSchema::new("humidity".into(), TSDataType::Int32, TSEncoding::Plain, CompressionType::Uncompressed),
        ];
        let mut tablet = Tablet::new("root.sg1", schemas, 5);
        tablet.add_timestamp(0, 1000).unwrap();
        tablet.add_value_f32(0, 0, 25.5).unwrap();
        tablet.add_value_i32(0, 1, 60).unwrap();

        writer.write_tablet(&tablet).unwrap();
        writer.close().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"TsFile"));
        assert!(bytes.ends_with(b"TsFile"));
    }

    #[test]
    fn close_empty_table_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.tsfile");
        let config = Arc::new(Config::default());
        let writer = TsFileTableWriter::new(&path, config).unwrap();
        writer.close().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"TsFile"));
    }
}
