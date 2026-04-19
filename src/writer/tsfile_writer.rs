// TsFileWriter is the top-level write API.
//
// C++ TsFileWriter maps device IDs to IChunkGroupWriter* (either
// NonAlignedChunkGroupWriter or AlignedChunkGroupWriter). In Rust we use two
// separate BTreeMaps to keep non-aligned and aligned groups distinct —
// no vtable dispatch, exhaustive pattern matching instead.
//
// Non-aligned write path:
//   write_record() / write_tablet()
//   → NonAlignedGroup (device → measurement → ChunkWriter)
//
// Aligned write path:
//   write_aligned_record() / write_aligned_tablet()
//   → AlignedGroup (device → TimeChunkWriter + Vec<ValueChunkWriter>)
//
// Flush strategy: C++ flushes a chunk group when its in-memory size exceeds
// `chunk_chunk_group_size_threshold`. We do the same: after every write we check
// the group size and flush if needed. Explicit `flush()` forces all groups.

use crate::config::Config;
use crate::device_id::DeviceId;
use crate::error::{Result, TsFileError};
use crate::io::io_writer::TsFileIOWriter;
use crate::record::TsRecord;
use crate::schema::MeasurementSchema;
use crate::tablet::Tablet;
use crate::types::{CompressionType, TSDataType, TSEncoding};
use crate::value::TsValue;
use crate::writer::chunk_writer::ChunkWriter;
use crate::writer::time_chunk_writer::TimeChunkWriter;
use crate::writer::value_chunk_writer::ValueChunkWriter;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Internal group types
// ---------------------------------------------------------------------------

/// One non-aligned chunk group: one ChunkWriter per measurement.
///
/// C++ NonAlignedChunkGroupWriter holds a map from measurement id to
/// ChunkWriter*. In Rust the map owns ChunkWriters by value.
struct NonAlignedGroup {
    /// measurement_name → ChunkWriter
    chunk_writers: BTreeMap<String, ChunkWriter>,
}

impl NonAlignedGroup {
    fn new() -> Self {
        Self {
            chunk_writers: BTreeMap::new(),
        }
    }

    fn memory_estimate(&self) -> usize {
        self.chunk_writers
            .values()
            .map(|cw| cw.memory_estimate())
            .sum()
    }
}

/// One aligned chunk group: a shared time column + one value column per measurement.
///
/// C++ AlignedChunkGroupWriter holds TimeChunkWriter* + Vec<ValueChunkWriter*>.
struct AlignedGroup {
    time_writer: TimeChunkWriter,
    /// measurement_name → ValueChunkWriter (ordered to match aligned write order)
    value_writers: BTreeMap<String, ValueChunkWriter>,
}

impl AlignedGroup {
    fn new(time_writer: TimeChunkWriter) -> Self {
        Self {
            time_writer,
            value_writers: BTreeMap::new(),
        }
    }

    fn memory_estimate(&self) -> usize {
        self.time_writer.memory_estimate()
            + self.value_writers.values().map(|v| v.memory_estimate()).sum::<usize>()
    }
}

// ---------------------------------------------------------------------------
// TsFileWriter
// ---------------------------------------------------------------------------

/// Top-level writer for TsFile format.
///
/// Manages schema registration, chunk group lifecycle, and flushes to
/// `TsFileIOWriter`. Supports both non-aligned (tree model) and aligned
/// (table model) writes.
///
/// Usage (non-aligned):
/// ```rust,ignore
/// let mut writer = TsFileWriter::new("out.tsfile", Arc::new(Config::default()))?;
/// writer.register_schema("root.d1", MeasurementSchema::new("s1", Int32, Plain, Lz4));
///
/// let mut rec = TsRecord::new("root.d1", 1000);
/// rec.add_i32("s1", 42);
/// writer.write_record(&rec)?;
/// writer.close()?;
/// ```
pub struct TsFileWriter {
    io_writer: TsFileIOWriter,
    /// device_id → non-aligned group
    non_aligned: BTreeMap<String, NonAlignedGroup>,
    /// device_id → aligned group
    aligned: BTreeMap<String, AlignedGroup>,
    /// Registered schemas: device → measurement → schema
    schemas: BTreeMap<String, BTreeMap<String, MeasurementSchema>>,
    config: Arc<Config>,
}

impl TsFileWriter {
    /// Create (or overwrite) a TsFile at `path`.
    pub fn new(path: impl AsRef<Path>, config: Arc<Config>) -> Result<Self> {
        let io_writer = TsFileIOWriter::new(path, config.clone())?;
        Ok(Self {
            io_writer,
            non_aligned: BTreeMap::new(),
            aligned: BTreeMap::new(),
            schemas: BTreeMap::new(),
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Schema registration
    // -----------------------------------------------------------------------

    /// Register a measurement schema for a device.
    ///
    /// Must be called before writing any data for that (device, measurement)
    /// pair. Registering the same measurement twice is a no-op (first wins).
    pub fn register_schema(&mut self, device_id: &str, schema: MeasurementSchema) {
        self.schemas
            .entry(device_id.to_string())
            .or_default()
            .entry(schema.measurement_name.clone())
            .or_insert(schema);
    }

    // -----------------------------------------------------------------------
    // Non-aligned writes
    // -----------------------------------------------------------------------

    /// Write one row of measurements for a device.
    ///
    /// Null data points (`DataPoint::null(name)`) are silently skipped —
    /// they contribute no data to the chunk. Measurements not covered by a
    /// registered schema are skipped with `SchemaNotFound`.
    pub fn write_record(&mut self, record: &TsRecord) -> Result<()> {
        let device_id = record.device_id.clone();
        let timestamp = record.timestamp;

        for dp in &record.data_points {
            if dp.is_null() {
                continue; // null → no data written for this measurement
            }
            let value = dp.value.as_ref().unwrap();

            let schema = self
                .schemas
                .get(&device_id)
                .and_then(|m| m.get(&dp.measurement_name))
                .ok_or_else(|| TsFileError::NotFound(format!(
                    "schema not found: device={}, measurement={}",
                    device_id, dp.measurement_name
                )))?
                .clone();

            let group = self
                .non_aligned
                .entry(device_id.clone())
                .or_insert_with(NonAlignedGroup::new);

            let cw = group
                .chunk_writers
                .entry(dp.measurement_name.clone())
                .or_insert_with(|| {
                    ChunkWriter::new(
                        dp.measurement_name.clone(),
                        schema.data_type,
                        schema.encoding,
                        schema.compression,
                        self.config.clone(),
                    )
                    .expect("ChunkWriter::new with registered schema must succeed")
                });

            cw.write_value(timestamp, value)?;
        }

        self.maybe_flush_non_aligned(&device_id)?;
        Ok(())
    }

    /// Write a columnar batch for a device.
    ///
    /// For each row and column: if the cell is null it is skipped; otherwise
    /// the value is written to the corresponding ChunkWriter. All columns
    /// must have a registered schema.
    pub fn write_tablet(&mut self, tablet: &Tablet) -> Result<()> {
        let device_id = tablet.device_name.clone();

        for col_idx in 0..tablet.schemas.len() {
            let schema = &tablet.schemas[col_idx];

            // Ensure the schema is registered (auto-register from tablet schema).
            self.schemas
                .entry(device_id.clone())
                .or_default()
                .entry(schema.measurement_name.clone())
                .or_insert_with(|| schema.clone());
        }

        for row in 0..tablet.row_count {
            let timestamp = tablet.timestamps[row];

            for col_idx in 0..tablet.schemas.len() {
                if tablet.is_null(row, col_idx) {
                    continue;
                }

                let schema = &tablet.schemas[col_idx];
                let group = self
                    .non_aligned
                    .entry(device_id.clone())
                    .or_insert_with(NonAlignedGroup::new);

                let cw = group
                    .chunk_writers
                    .entry(schema.measurement_name.clone())
                    .or_insert_with(|| {
                        ChunkWriter::new(
                            schema.measurement_name.clone(),
                            schema.data_type,
                            schema.encoding,
                            schema.compression,
                            self.config.clone(),
                        )
                        .expect("ChunkWriter::new with tablet schema must succeed")
                    });

                let value = column_value(tablet, row, col_idx);
                if let Some(v) = value {
                    cw.write_value(timestamp, &v)?;
                }
            }
        }

        self.maybe_flush_non_aligned(&device_id)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Aligned writes
    // -----------------------------------------------------------------------

    /// Write one aligned row: time + multiple value columns for a device.
    ///
    /// All registered value columns must be provided (use `None` for null).
    /// The time column is always written; null values write a null slot in
    /// the corresponding value chunk.
    pub fn write_aligned_record(
        &mut self,
        device_id: &str,
        timestamp: i64,
        values: &[(&str, Option<TsValue>)],
    ) -> Result<()> {
        // Ensure aligned group exists for this device.
        if !self.aligned.contains_key(device_id) {
            let time_writer = TimeChunkWriter::new(
                self.config.time_encoding_type,
                self.config.default_compression_type,
                self.config.clone(),
            )?;
            self.aligned
                .insert(device_id.to_string(), AlignedGroup::new(time_writer));
        }
        let group = self.aligned.get_mut(device_id).unwrap();

        // Write the timestamp.
        group.time_writer.write(timestamp)?;

        // Write each value column.
        for (meas_name, value_opt) in values {
            let schema = self
                .schemas
                .get(device_id)
                .and_then(|m| m.get(*meas_name))
                .ok_or_else(|| TsFileError::NotFound(format!(
                    "schema not found: device={}, measurement={}",
                    device_id, meas_name
                )))?
                .clone();

            let vcw = group
                .value_writers
                .entry(meas_name.to_string())
                .or_insert_with(|| {
                    ValueChunkWriter::new(
                        meas_name.to_string(),
                        schema.data_type,
                        schema.encoding,
                        schema.compression,
                        self.config.clone(),
                    )
                    .expect("ValueChunkWriter::new must succeed")
                });

            match value_opt {
                None => vcw.write_null()?,
                Some(v) => vcw.write_value(timestamp, v)?,
            }
        }

        self.maybe_flush_aligned(device_id)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Flush and close
    // -----------------------------------------------------------------------

    /// Flush all in-memory chunk groups to disk.
    ///
    /// C++ flushes are triggered both explicitly and when memory thresholds
    /// are reached. This method performs an unconditional full flush.
    pub fn flush(&mut self) -> Result<()> {
        // Flush non-aligned groups.
        let device_ids: Vec<String> = self.non_aligned.keys().cloned().collect();
        for device_id in device_ids {
            self.flush_non_aligned_group(&device_id)?;
        }

        // Flush aligned groups.
        let device_ids: Vec<String> = self.aligned.keys().cloned().collect();
        for device_id in device_ids {
            self.flush_aligned_group(&device_id)?;
        }

        Ok(())
    }

    /// Flush all data and finalize the file (write metadata index + footer).
    ///
    /// After calling `close()` the writer must not be used again.
    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.io_writer.end_file()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn maybe_flush_non_aligned(&mut self, device_id: &str) -> Result<()> {
        let estimate = self
            .non_aligned
            .get(device_id)
            .map(|g| g.memory_estimate())
            .unwrap_or(0);
        if estimate >= self.config.chunk_group_size_threshold as usize {
            self.flush_non_aligned_group(device_id)?;
        }
        Ok(())
    }

    fn maybe_flush_aligned(&mut self, device_id: &str) -> Result<()> {
        let estimate = self
            .aligned
            .get(device_id)
            .map(|g| g.memory_estimate())
            .unwrap_or(0);
        if estimate >= self.config.chunk_group_size_threshold as usize {
            self.flush_aligned_group(device_id)?;
        }
        Ok(())
    }

    fn flush_non_aligned_group(&mut self, device_id: &str) -> Result<()> {
        let group = match self.non_aligned.get_mut(device_id) {
            Some(g) => g,
            None => return Ok(()),
        };

        // Only flush if there is actual data.
        let has_data = group.chunk_writers.values().any(|cw| cw.has_data());
        if !has_data {
            return Ok(());
        }

        let dev = DeviceId::parse(device_id)?;
        self.io_writer.start_chunk_group(&dev)?;

        for cw in group.chunk_writers.values_mut() {
            cw.flush_to(&mut self.io_writer)?;
        }

        self.io_writer.end_chunk_group()?;
        Ok(())
    }

    fn flush_aligned_group(&mut self, device_id: &str) -> Result<()> {
        let group = match self.aligned.get_mut(device_id) {
            Some(g) => g,
            None => return Ok(()),
        };

        let has_data = group.time_writer.has_data()
            || group.value_writers.values().any(|v| v.has_data());
        if !has_data {
            return Ok(());
        }

        let dev = DeviceId::parse(device_id)?;
        self.io_writer.start_chunk_group(&dev)?;

        group.time_writer.flush_to(&mut self.io_writer)?;
        for vcw in group.value_writers.values_mut() {
            vcw.flush_to(&mut self.io_writer)?;
        }

        self.io_writer.end_chunk_group()?;
        Ok(())
    }

}

// Extract a typed TsValue from a Tablet cell. Free function to avoid
// holding an immutable borrow on `self` while a mutable borrow is live.
fn column_value(tablet: &Tablet, row: usize, col: usize) -> Option<TsValue> {
    use crate::tablet::ColumnData;
    match &tablet.columns[col] {
        ColumnData::Boolean(v) => v.get(row).map(|&b| TsValue::Boolean(b)),
        ColumnData::Int32(v) => v.get(row).map(|&i| TsValue::Int32(i)),
        ColumnData::Int64(v) => v.get(row).map(|&i| TsValue::Int64(i)),
        ColumnData::Float(v) => v.get(row).map(|&f| TsValue::Float(f)),
        ColumnData::Double(v) => v.get(row).map(|&f| TsValue::Double(f)),
        ColumnData::Text(v) => v.get(row).map(|b| TsValue::Text(b.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::MeasurementSchema;
    use crate::types::{CompressionType, TSDataType, TSEncoding};
    use tempfile::tempdir;

    fn plain_config() -> Arc<Config> {
        let mut cfg = Config::default();
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.float_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        Arc::new(cfg)
    }

    fn int32_schema(name: &str) -> MeasurementSchema {
        MeasurementSchema::new(
            name.to_string(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        )
    }

    // -----------------------------------------------------------------------
    // Non-aligned: write_record
    // -----------------------------------------------------------------------

    #[test]
    fn write_record_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("basic.tsfile");
        let config = plain_config();
        let mut writer = TsFileWriter::new(&path, config).unwrap();

        writer.register_schema("root.sg1.d1", int32_schema("s1"));
        writer.register_schema("root.sg1.d1", int32_schema("s2"));

        let mut rec = TsRecord::new("root.sg1.d1", 1000);
        rec.add_i32("s1", 42);
        rec.add_i32("s2", 99);
        writer.write_record(&rec).unwrap();

        let mut rec2 = TsRecord::new("root.sg1.d1", 2000);
        rec2.add_i32("s1", 43);
        writer.write_record(&rec2).unwrap();

        writer.close().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"TsFile"));
        assert!(bytes.ends_with(b"TsFile"));
    }

    #[test]
    fn write_record_null_point_skipped() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("null.tsfile");
        let config = plain_config();
        let mut writer = TsFileWriter::new(&path, config).unwrap();
        writer.register_schema("d1", int32_schema("s1"));

        let mut rec = TsRecord::new("d1", 1000);
        rec.add_null("s1"); // null — skipped, no chunk created
        writer.write_record(&rec).unwrap();

        writer.close().unwrap();
    }

    #[test]
    fn write_record_schema_not_found_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("err.tsfile");
        let config = plain_config();
        let mut writer = TsFileWriter::new(&path, config).unwrap();
        // No schema registered for "s1"

        let mut rec = TsRecord::new("d1", 1000);
        rec.add_i32("s1", 42);
        assert!(writer.write_record(&rec).is_err());
    }

    // -----------------------------------------------------------------------
    // Non-aligned: write_tablet
    // -----------------------------------------------------------------------

    #[test]
    fn write_tablet_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("tablet.tsfile");
        let config = plain_config();
        let mut writer = TsFileWriter::new(&path, config).unwrap();

        let schemas = vec![
            int32_schema("temperature"),
            int32_schema("humidity"),
        ];
        let mut tablet = Tablet::new("root.sg1.d1", schemas, 10);
        tablet.add_timestamp(0, 1_000_000).unwrap();
        tablet.add_value_i32(0, 0, 25).unwrap();
        tablet.add_value_i32(0, 1, 60).unwrap();
        tablet.add_timestamp(1, 2_000_000).unwrap();
        tablet.add_value_i32(1, 0, 26).unwrap();
        // col 1 not set → null

        writer.write_tablet(&tablet).unwrap();
        writer.close().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"TsFile"));
    }

    // -----------------------------------------------------------------------
    // Aligned writes
    // -----------------------------------------------------------------------

    #[test]
    fn write_aligned_record_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("aligned.tsfile");
        let config = plain_config();
        let mut writer = TsFileWriter::new(&path, config).unwrap();

        writer.register_schema("d1", int32_schema("s1"));
        writer.register_schema("d1", int32_schema("s2"));

        writer.write_aligned_record(
            "d1",
            1000,
            &[
                ("s1", Some(TsValue::Int32(10))),
                ("s2", Some(TsValue::Int32(20))),
            ],
        ).unwrap();

        writer.write_aligned_record(
            "d1",
            2000,
            &[
                ("s1", None), // null slot
                ("s2", Some(TsValue::Int32(21))),
            ],
        ).unwrap();

        writer.close().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"TsFile"));
        assert!(bytes.ends_with(b"TsFile"));
    }

    // -----------------------------------------------------------------------
    // Multiple devices
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_devices() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.tsfile");
        let config = plain_config();
        let mut writer = TsFileWriter::new(&path, config).unwrap();

        for dev in ["root.sg1.d1", "root.sg1.d2"] {
            writer.register_schema(dev, int32_schema("s1"));
        }

        for dev in ["root.sg1.d1", "root.sg1.d2"] {
            let mut rec = TsRecord::new(dev, 1000);
            rec.add_i32("s1", 42);
            writer.write_record(&rec).unwrap();
        }

        writer.close().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"TsFile"));
    }

    // -----------------------------------------------------------------------
    // Empty file
    // -----------------------------------------------------------------------

    #[test]
    fn close_with_no_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.tsfile");
        let config = plain_config();
        let writer = TsFileWriter::new(&path, config).unwrap();
        writer.close().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"TsFile"));
        assert!(bytes.ends_with(b"TsFile"));
    }
}
