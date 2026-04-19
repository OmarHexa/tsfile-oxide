//! Shared test fixtures for reader tests. Builds small, deterministic
//! `.tsfile`s using the Phase-4 writer so reader tests don't each
//! re-implement writer setup.

#![cfg(test)]

use crate::config::Config;
use crate::device_id::DeviceId;
use crate::schema::MeasurementSchema;
use crate::tablet::Tablet;
use crate::types::{CompressionType, TSDataType, TSEncoding};
use crate::writer::tsfile_writer::TsFileWriter;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// Write a non-aligned tsfile with one device, one int64 measurement,
/// and TWO chunks (achieved by calling `flush()` between two tablets).
/// Returns (tempdir, path, device, measurement_name).
///
/// Rows per chunk: 10. Chunk 1 times = 0..10, values = 0..10.
/// Chunk 2 times = 10..20, values = 100..110. Timestamps are strictly
/// increasing across chunks so readers can assert ordering.
pub fn write_two_chunk_int64_file() -> (TempDir, PathBuf, DeviceId, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two-chunk.tsfile");
    let device = DeviceId::parse("root.sg.d1").unwrap();
    let measurement = "m".to_string();

    let schema = MeasurementSchema::new(
        measurement.clone(),
        TSDataType::Int64,
        TSEncoding::Ts2Diff,
        CompressionType::Uncompressed,
    );

    let mut w = TsFileWriter::new(&path, Arc::new(Config::default())).unwrap();

    // Tablet 1: times 0..10, values 0..10
    let tablet1 = build_int64_tablet(&device, schema.clone(), 0..10, 0..10);
    w.write_tablet(&tablet1).unwrap();
    w.flush().unwrap(); // seal chunk 1

    // Tablet 2: times 10..20, values 100..110
    let tablet2 = build_int64_tablet(&device, schema, 10..20, 100..110);
    w.write_tablet(&tablet2).unwrap();
    w.close().unwrap();

    (dir, path, device, measurement)
}

fn build_int64_tablet(
    device: &DeviceId,
    schema: MeasurementSchema,
    times: std::ops::Range<i64>,
    values: std::ops::Range<i64>,
) -> Tablet {
    // Tablet::new takes the device path string (dot-joined), a Vec of schemas,
    // and a capacity hint. Then each row is added via add_timestamp + add_value_i64.
    let times_vec: Vec<i64> = times.collect();
    let values_vec: Vec<i64> = values.collect();
    let n = times_vec.len();

    let mut tablet = Tablet::new(device.to_string(), vec![schema], n);
    for i in 0..n {
        tablet.add_timestamp(i, times_vec[i]).unwrap();
        // col 0 = the single measurement column
        tablet.add_value_i64(i, 0, values_vec[i]).unwrap();
    }
    tablet
}

// ---------------------------------------------------------------------------
// Aligned fixtures (Task 11)
// ---------------------------------------------------------------------------

use crate::io::io_reader::TsFileIOReader;
use crate::reader::tsblock::ColumnMeta;
use crate::tsfile_format::{ChunkMeta, TimeseriesIndex};
use crate::value::TsValue;

/// Write an aligned tsfile with one device and two measurements
/// ("i": Int64, "d": Double) and `n` rows where row r has timestamp r,
/// values (r as i64, r as f64), and no nulls. Returns `(tempdir, path,
/// device, measurement_names)` in column order.
///
/// We use `write_aligned_record` which requires explicit schema
/// registration. Schemas are registered before any writes so that the
/// value chunk writers pick up the correct data type and encoding.
pub fn write_aligned_two_column_file(n: usize) -> (TempDir, PathBuf, DeviceId, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aligned.tsfile");
    let device = DeviceId::parse("root.sg.aligned_d").unwrap();
    let names = vec!["i".to_string(), "d".to_string()];

    let schemas = vec![
        MeasurementSchema::new("i".into(), TSDataType::Int64,  TSEncoding::Plain, CompressionType::Uncompressed),
        MeasurementSchema::new("d".into(), TSDataType::Double, TSEncoding::Plain, CompressionType::Uncompressed),
    ];

    let mut w = crate::writer::tsfile_writer::TsFileWriter::new(&path, Arc::new(Config::default())).unwrap();
    // Explicit schema registration is required for write_aligned_record — the
    // aligned write path does not auto-register from the value slice.
    for s in &schemas {
        w.register_schema(&device.to_string(), s.clone());
    }
    for r in 0..n {
        let t = r as i64;
        w.write_aligned_record(
            &device.to_string(),
            t,
            &[
                ("i", Some(TsValue::Int64(t))),
                ("d", Some(TsValue::Double(t as f64))),
            ],
        ).unwrap();
    }
    w.close().unwrap();

    (dir, path, device, names)
}

/// Classify a device's TimeseriesIndex map into
/// `(time_chunks, value_chunks, column_meta)` for the aligned reader
/// pipeline. Time chunks live under the empty-string measurement name
/// (the writer convention for aligned time columns). Value columns are
/// returned in the order `value_names` specifies.
pub fn gather_aligned_chunks(
    io: &mut TsFileIOReader,
    device: &DeviceId,
    value_names: &[String],
) -> (Vec<ChunkMeta>, Vec<Vec<ChunkMeta>>, std::sync::Arc<[ColumnMeta]>) {
    let map = io.get_timeseries_indexes(device).unwrap();

    // Time chunks live under measurement_name = "" (empty string) per
    // the writer convention (see src/writer/time_chunk_writer.rs).
    let time_idx: &TimeseriesIndex = map.get("")
        .expect("aligned device must have an empty-named time TimeseriesIndex");
    let time_chunks: Vec<ChunkMeta> = time_idx.chunk_meta_list.clone();

    let mut value_chunks: Vec<Vec<ChunkMeta>> = Vec::with_capacity(value_names.len());
    let mut column_meta: Vec<ColumnMeta> = Vec::with_capacity(value_names.len());
    for name in value_names {
        let idx = map.get(name).unwrap_or_else(|| panic!("missing aligned value column {name}"));
        value_chunks.push(idx.chunk_meta_list.clone());
        column_meta.push(ColumnMeta {
            name: name.clone(),
            data_type: idx.data_type,
        });
    }

    (time_chunks, value_chunks, std::sync::Arc::from(column_meta))
}
