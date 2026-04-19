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
