// C++ MetadataQuerier adds an LRU cache in front of TsFileIOReader. 5a
// skips the cache — TsFileIOReader already loads the whole device→
// measurement→TimeseriesIndex map eagerly, so a pass-through is
// equivalent for now. 5b adds the cache when lazy loading lands.

use crate::device_id::DeviceId;
use crate::error::{Result, TsFileError};
use crate::io::io_reader::TsFileIOReader;
use crate::tsfile_format::ChunkMeta;

pub struct MetadataQuerier<'a> {
    io: &'a mut TsFileIOReader,
}

impl<'a> MetadataQuerier<'a> {
    pub fn new(io: &'a mut TsFileIOReader) -> Self { Self { io } }

    /// Return all ChunkMeta entries for a single (device, measurement),
    /// ordered chronologically as the writer emitted them.
    pub fn series_chunks(
        &mut self,
        device: &DeviceId,
        measurement: &str,
    ) -> Result<Vec<ChunkMeta>> {
        let map = self.io.get_timeseries_indexes(device)?;
        let ts_index = map.get(measurement).ok_or_else(|| {
            TsFileError::NotFound(format!("measurement: {}.{measurement}", device))
        })?;
        Ok(ts_index.chunk_meta_list.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::device_id::DeviceId;
    use crate::schema::MeasurementSchema;
    use crate::tablet::Tablet;
    use crate::types::{CompressionType, TSDataType, TSEncoding};
    use crate::writer::tsfile_writer::TsFileWriter;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Write a minimal non-aligned tsfile with one device and one
    /// measurement. Returns the path and metadata the test needs.
    fn write_minimal_file() -> (tempfile::TempDir, std::path::PathBuf, DeviceId, String) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mq.tsfile");
        let device = DeviceId::parse("root.sg.d").unwrap();
        let measurement = "m".to_string();

        let mut w = TsFileWriter::new(&path, Arc::new(Config::default())).unwrap();

        let tablet = build_int64_tablet(&device, &measurement, 10);
        w.write_tablet(&tablet).unwrap();
        w.close().unwrap();
        (dir, path, device, measurement)
    }

    /// Build a Tablet holding `n` int64 rows with times 0..n and values 100..100+n.
    fn build_int64_tablet(device: &DeviceId, measurement: &str, n: usize) -> Tablet {
        let schema = MeasurementSchema::new(
            measurement.to_string(),
            TSDataType::Int64,
            TSEncoding::Ts2Diff,
            CompressionType::Uncompressed,
        );
        let mut tablet = Tablet::new(device.to_string(), vec![schema], n);
        for i in 0..n {
            tablet.add_timestamp(i, i as i64).unwrap();
            tablet.add_value_i64(i, 0, 100 + i as i64).unwrap();
        }
        tablet
    }

    #[test]
    fn returns_chunk_metas_for_known_series() {
        let (_dir, path, device, measurement) = write_minimal_file();
        let mut io = TsFileIOReader::open(&path).unwrap();
        let mut mq = MetadataQuerier::new(&mut io);
        let chunks = mq.series_chunks(&device, &measurement).unwrap();
        assert!(!chunks.is_empty(), "expected at least one chunk for the written series");
        assert_eq!(chunks[0].measurement_name, measurement);
        assert_eq!(chunks[0].data_type, TSDataType::Int64);
    }

    #[test]
    fn missing_measurement_returns_not_found() {
        let (_dir, path, device, _m) = write_minimal_file();
        let mut io = TsFileIOReader::open(&path).unwrap();
        let mut mq = MetadataQuerier::new(&mut io);
        let err = mq.series_chunks(&device, "nope").unwrap_err();
        assert!(
            matches!(err, TsFileError::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }

    #[test]
    fn missing_device_returns_not_found() {
        let (_dir, path, _device, _m) = write_minimal_file();
        let mut io = TsFileIOReader::open(&path).unwrap();
        let mut mq = MetadataQuerier::new(&mut io);
        let missing = DeviceId::parse("root.missing.d").unwrap();
        let err = mq.series_chunks(&missing, "m").unwrap_err();
        assert!(
            matches!(err, TsFileError::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }
}
