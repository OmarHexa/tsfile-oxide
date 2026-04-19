// C++ TsFileReader is a thin orchestrator: open a file, expose a query
// entry point. 5a supports the tree model only. Multi-measurement
// non-aligned queries and QueryExpression trees are Phase 5b.

use crate::device_id::DeviceId;
use crate::error::{Result, TsFileError};
use crate::io::io_reader::TsFileIOReader;
use crate::reader::filter::Filter;
use crate::reader::metadata_querier::MetadataQuerier;
use crate::reader::result_set::ResultSet;
use crate::reader::scan_iterator::{AlignedSeriesScan, SeriesScanIterator};
use crate::reader::tsblock::ColumnMeta;
use crate::tsfile_format::ChunkMeta;
use std::path::Path;
use std::sync::Arc;

pub struct TsFileReader {
    io: TsFileIOReader,
}

impl TsFileReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self { io: TsFileIOReader::open(path)? })
    }

    /// Tree-model query over one device.
    ///
    /// 5a restrictions:
    /// - Non-aligned: exactly one measurement per query.
    /// - Aligned: N measurements over the device's shared time chunk.
    /// - Mixed aligned/non-aligned chunks in one query → InvalidArg.
    pub fn query(
        &mut self,
        device: &DeviceId,
        measurements: &[&str],
        filter: Option<Box<dyn Filter>>,
    ) -> Result<ResultSet<'_>> {
        if measurements.is_empty() {
            return Err(TsFileError::InvalidArg(
                "query requires at least one measurement".into(),
            ));
        }

        // Fetch ChunkMeta lists per measurement.
        let mut series: Vec<(String, Vec<ChunkMeta>)> = Vec::with_capacity(measurements.len());
        {
            let mut mq = MetadataQuerier::new(&mut self.io);
            for m in measurements {
                let chunks = mq.series_chunks(device, m)?;
                series.push(((*m).to_string(), chunks));
            }
        }

        // Classify: all aligned or all non-aligned? Mixed is illegal.
        // A measurement present in the index but with zero ChunkMeta entries
        // is treated as NotFound — same observable outcome as a missing
        // measurement, and prevents a panic further down in gather_aligned_chunks.
        if let Some((name, _)) = series.iter().find(|(_, cs)| cs.is_empty()) {
            return Err(TsFileError::NotFound(format!(
                "measurement: {device}.{name} (no chunks)"
            )));
        }

        let any_aligned = series.iter().any(|(_, cs)| cs.iter().any(|c| c.is_aligned()));
        let any_regular = series.iter().any(|(_, cs)| cs.iter().any(|c| !c.is_aligned()));
        if any_aligned && any_regular {
            return Err(TsFileError::InvalidArg(
                "mixed aligned and non-aligned chunks in one query".into(),
            ));
        }

        let filter_arc: Option<Arc<dyn Filter>> = filter.map(Arc::from);

        if any_regular {
            // Non-aligned path: exactly one measurement in 5a.
            if measurements.len() != 1 {
                return Err(TsFileError::InvalidArg(
                    "non-aligned queries accept exactly one measurement in 5a".into(),
                ));
            }
            let (name, chunks) = series.pop().unwrap();
            let dt = chunks.first()
                .ok_or_else(|| TsFileError::NotFound(format!("measurement: {device}.{name}")))?
                .data_type;
            let cm: Arc<[ColumnMeta]> = Arc::from(vec![ColumnMeta {
                name: name.clone(),
                data_type: dt,
            }]);
            let it = SeriesScanIterator::new(
                &mut self.io,
                chunks,
                cm.clone(),
                filter_arc.clone(),
            );
            return Ok(ResultSet::from_regular(it, filter_arc, cm));
        }

        // Aligned path.
        let (time_chunks, value_chunks, column_meta) =
            gather_aligned_chunks(&mut self.io, device, &series)?;
        let it = AlignedSeriesScan::new(
            &mut self.io,
            time_chunks,
            value_chunks,
            column_meta.clone(),
            filter_arc.clone(),
        );
        Ok(ResultSet::from_aligned(it, filter_arc, column_meta))
    }
}

/// Locate the aligned time chunks for `device` plus the per-measurement
/// value chunk lists (in the order of `series`). The writer stores
/// aligned time chunks under measurement_name = "" (see
/// src/writer/time_chunk_writer.rs).
fn gather_aligned_chunks(
    io: &mut TsFileIOReader,
    device: &DeviceId,
    series: &[(String, Vec<ChunkMeta>)],
) -> Result<(Vec<ChunkMeta>, Vec<Vec<ChunkMeta>>, Arc<[ColumnMeta]>)> {
    let map = io.get_timeseries_indexes(device)?;
    let time_idx = map.get("").ok_or_else(|| {
        TsFileError::NotFound(format!("aligned time chunks: {device}"))
    })?;
    let time_chunks = time_idx.chunk_meta_list.clone();

    let value_chunks: Vec<Vec<ChunkMeta>> = series.iter().map(|(_, cs)| cs.clone()).collect();

    let column_meta: Vec<ColumnMeta> = series.iter().map(|(name, cs)| ColumnMeta {
        name: name.clone(),
        data_type: cs.first()
            .expect("classification rejects empty series")
            .data_type,
    }).collect();

    // Every value column must have the same number of chunks as the time column.
    for vc in &value_chunks {
        if vc.len() != time_chunks.len() {
            return Err(TsFileError::Corrupted(format!(
                "aligned value column has {} chunks, expected {} (matching time)",
                vc.len(), time_chunks.len()
            )));
        }
    }

    Ok((time_chunks, value_chunks, Arc::from(column_meta)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::filter::time::TimeBetween;
    use crate::reader::test_fixtures;
    use crate::value::TsValue;

    #[test]
    fn non_aligned_single_measurement_round_trip() {
        let (_dir, path, device, measurement) =
            test_fixtures::write_two_chunk_int64_file();
        let mut reader = TsFileReader::open(&path).unwrap();
        let rs = reader.query(&device, &[&measurement], None).unwrap();
        let rows: Vec<_> = rs.collect::<Result<Vec<_>>>().unwrap();
        assert!(rows.len() >= 20, "two chunks should produce >= 20 rows total");
        // Timestamps strictly increasing.
        let mut last = i64::MIN;
        for r in &rows {
            assert!(r.timestamp > last);
            last = r.timestamp;
        }
    }

    #[test]
    fn aligned_multi_column_round_trip() {
        let (_dir, path, device, names) =
            test_fixtures::write_aligned_two_column_file(50);
        let mut reader = TsFileReader::open(&path).unwrap();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        let rs = reader.query(&device, &names_ref, None).unwrap();
        let rows: Vec<_> = rs.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.len(), 50);
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.timestamp, i as i64);
            assert_eq!(r.values[0], Some(TsValue::Int64(i as i64)));
            assert_eq!(r.values[1], Some(TsValue::Double(i as f64)));
        }
    }

    #[test]
    fn aligned_filter_pushdown_time_between() {
        let (_dir, path, device, names) =
            test_fixtures::write_aligned_two_column_file(100);
        let mut reader = TsFileReader::open(&path).unwrap();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        let filter: Box<dyn Filter> = Box::new(TimeBetween::new(10, 20, true));
        let rs = reader.query(&device, &names_ref, Some(filter)).unwrap();
        let rows: Vec<_> = rs.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.len(), 11); // timestamps 10..=20 inclusive
        assert_eq!(rows.first().unwrap().timestamp, 10);
        assert_eq!(rows.last().unwrap().timestamp, 20);
    }

    #[test]
    fn non_aligned_requires_single_measurement() {
        let (_dir, path, device, measurement) =
            test_fixtures::write_two_chunk_int64_file();
        let mut reader = TsFileReader::open(&path).unwrap();
        // unwrap_err() requires T: Debug; use .err().expect() instead since
        // ResultSet holds dyn Trait fields that don't derive Debug.
        let err = reader.query(&device, &[&measurement, &measurement], None)
            .err().expect("expected an error");
        assert!(matches!(err, TsFileError::InvalidArg(_)), "expected InvalidArg, got {err:?}");
    }

    #[test]
    fn missing_measurement_errors() {
        let (_dir, path, device, _m) = test_fixtures::write_two_chunk_int64_file();
        let mut reader = TsFileReader::open(&path).unwrap();
        let err = reader.query(&device, &["nope"], None)
            .err().expect("expected an error");
        assert!(matches!(err, TsFileError::NotFound(_)), "expected NotFound, got {err:?}");
    }

    #[test]
    fn empty_measurements_errors() {
        let (_dir, path, device, _m) = test_fixtures::write_two_chunk_int64_file();
        let mut reader = TsFileReader::open(&path).unwrap();
        let err = reader.query(&device, &[], None)
            .err().expect("expected an error");
        assert!(matches!(err, TsFileError::InvalidArg(_)));
    }
}
