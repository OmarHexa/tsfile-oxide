// C++ TsFileSeriesScanIterator iterates all chunks for one (device,
// measurement), opening a ChunkReader per chunk. 5a's regular variant
// mirrors that shape; the aligned variant ships in Task 11.

use crate::error::Result;
use crate::io::io_reader::TsFileIOReader;
use crate::reader::chunk_reader::{AlignedTimeChunkReader, AlignedValueChunkReader, RegularChunkReader};
use crate::reader::filter::Filter;
use crate::reader::tsblock::{Column, ColumnMeta, TsBlock};
use crate::tsfile_format::ChunkMeta;
use std::sync::Arc;

pub struct SeriesScanIterator<'a> {
    io: &'a mut TsFileIOReader,
    chunks: Vec<ChunkMeta>,
    cursor: usize,
    current: Option<RegularChunkReader>,
    column_meta: Arc<[ColumnMeta]>,
    filter: Option<Arc<dyn Filter>>,
}

impl<'a> SeriesScanIterator<'a> {
    pub fn new(
        io: &'a mut TsFileIOReader,
        chunks: Vec<ChunkMeta>,
        column_meta: Arc<[ColumnMeta]>,
        filter: Option<Arc<dyn Filter>>,
    ) -> Self {
        Self { io, chunks, cursor: 0, current: None, column_meta, filter }
    }

    /// Pull the next decoded block. Returns `Ok(None)` when all chunks
    /// (surviving the chunk-statistic filter) are exhausted.
    pub fn next_block(&mut self) -> Result<Option<TsBlock>> {
        loop {
            // Drain the active chunk before advancing.
            if let Some(r) = self.current.as_mut() {
                if let Some(b) = r.next_block()? { return Ok(Some(b)); }
                self.current = None;
            }
            if self.cursor >= self.chunks.len() { return Ok(None); }

            // Advance, applying chunk-level statistic filter.
            let cm = self.chunks[self.cursor].clone();
            self.cursor += 1;
            if let Some(f) = self.filter.as_ref() {
                if !f.satisfy_statistic(&cm.statistic) { continue; }
            }

            let (header, page_bytes) = self.io.load_chunk(&cm)?;
            self.current = Some(RegularChunkReader::new(
                header,
                page_bytes,
                self.column_meta.clone(),
                self.filter.clone(),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// AlignedSeriesScan
// ---------------------------------------------------------------------------

/// Paired time + N value scan for one aligned device query.
///
/// Chunk groups are walked in lockstep: each `time_chunks[i]` is paired
/// positionally with `value_chunks[col][i]` for every value column. Pages
/// within each chunk are matched positionally as well — the writer
/// coordinates page boundaries so rows align one-for-one.
///
/// C++ AlignedSeriesReader manages this with two separate ChunkReader
/// instances (time + value). In Rust we name the pairing explicitly in
/// `AlignedSeriesScan` so the caller cannot accidentally mix up the ordering.
pub struct AlignedSeriesScan<'a> {
    io: &'a mut TsFileIOReader,
    time_chunks: Vec<ChunkMeta>,
    /// Outer dim: value column (parallel to column_meta). Inner dim:
    /// chunk, parallel to time_chunks.
    value_chunks: Vec<Vec<ChunkMeta>>,
    cursor: usize,
    current_time: Option<AlignedTimeChunkReader>,
    current_values: Vec<AlignedValueChunkReader>,
    column_meta: Arc<[ColumnMeta]>,
    filter: Option<Arc<dyn Filter>>,
}

impl<'a> AlignedSeriesScan<'a> {
    pub fn new(
        io: &'a mut TsFileIOReader,
        time_chunks: Vec<ChunkMeta>,
        value_chunks: Vec<Vec<ChunkMeta>>,
        column_meta: Arc<[ColumnMeta]>,
        filter: Option<Arc<dyn Filter>>,
    ) -> Self {
        debug_assert_eq!(value_chunks.len(), column_meta.len(),
            "value_chunks outer dim must match column_meta length");
        for vc in &value_chunks {
            debug_assert_eq!(vc.len(), time_chunks.len(),
                "each value column must have one ChunkMeta per time chunk");
        }
        Self {
            io, time_chunks, value_chunks,
            cursor: 0,
            current_time: None,
            current_values: Vec::new(),
            column_meta, filter,
        }
    }

    /// Pull the next decoded TsBlock. Returns `Ok(None)` when all chunks
    /// (surviving the time-chunk statistic filter) are exhausted.
    pub fn next_block(&mut self) -> Result<Option<TsBlock>> {
        loop {
            // Pull one time page, then one value page per column.
            if let Some(tr) = self.current_time.as_mut() {
                if let Some(times) = tr.next_time_page()? {
                    let n = times.len();
                    let mut columns = Vec::with_capacity(self.current_values.len());
                    for (col_idx, vr) in self.current_values.iter_mut().enumerate() {
                        let col = vr.next_value_page(n)?.ok_or_else(|| {
                            crate::error::TsFileError::Corrupted(format!(
                                "aligned value chunk[{col_idx}] produced fewer pages than time chunk"
                            ))
                        })?;
                        columns.push(col);
                    }
                    return Ok(Some(TsBlock::new(times, columns, self.column_meta.clone())));
                }
                // Time chunk exhausted; advance to next chunk.
                self.current_time = None;
                self.current_values.clear();
            }

            if self.cursor >= self.time_chunks.len() { return Ok(None); }

            let time_cm = self.time_chunks[self.cursor].clone();
            let value_cms: Vec<ChunkMeta> = self.value_chunks
                .iter().map(|col| col[self.cursor].clone()).collect();
            self.cursor += 1;

            // Chunk-level statistic pruning uses the time chunk's statistic.
            // Value-column predicate pushdown is deferred to phase 5b.
            if let Some(f) = self.filter.as_ref() {
                if !f.satisfy_statistic(&time_cm.statistic) { continue; }
            }

            let (th, tp) = self.io.load_chunk(&time_cm)?;
            self.current_time = Some(AlignedTimeChunkReader::new(th, tp, self.filter.clone()));
            for vc in &value_cms {
                let (vh, vp) = self.io.load_chunk(vc)?;
                self.current_values.push(AlignedValueChunkReader::new(vh, vp));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::filter::time::TimeLt;
    use crate::reader::metadata_querier::MetadataQuerier;
    use crate::reader::test_fixtures;
    use crate::reader::tsblock::Column;
    use crate::types::TSDataType;

    fn cm_int64(name: &str) -> Arc<[ColumnMeta]> {
        Arc::from(vec![ColumnMeta { name: name.into(), data_type: TSDataType::Int64 }])
    }

    #[test]
    fn iterates_all_chunks_in_order() {
        let (_dir, path, device, measurement) =
            test_fixtures::write_two_chunk_int64_file();

        let mut io = TsFileIOReader::open(&path).unwrap();
        let chunks = MetadataQuerier::new(&mut io)
            .series_chunks(&device, &measurement)
            .unwrap();
        assert!(chunks.len() >= 2, "test fixture should produce >= 2 chunks");

        let mut it = SeriesScanIterator::new(&mut io, chunks, cm_int64(&measurement), None);
        let mut total_rows = 0usize;
        let mut last_time = i64::MIN;
        while let Some(block) = it.next_block().unwrap() {
            for &t in &block.times {
                assert!(t > last_time, "timestamps must be strictly increasing across chunks");
                last_time = t;
            }
            // Verify the column variant is correct.
            assert!(matches!(&block.columns[0], Column::Int64 { .. }));
            total_rows += block.num_rows();
        }
        assert!(total_rows >= 20, "expected at least 20 rows across the two chunks");
    }

    #[test]
    fn statistic_pruning_skips_all_chunks() {
        let (_dir, path, device, measurement) =
            test_fixtures::write_two_chunk_int64_file();
        let mut io = TsFileIOReader::open(&path).unwrap();
        let chunks = MetadataQuerier::new(&mut io)
            .series_chunks(&device, &measurement)
            .unwrap();

        // TimeLt(i64::MIN) rejects every non-empty chunk.
        let f: Arc<dyn Filter> = Arc::new(TimeLt::new(i64::MIN));
        let mut it = SeriesScanIterator::new(&mut io, chunks, cm_int64(&measurement), Some(f));
        assert!(it.next_block().unwrap().is_none());
    }

    #[test]
    fn aligned_scan_round_trip_two_columns() {
        use crate::reader::test_fixtures;
        let (_dir, path, device, meas) = test_fixtures::write_aligned_two_column_file(50);
        let mut io = TsFileIOReader::open(&path).unwrap();
        let (time_chunks, value_chunks, column_meta) =
            test_fixtures::gather_aligned_chunks(&mut io, &device, &meas);

        let mut scan = AlignedSeriesScan::new(
            &mut io, time_chunks, value_chunks, column_meta, None,
        );
        let mut total = 0usize;
        while let Some(block) = scan.next_block().unwrap() {
            for r in 0..block.num_rows() {
                let t = block.times[r];
                let expected_i = t as i64;
                let expected_d = t as f64;
                match &block.columns[0] {
                    Column::Int64 { values, nulls: Some(bm) } => {
                        assert!(!bm.get(r));
                        assert_eq!(values[r], expected_i);
                    }
                    _ => panic!("col 0 wrong"),
                }
                match &block.columns[1] {
                    Column::Double { values, nulls: Some(bm) } => {
                        assert!(!bm.get(r));
                        assert_eq!(values[r], expected_d);
                    }
                    _ => panic!("col 1 wrong"),
                }
                total += 1;
            }
        }
        assert_eq!(total, 50);
    }
}
