// C++ TsFileSeriesScanIterator iterates all chunks for one (device,
// measurement), opening a ChunkReader per chunk. 5a's regular variant
// mirrors that shape; the aligned variant ships in Task 11.

use crate::error::Result;
use crate::io::io_reader::TsFileIOReader;
use crate::reader::chunk_reader::RegularChunkReader;
use crate::reader::filter::Filter;
use crate::reader::tsblock::{ColumnMeta, TsBlock};
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
}
