// C++ ChunkReader has two subclasses (Regular and Aligned). We model it
// as an enum — two concrete variants, closed set. Each reads chunk
// bytes (already in memory via TsFileIOReader::load_chunk) and yields a
// decoded page at a time.
//
// Wire format mirror (see src/writer/page_writer.rs and chunk_writer.rs):
//   Chunk = ChunkHeader + { PageHeader + compressed_page_body }*
//   Decompressed page body = [time_data_size: var_u32] [time_bytes] [value_bytes]
//   Time encoding: config.time_encoding_type (hard-coded to Ts2Diff in 5a;
//   the on-disk format does not store it. See TODO below.)

use crate::bitmap::BitMap;
use crate::compress::Compressor;
use crate::encoding::decoder::Decoder;
use crate::error::{Result, TsFileError};
use crate::reader::filter::Filter;
use crate::reader::tsblock::{Column, ColumnMeta, TsBlock};
use crate::serialize;
use crate::tsfile_format::{ChunkHeader, PageHeader};
use crate::types::{TSDataType, TSEncoding};
use std::io::{Cursor, Read};
use std::sync::Arc;

/// TODO(phase-5b): remove this hard-coded assumption and carry the writer's
/// time encoding through the reader (e.g., via TsFileReader's Config).
const TIME_ENCODING: TSEncoding = TSEncoding::Ts2Diff;

/// Reads a single regular (non-aligned) chunk's bytes and yields decoded
/// TsBlocks one page at a time.
pub struct RegularChunkReader {
    header: ChunkHeader,
    remaining: Cursor<Vec<u8>>,
    column_meta: Arc<[ColumnMeta]>,
    filter: Option<Arc<dyn Filter>>,
}

/// Three-variant enum matching the C++ class hierarchy (RegularChunkReader,
/// AlignedTimeChunkReader, AlignedValueChunkReader). The set of concrete
/// reader types is closed and known at compile time — enum dispatch instead
/// of vtable.
pub enum ChunkReader {
    Regular(RegularChunkReader),
    AlignedTime(AlignedTimeChunkReader),
    AlignedValue(AlignedValueChunkReader),
}

impl RegularChunkReader {
    pub fn new(
        header: ChunkHeader,
        page_bytes: Vec<u8>,
        column_meta: Arc<[ColumnMeta]>,
        filter: Option<Arc<dyn Filter>>,
    ) -> Self {
        debug_assert!(header.is_regular_chunk());
        Self { header, remaining: Cursor::new(page_bytes), column_meta, filter }
    }

    pub fn has_more(&self) -> bool {
        (self.remaining.position() as usize) < self.remaining.get_ref().len()
    }

    /// Decode the next page into a single-column TsBlock, applying
    /// page-level statistic filtering. Returns `Ok(None)` when the
    /// chunk is exhausted.
    pub fn next_block(&mut self) -> Result<Option<TsBlock>> {
        while self.has_more() {
            let has_stat = !self.header.is_single_page();
            let page_header = PageHeader::deserialize_from(
                &mut self.remaining,
                self.header.data_type,
                has_stat,
            )?;

            // Page-level statistic pruning (only applicable to multi-page chunks).
            if let (Some(filter), Some(stat)) =
                (self.filter.as_ref(), page_header.statistic.as_ref())
                && !filter.satisfy_statistic(stat)
            {
                let mut skip = vec![0u8; page_header.compressed_size as usize];
                self.remaining.read_exact(&mut skip)?;
                continue;
            }

            let mut compressed = vec![0u8; page_header.compressed_size as usize];
            self.remaining.read_exact(&mut compressed)?;
            let compressor = Compressor::new(self.header.compression);
            let body = compressor
                .decompress(&compressed, page_header.uncompressed_size as usize)?;

            let block = decode_regular_page(&self.header, &body, self.column_meta.clone())?;
            return Ok(Some(block));
        }
        Ok(None)
    }
}

/// Decode one decompressed regular-page body into a single-column TsBlock.
fn decode_regular_page(
    header: &ChunkHeader,
    body: &[u8],
    column_meta: Arc<[ColumnMeta]>,
) -> Result<TsBlock> {
    let mut cur = Cursor::new(body);
    let time_buf_size = serialize::read_var_u32(&mut cur)? as usize;
    let time_start = cur.position() as usize;
    let time_end = time_start + time_buf_size;
    if time_end > body.len() {
        return Err(TsFileError::Corrupted(format!(
            "page body too short: time_data_size={time_buf_size} exceeds body len={}",
            body.len()
        )));
    }
    let time_bytes = &body[time_start..time_end];
    let value_bytes = &body[time_end..];

    let times = decode_i64_stream(TIME_ENCODING, TSDataType::Int64, time_bytes)?;
    let values = decode_value_column(header.encoding, header.data_type, value_bytes, times.len())?;

    Ok(TsBlock::new(times, vec![values], column_meta))
}

fn is_unexpected_eof(err: &TsFileError) -> bool {
    // `serialize::read_var_u32/u64` now return `Io(UnexpectedEof)` when the
    // stream is empty (zero bytes read), so this single arm covers both
    // read_exact-based decoders (plain, gorilla) and varint-based decoders.
    matches!(err, TsFileError::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof)
}

fn decode_i64_stream(enc: TSEncoding, dt: TSDataType, bytes: &[u8]) -> Result<Vec<i64>> {
    let mut dec = Decoder::new(dt, enc)?;
    let mut cur = Cursor::new(bytes);
    let mut out = Vec::new();
    loop {
        match dec.decode_i64(&mut cur) {
            Ok(v) => out.push(v),
            Err(e) if is_unexpected_eof(&e) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

fn decode_value_column(
    enc: TSEncoding,
    dt: TSDataType,
    bytes: &[u8],
    expected_count: usize,
) -> Result<Column> {
    let mut dec = Decoder::new(dt, enc)?;
    let mut cur = Cursor::new(bytes);
    match dt {
        TSDataType::Boolean => {
            let mut v = Vec::with_capacity(expected_count);
            for _ in 0..expected_count { v.push(dec.decode_bool(&mut cur)?); }
            Ok(Column::Boolean { values: v, nulls: None })
        }
        TSDataType::Int32 => {
            let mut v = Vec::with_capacity(expected_count);
            for _ in 0..expected_count { v.push(dec.decode_i32(&mut cur)?); }
            Ok(Column::Int32 { values: v, nulls: None })
        }
        TSDataType::Int64 => {
            let mut v = Vec::with_capacity(expected_count);
            for _ in 0..expected_count { v.push(dec.decode_i64(&mut cur)?); }
            Ok(Column::Int64 { values: v, nulls: None })
        }
        TSDataType::Float => {
            let mut v = Vec::with_capacity(expected_count);
            for _ in 0..expected_count { v.push(dec.decode_f32(&mut cur)?); }
            Ok(Column::Float { values: v, nulls: None })
        }
        TSDataType::Double => {
            let mut v = Vec::with_capacity(expected_count);
            for _ in 0..expected_count { v.push(dec.decode_f64(&mut cur)?); }
            Ok(Column::Double { values: v, nulls: None })
        }
        TSDataType::Text | TSDataType::String => Err(TsFileError::Unsupported(format!(
            "{dt:?} regular chunk decode: 5a handles numeric types; text/string deferred"
        ))),
    }
}

// ---------------------------------------------------------------------------
// AlignedTimeChunkReader
// ---------------------------------------------------------------------------

/// Iterates time pages inside an aligned time chunk. Each call to
/// `next_time_page` decodes one PageHeader + compressed body and returns
/// the decoded timestamps.
///
/// C++ AlignedChunkReader reads the time chunk first, storing timestamps so
/// the paired value chunk reader can index into them. In Rust we yield one
/// page at a time — the caller (AlignedSeriesScan) pairs time and value pages
/// positionally, mirroring the writer's page boundary coordination.
pub struct AlignedTimeChunkReader {
    header: ChunkHeader,
    remaining: Cursor<Vec<u8>>,
    filter: Option<Arc<dyn Filter>>,
}

/// Iterates value pages inside an aligned value chunk. Each call to
/// `next_value_page` decodes one PageHeader + compressed body, given the
/// row count `n` from the paired time page. Returns a typed Column
/// carrying the null bitmap.
///
/// C++ AlignedChunkReader reads each value chunk in lock-step with the
/// time chunk. We replicate that lock-step via `AlignedSeriesScan`, which
/// calls `next_time_page` and then `next_value_page(n)` per column per
/// iteration round.
pub struct AlignedValueChunkReader {
    header: ChunkHeader,
    remaining: Cursor<Vec<u8>>,
}

impl AlignedTimeChunkReader {
    pub fn new(header: ChunkHeader, page_bytes: Vec<u8>, filter: Option<Arc<dyn Filter>>) -> Self {
        debug_assert!(header.is_time_chunk());
        Self { header, remaining: Cursor::new(page_bytes), filter }
    }

    pub fn has_more(&self) -> bool {
        (self.remaining.position() as usize) < self.remaining.get_ref().len()
    }

    /// Decode the next time page, applying page-level statistic filtering.
    /// Returns `Ok(None)` when the chunk is exhausted.
    pub fn next_time_page(&mut self) -> Result<Option<Vec<i64>>> {
        while self.has_more() {
            let has_stat = !self.header.is_single_page();
            let page_header = PageHeader::deserialize_from(
                &mut self.remaining,
                self.header.data_type,
                has_stat,
            )?;

            if let (Some(f), Some(stat)) =
                (self.filter.as_ref(), page_header.statistic.as_ref())
                && !f.satisfy_statistic(stat)
            {
                let mut skip = vec![0u8; page_header.compressed_size as usize];
                self.remaining.read_exact(&mut skip)?;
                continue;
            }

            let mut compressed = vec![0u8; page_header.compressed_size as usize];
            self.remaining.read_exact(&mut compressed)?;
            let compressor = Compressor::new(self.header.compression);
            let body = compressor
                .decompress(&compressed, page_header.uncompressed_size as usize)?;
            // Time-page body is the entire time stream — no var_u32 size prefix
            // and no row count. The whole decompressed body is the encoded time
            // stream (contrast with regular pages which prefix [time_data_size]).
            let times = decode_i64_stream(self.header.encoding, TSDataType::Int64, &body)?;
            return Ok(Some(times));
        }
        Ok(None)
    }
}

impl AlignedValueChunkReader {
    pub fn new(header: ChunkHeader, page_bytes: Vec<u8>) -> Self {
        debug_assert!(header.is_value_chunk());
        Self { header, remaining: Cursor::new(page_bytes) }
    }

    pub fn has_more(&self) -> bool {
        (self.remaining.position() as usize) < self.remaining.get_ref().len()
    }

    /// Decode the next value page, using `n` (row count from the paired
    /// time page) to size the null bitmap and the value slot count.
    /// Values at null positions are NOT encoded in the stream; the
    /// Column's `values` at null positions are filled with defaults and
    /// must be masked by the returned bitmap.
    pub fn next_value_page(&mut self, n: usize) -> Result<Option<Column>> {
        if !self.has_more() { return Ok(None); }
        let has_stat = !self.header.is_single_page();
        let page_header = PageHeader::deserialize_from(
            &mut self.remaining,
            self.header.data_type,
            has_stat,
        )?;
        let mut compressed = vec![0u8; page_header.compressed_size as usize];
        self.remaining.read_exact(&mut compressed)?;
        let compressor = Compressor::new(self.header.compression);
        let body = compressor
            .decompress(&compressed, page_header.uncompressed_size as usize)?;

        // Split body into [bitmap | value_bytes].
        // Bitmap layout: ceil(n / 8) bytes, LSB-first (bit r set = row r is null).
        let bitmap_bytes = n.div_ceil(8);
        if body.len() < bitmap_bytes {
            return Err(TsFileError::Corrupted(format!(
                "aligned value page too small for {n} rows: body={} bytes, need >= {bitmap_bytes}",
                body.len()
            )));
        }
        let bitmap = BitMap::from_bytes(n, body[..bitmap_bytes].to_vec());
        let value_bytes = &body[bitmap_bytes..];

        let col = decode_aligned_value_column(
            self.header.encoding,
            self.header.data_type,
            value_bytes,
            n,
            &bitmap,
        )?;
        Ok(Some(col))
    }
}

/// Decode `n` rows of an aligned value column, skipping null slots
/// and placing defaults at null positions so `Column.values.len() == n`.
///
/// C++ AlignedChunkReader::decodeValueChunk iterates the bitmap to decide
/// whether to decode or skip each slot. We replicate that logic: for each
/// row r, if the null bitmap marks it set we insert a typed zero placeholder
/// rather than reading from the stream. Callers must always consult the
/// bitmap before using a value.
fn decode_aligned_value_column(
    enc: TSEncoding,
    dt: TSDataType,
    bytes: &[u8],
    n: usize,
    nulls: &BitMap,
) -> Result<Column> {
    let mut dec = Decoder::new(dt, enc)?;
    let mut cur = Cursor::new(bytes);
    macro_rules! decode_typed {
        ($zero:expr, $decode:ident, $variant:ident) => {{
            let mut v = Vec::with_capacity(n);
            for r in 0..n {
                if nulls.get(r) {
                    v.push($zero);
                } else {
                    v.push(dec.$decode(&mut cur)?);
                }
            }
            Ok(Column::$variant { values: v, nulls: Some(nulls.clone()) })
        }};
    }
    match dt {
        TSDataType::Boolean => decode_typed!(false,   decode_bool, Boolean),
        TSDataType::Int32   => decode_typed!(0i32,    decode_i32,  Int32),
        TSDataType::Int64   => decode_typed!(0i64,    decode_i64,  Int64),
        TSDataType::Float   => decode_typed!(0.0f32,  decode_f32,  Float),
        TSDataType::Double  => decode_typed!(0.0f64,  decode_f64,  Double),
        TSDataType::Text | TSDataType::String => Err(TsFileError::Unsupported(format!(
            "{dt:?} aligned value column: 5a handles numeric types; text/string deferred"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::statistic::Statistic;
    use crate::tsfile_format::{CHUNK_HEADER_MARKER, ONLY_ONE_PAGE_CHUNK_HEADER_MARKER};
    use crate::types::{CompressionType, TSEncoding};
    use crate::writer::page_writer::PageWriter;
    use crate::reader::filter::time::TimeLt;
    use std::sync::Arc as StdArc;

    fn cm_int64(name: &str) -> Arc<[ColumnMeta]> {
        Arc::from(vec![ColumnMeta { name: name.into(), data_type: TSDataType::Int64 }])
    }

    /// Build a single-page regular chunk's `(ChunkHeader, page_bytes)`
    /// pair using the real writer primitives.
    fn build_single_page_int64(
        times: &[i64],
        values: &[i64],
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> (ChunkHeader, Vec<u8>) {
        let cfg = StdArc::new(Config::default());
        let mut pw = PageWriter::with_encoding(
            TSDataType::Int64,
            TSEncoding::Ts2Diff,       // time encoding: writer default
            encoding,
            compression,
            cfg,
        ).unwrap();
        for (t, v) in times.iter().zip(values.iter()) {
            pw.write_i64(*t, *v).unwrap();
        }
        let sealed = pw.seal().unwrap();

        // Single-page PageHeader omits statistic.
        let page_header = PageHeader::new(
            sealed.uncompressed_size as i32,
            sealed.compressed_data.len() as i32,
            None,
        );
        let mut page_bytes = Vec::new();
        page_header.serialize_to(&mut page_bytes).unwrap();
        page_bytes.extend_from_slice(&sealed.compressed_data);

        let chunk_header = ChunkHeader::new(
            ONLY_ONE_PAGE_CHUNK_HEADER_MARKER,
            "m".into(),
            page_bytes.len() as u32,
            TSDataType::Int64,
            encoding,
            compression,
        );
        (chunk_header, page_bytes)
    }

    /// Build a multi-page regular chunk (two pages) with per-page
    /// statistics embedded in each PageHeader.
    fn build_two_page_int64(
        times_a: &[i64], values_a: &[i64],
        times_b: &[i64], values_b: &[i64],
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> (ChunkHeader, Vec<u8>) {
        let cfg = StdArc::new(Config::default());
        let mut pw = PageWriter::with_encoding(
            TSDataType::Int64, TSEncoding::Ts2Diff, encoding, compression, cfg.clone(),
        ).unwrap();
        for (t, v) in times_a.iter().zip(values_a.iter()) {
            pw.write_i64(*t, *v).unwrap();
        }
        let sealed_a = pw.seal().unwrap();

        // Reuse the same PageWriter (matches writer behaviour).
        for (t, v) in times_b.iter().zip(values_b.iter()) {
            pw.write_i64(*t, *v).unwrap();
        }
        let sealed_b = pw.seal().unwrap();

        let mut page_bytes = Vec::new();
        for s in [&sealed_a, &sealed_b] {
            let ph = PageHeader::new(
                s.uncompressed_size as i32,
                s.compressed_data.len() as i32,
                Some(s.statistic.clone()),
            );
            ph.serialize_to(&mut page_bytes).unwrap();
            page_bytes.extend_from_slice(&s.compressed_data);
        }

        let chunk_header = ChunkHeader::new(
            CHUNK_HEADER_MARKER,
            "m".into(),
            page_bytes.len() as u32,
            TSDataType::Int64,
            encoding,
            compression,
        );
        (chunk_header, page_bytes)
    }

    // Expected: verify a single-page int64 chunk round-trips correctly.
    #[test]
    fn round_trip_int64_ts2diff_uncompressed_single_page() {
        let times: Vec<i64> = (0..10).collect();
        let values: Vec<i64> = (100..110).collect();
        let (header, page_bytes) = build_single_page_int64(
            &times, &values, TSEncoding::Ts2Diff, CompressionType::Uncompressed,
        );

        let mut reader = RegularChunkReader::new(header, page_bytes, cm_int64("m"), None);
        let block = reader.next_block().unwrap().unwrap();
        assert_eq!(block.num_rows(), 10);
        assert_eq!(block.times, times);
        match &block.columns[0] {
            Column::Int64 { values: v, nulls: None } => assert_eq!(v, &values),
            _ => panic!("wrong column variant"),
        }
        assert!(reader.next_block().unwrap().is_none());
    }

    #[test]
    fn round_trip_matrix_numeric_types_encodings_compressions() {
        // Exercise (type x encoding x compression) combinations we're
        // expected to support. Keep N small for test speed.
        let types_encodings: &[(TSDataType, TSEncoding)] = &[
            (TSDataType::Int32, TSEncoding::Plain),
            (TSDataType::Int32, TSEncoding::Ts2Diff),
            (TSDataType::Int32, TSEncoding::Gorilla),
            (TSDataType::Int32, TSEncoding::Rle),
            (TSDataType::Int32, TSEncoding::Zigzag),
            (TSDataType::Int32, TSEncoding::Sprintz),
            (TSDataType::Int64, TSEncoding::Plain),
            (TSDataType::Int64, TSEncoding::Ts2Diff),
            (TSDataType::Int64, TSEncoding::Gorilla),
            (TSDataType::Int64, TSEncoding::Rle),
            (TSDataType::Int64, TSEncoding::Zigzag),
            (TSDataType::Int64, TSEncoding::Sprintz),
            (TSDataType::Float, TSEncoding::Plain),
            (TSDataType::Float, TSEncoding::Ts2Diff),
            (TSDataType::Float, TSEncoding::Gorilla),
            (TSDataType::Float, TSEncoding::Sprintz),
            (TSDataType::Double, TSEncoding::Plain),
            (TSDataType::Double, TSEncoding::Ts2Diff),
            (TSDataType::Double, TSEncoding::Gorilla),
            (TSDataType::Double, TSEncoding::Sprintz),
        ];
        let compressions = &[
            CompressionType::Uncompressed,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
        ];

        for &(dt, enc) in types_encodings {
            for &comp in compressions {
                let (header, body) = build_numeric_single_page(dt, enc, comp, 50);
                let cm: Arc<[ColumnMeta]> = Arc::from(vec![ColumnMeta {
                    name: "m".into(), data_type: dt,
                }]);
                let mut reader = RegularChunkReader::new(header, body, cm, None);
                let block = reader.next_block().unwrap_or_else(|e|
                    panic!("decode failed for {dt:?}/{enc:?}/{comp:?}: {e}")
                ).unwrap();
                assert_eq!(block.num_rows(), 50, "{dt:?}/{enc:?}/{comp:?}");
                assert!(reader.next_block().unwrap().is_none());
            }
        }
    }

    fn build_numeric_single_page(
        dt: TSDataType,
        enc: TSEncoding,
        comp: CompressionType,
        n: usize,
    ) -> (ChunkHeader, Vec<u8>) {
        let cfg = StdArc::new(Config::default());
        let mut pw = PageWriter::with_encoding(
            dt, TSEncoding::Ts2Diff, enc, comp, cfg,
        ).unwrap();
        for i in 0..n {
            let t = i as i64;
            match dt {
                TSDataType::Int32  => pw.write_i32(t, i as i32).unwrap(),
                TSDataType::Int64  => pw.write_i64(t, i as i64).unwrap(),
                TSDataType::Float  => pw.write_f32(t, i as f32).unwrap(),
                TSDataType::Double => pw.write_f64(t, i as f64).unwrap(),
                _ => unreachable!(),
            }
        }
        let sealed = pw.seal().unwrap();
        let ph = PageHeader::new(
            sealed.uncompressed_size as i32,
            sealed.compressed_data.len() as i32,
            None,
        );
        let mut page_bytes = Vec::new();
        ph.serialize_to(&mut page_bytes).unwrap();
        page_bytes.extend_from_slice(&sealed.compressed_data);
        let ch = ChunkHeader::new(
            ONLY_ONE_PAGE_CHUNK_HEADER_MARKER,
            "m".into(),
            page_bytes.len() as u32,
            dt, enc, comp,
        );
        (ch, page_bytes)
    }

    #[test]
    fn multi_page_chunk_yields_all_pages_in_order() {
        let times_a: Vec<i64> = (0..5).collect();
        let values_a: Vec<i64> = (1000..1005).collect();
        let times_b: Vec<i64> = (10..15).collect();
        let values_b: Vec<i64> = (2000..2005).collect();
        let (header, body) = build_two_page_int64(
            &times_a, &values_a, &times_b, &values_b,
            TSEncoding::Ts2Diff, CompressionType::Uncompressed,
        );
        let mut reader = RegularChunkReader::new(header, body, cm_int64("m"), None);
        let block1 = reader.next_block().unwrap().unwrap();
        assert_eq!(block1.times, times_a);
        let block2 = reader.next_block().unwrap().unwrap();
        assert_eq!(block2.times, times_b);
        assert!(reader.next_block().unwrap().is_none());
    }

    #[test]
    fn page_statistic_filter_skips_pages() {
        let times_a: Vec<i64> = (0..5).collect();
        let values_a: Vec<i64> = (0..5).collect();
        let times_b: Vec<i64> = (100..105).collect();
        let values_b: Vec<i64> = (100..105).collect();
        let (header, body) = build_two_page_int64(
            &times_a, &values_a, &times_b, &values_b,
            TSEncoding::Ts2Diff, CompressionType::Uncompressed,
        );

        // TimeLt(0) rejects everything: both pages should be pruned.
        let f: Arc<dyn Filter> = Arc::new(TimeLt::new(0));
        let mut reader = RegularChunkReader::new(header, body, cm_int64("m"), Some(f));
        assert!(reader.next_block().unwrap().is_none());
    }

    // Silence a potential dead-code warning about Statistic import if
    // unused elsewhere.
    #[allow(dead_code)]
    fn _use_statistic(_s: Statistic) {}

    #[test]
    fn decode_i64_stream_empty_returns_empty() {
        let result = decode_i64_stream(TIME_ENCODING, TSDataType::Int64, &[]);
        assert_eq!(result.unwrap(), Vec::<i64>::new());
    }

    #[test]
    fn aligned_time_page_round_trip() {
        use crate::writer::time_page_writer::TimePageWriter;
        use crate::tsfile_format::ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER;

        let cfg = StdArc::new(Config::default());
        let mut pw = TimePageWriter::new(
            TSEncoding::Ts2Diff,
            CompressionType::Uncompressed,
            cfg,
        ).unwrap();
        for t in 0..30i64 { pw.write(t).unwrap(); }
        let sealed = pw.seal().unwrap();

        // Build a single-page time chunk (PageHeader omits stat).
        let ph = PageHeader::new(
            sealed.uncompressed_size as i32,
            sealed.compressed_data.len() as i32,
            None,
        );
        let mut page_bytes = Vec::new();
        ph.serialize_to(&mut page_bytes).unwrap();
        page_bytes.extend_from_slice(&sealed.compressed_data);

        let header = ChunkHeader::new(
            ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER,
            String::new(),
            page_bytes.len() as u32,
            TSDataType::Int64,
            TSEncoding::Ts2Diff,
            CompressionType::Uncompressed,
        );
        let mut r = AlignedTimeChunkReader::new(header, page_bytes, None);
        let times = r.next_time_page().unwrap().unwrap();
        assert_eq!(times, (0..30).collect::<Vec<_>>());
        assert!(r.next_time_page().unwrap().is_none());
    }

    #[test]
    fn aligned_value_page_with_nulls_round_trip() {
        use crate::writer::value_page_writer::ValuePageWriter;
        use crate::tsfile_format::ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER;

        let cfg = StdArc::new(Config::default());
        let mut pw = ValuePageWriter::new(
            TSDataType::Int64,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            cfg,
        ).unwrap();
        // 10 rows: every 3rd row is null (rows 0, 3, 6, 9).
        for t in 0..10i64 {
            if t % 3 == 0 { pw.write_null(); }
            else          { pw.write_i64(t, t * 100).unwrap(); }
        }
        let sealed = pw.seal().unwrap();
        let ph = PageHeader::new(
            sealed.uncompressed_size as i32,
            sealed.compressed_data.len() as i32,
            None,
        );
        let mut page_bytes = Vec::new();
        ph.serialize_to(&mut page_bytes).unwrap();
        page_bytes.extend_from_slice(&sealed.compressed_data);

        let header = ChunkHeader::new(
            ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER,
            "m".into(),
            page_bytes.len() as u32,
            TSDataType::Int64,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        );
        let mut r = AlignedValueChunkReader::new(header, page_bytes);
        let col = r.next_value_page(10).unwrap().unwrap();
        match col {
            Column::Int64 { values, nulls: Some(bm) } => {
                assert_eq!(values.len(), 10);
                for (i, &v) in values.iter().enumerate() {
                    if i % 3 == 0 {
                        assert!(bm.get(i), "row {i} should be null");
                    } else {
                        assert!(!bm.get(i), "row {i} should be present");
                        assert_eq!(v, (i as i64) * 100);
                    }
                }
            }
            _ => panic!("wrong column variant"),
        }
    }
}
