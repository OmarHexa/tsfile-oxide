// ChunkWriter aggregates pages for one measurement and flushes the complete
// chunk to TsFileIOWriter.
//
// C++ ChunkWriter holds a PageWriter* and raw chunk_data bytes. It flushes
// by writing the ChunkHeader + all accumulated page bytes to ByteStream.
// In Rust, ChunkWriter owns the PageWriter by value; SealedPage collects
// compressed page data to compute the exact data_size for the ChunkHeader
// before any bytes go to the file.
//
// Single-page optimisation: when a chunk ends up with exactly one page, we
// use ONLY_ONE_PAGE_CHUNK_HEADER_MARKER and omit the per-page statistic from
// the PageHeader (the ChunkMeta holds the statistic instead). For multi-page
// chunks, each PageHeader carries its own statistic so the reader can skip
// individual pages based on time/value filters.

use crate::config::Config;
use crate::error::{Result, TsFileError};
use crate::io::io_writer::TsFileIOWriter;
use crate::statistic::Statistic;
use crate::tsfile_format::{
    CHUNK_HEADER_MARKER, ChunkHeader, ONLY_ONE_PAGE_CHUNK_HEADER_MARKER, PageHeader,
};
use crate::types::{CompressionType, TSDataType, TSEncoding};
use crate::value::TsValue;
use crate::writer::page_writer::{PageWriter, SealedPage};
use std::sync::Arc;

/// Writes all pages for one measurement column.
///
/// After the last point is written, call `flush_to()` to push the complete
/// chunk (header + page bytes) through `TsFileIOWriter`.
pub struct ChunkWriter {
    pub measurement_name: String,
    pub data_type: TSDataType,
    encoding: TSEncoding,
    compression: CompressionType,
    /// Active page writer — accumulates points until `is_full()`.
    page_writer: PageWriter,
    /// Pages already sealed (encoder flushed + compressed), waiting for flush.
    sealed_pages: Vec<SealedPage>,
    /// Merged statistics across all sealed pages; updated incrementally.
    chunk_statistic: Statistic,
    config: Arc<Config>,
}

impl ChunkWriter {
    /// Create a ChunkWriter for `measurement_name` using the schema-specified
    /// encoding and compression.
    pub fn new(
        measurement_name: String,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
        config: Arc<Config>,
    ) -> Result<Self> {
        let page_writer = PageWriter::with_encoding(
            data_type,
            config.time_encoding_type,
            encoding,
            compression,
            config.clone(),
        )?;
        Ok(Self {
            measurement_name,
            data_type,
            encoding,
            compression,
            page_writer,
            sealed_pages: Vec::new(),
            chunk_statistic: Statistic::new(data_type),
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Write methods
    // -----------------------------------------------------------------------

    pub fn write_bool(&mut self, timestamp: i64, value: bool) -> Result<()> {
        self.page_writer.write_bool(timestamp, value)?;
        self.maybe_seal_page()
    }

    pub fn write_i32(&mut self, timestamp: i64, value: i32) -> Result<()> {
        self.page_writer.write_i32(timestamp, value)?;
        self.maybe_seal_page()
    }

    pub fn write_i64(&mut self, timestamp: i64, value: i64) -> Result<()> {
        self.page_writer.write_i64(timestamp, value)?;
        self.maybe_seal_page()
    }

    pub fn write_f32(&mut self, timestamp: i64, value: f32) -> Result<()> {
        self.page_writer.write_f32(timestamp, value)?;
        self.maybe_seal_page()
    }

    pub fn write_f64(&mut self, timestamp: i64, value: f64) -> Result<()> {
        self.page_writer.write_f64(timestamp, value)?;
        self.maybe_seal_page()
    }

    pub fn write_text(&mut self, timestamp: i64, value: &[u8]) -> Result<()> {
        self.page_writer.write_text(timestamp, value)?;
        self.maybe_seal_page()
    }

    /// Write a dynamically-typed value. Used at API boundaries (TsFileWriter)
    /// where the type is only known at runtime.
    pub fn write_value(&mut self, timestamp: i64, value: &TsValue) -> Result<()> {
        match (self.data_type, value) {
            (TSDataType::Boolean, TsValue::Boolean(v)) => self.write_bool(timestamp, *v),
            (TSDataType::Int32, TsValue::Int32(v)) => self.write_i32(timestamp, *v),
            (TSDataType::Int64, TsValue::Int64(v)) => self.write_i64(timestamp, *v),
            (TSDataType::Float, TsValue::Float(v)) => self.write_f32(timestamp, *v),
            (TSDataType::Double, TsValue::Double(v)) => self.write_f64(timestamp, *v),
            (TSDataType::Text, TsValue::Text(v)) => self.write_text(timestamp, v),
            (TSDataType::Text, TsValue::String(v)) => self.write_text(timestamp, v.as_bytes()),
            _ => Err(TsFileError::TypeMismatch {
                expected: self.data_type,
                actual: value.data_type(),
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Memory / state
    // -----------------------------------------------------------------------

    /// Approximate in-memory size: active page buffers + compressed sealed pages.
    pub fn memory_estimate(&self) -> usize {
        self.page_writer.memory_estimate()
            + self
                .sealed_pages
                .iter()
                .map(|p| p.compressed_data.len())
                .sum::<usize>()
    }

    pub fn has_data(&self) -> bool {
        self.page_writer.has_data() || !self.sealed_pages.is_empty()
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Seal any in-progress page and write the complete chunk to `io_writer`.
    ///
    /// After this call the ChunkWriter is reset and ready for reuse.
    pub fn flush_to(&mut self, io_writer: &mut TsFileIOWriter) -> Result<()> {
        // Seal the active page if it has data.
        if self.page_writer.has_data() {
            let sealed = self.page_writer.seal()?;
            self.push_sealed_page(sealed);
        }

        if self.sealed_pages.is_empty() {
            return Ok(());
        }

        let is_single = self.sealed_pages.len() == 1;
        let marker = if is_single {
            ONLY_ONE_PAGE_CHUNK_HEADER_MARKER
        } else {
            CHUNK_HEADER_MARKER
        };

        // Serialize all pages into a temporary buffer to compute data_size.
        let mut page_data: Vec<u8> = Vec::new();
        for page in &self.sealed_pages {
            // Statistics in PageHeader: omit for single-page, include for multi.
            let stat = if is_single {
                None
            } else {
                Some(page.statistic.clone())
            };
            let header = PageHeader::new(
                page.uncompressed_size as i32,
                page.compressed_data.len() as i32,
                stat,
            );
            header.serialize_to(&mut page_data)?;
            page_data.extend_from_slice(&page.compressed_data);
        }

        let chunk_header = ChunkHeader::new(
            marker,
            self.measurement_name.clone(),
            page_data.len() as u32,
            self.data_type,
            self.encoding,
            self.compression,
        );

        io_writer.start_chunk(&chunk_header)?;
        io_writer.write_page_data(&page_data)?;
        io_writer.end_chunk(&self.chunk_statistic)?;

        self.reset_after_flush();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /// Seal the active page if it has reached its threshold.
    fn maybe_seal_page(&mut self) -> Result<()> {
        if self.page_writer.is_full() {
            let sealed = self.page_writer.seal()?;
            self.push_sealed_page(sealed);
        }
        Ok(())
    }

    fn push_sealed_page(&mut self, page: SealedPage) {
        self.chunk_statistic.merge(&page.statistic);
        self.sealed_pages.push(page);
    }

    fn reset_after_flush(&mut self) {
        self.sealed_pages.clear();
        self.chunk_statistic = Statistic::new(self.data_type);
        self.page_writer.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::io_writer::TsFileIOWriter;
    use tempfile::tempdir;

    fn lz4_config() -> Arc<Config> {
        let mut cfg = Config::default();
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.float_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        Arc::new(cfg)
    }

    fn make_writer(path: &std::path::Path) -> TsFileIOWriter {
        TsFileIOWriter::new(path, Arc::new(Config::default())).unwrap()
    }

    #[test]
    fn single_page_chunk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.tsfile");
        let mut io_w = make_writer(&path);
        let dev = crate::device_id::DeviceId::parse("d1").unwrap();
        io_w.start_chunk_group(&dev).unwrap();

        let config = lz4_config();
        let mut cw = ChunkWriter::new(
            "s1".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();

        cw.write_i32(1000, 42).unwrap();
        cw.write_i32(2000, 43).unwrap();
        assert!(cw.has_data());

        cw.flush_to(&mut io_w).unwrap();
        assert!(!cw.has_data());

        io_w.end_chunk_group().unwrap();
        io_w.end_file().unwrap();
    }

    #[test]
    fn multi_page_chunk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.tsfile");
        let mut cfg = Config::default();
        cfg.page_writer_max_point_num = 2;
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        let config = Arc::new(cfg);

        let mut io_w = TsFileIOWriter::new(&path, config.clone()).unwrap();
        let dev = crate::device_id::DeviceId::parse("d1").unwrap();
        io_w.start_chunk_group(&dev).unwrap();

        let mut cw = ChunkWriter::new(
            "s1".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();

        // 3 points with max 2 per page → forces a page seal after 2nd point
        cw.write_i32(1, 10).unwrap();
        cw.write_i32(2, 20).unwrap();
        cw.write_i32(3, 30).unwrap();

        cw.flush_to(&mut io_w).unwrap();
        io_w.end_chunk_group().unwrap();
        io_w.end_file().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn write_value_type_mismatch() {
        let config = lz4_config();
        let mut cw = ChunkWriter::new(
            "s1".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        let v = TsValue::Float(1.5);
        assert!(cw.write_value(1, &v).is_err());
    }

    #[test]
    fn flush_empty_is_noop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.tsfile");
        let config = lz4_config();
        let mut io_w = TsFileIOWriter::new(&path, config.clone()).unwrap();
        let dev = crate::device_id::DeviceId::parse("d1").unwrap();
        io_w.start_chunk_group(&dev).unwrap();

        let mut cw = ChunkWriter::new(
            "s1".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        // flushing with no data is a no-op — should not error
        cw.flush_to(&mut io_w).unwrap();

        io_w.end_chunk_group().unwrap();
        io_w.end_file().unwrap();
    }
}
