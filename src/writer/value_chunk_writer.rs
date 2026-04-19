// ValueChunkWriter aggregates value pages for one measurement in an aligned
// chunk group.
//
// C++ ValueChunkWriter is the aligned equivalent of ChunkWriter for the value
// column. It uses VALUE_CHUNK_HEADER_MARKER (6) or its single-page variant.
// The measurement_id identifies which timeseries this value chunk belongs to;
// the corresponding time chunk (measurement_id = "") provides the timestamps.

use crate::config::Config;
use crate::error::{Result, TsFileError};
use crate::io::io_writer::TsFileIOWriter;
use crate::statistic::Statistic;
use crate::tsfile_format::{
    ChunkHeader, PageHeader, ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER, VALUE_CHUNK_HEADER_MARKER,
};
use crate::types::{CompressionType, TSDataType, TSEncoding};
use crate::value::TsValue;
use crate::writer::value_page_writer::{SealedValuePage, ValuePageWriter};
use std::sync::Arc;

/// Writes one value column for an aligned timeseries.
///
/// Null values (absent measurements at a given timestamp) are written via
/// `write_null()` so the value page's null bitmap stays aligned with the
/// shared time column written by `TimeChunkWriter`.
pub struct ValueChunkWriter {
    pub measurement_name: String,
    pub data_type: TSDataType,
    encoding: TSEncoding,
    compression: CompressionType,
    page_writer: ValuePageWriter,
    sealed_pages: Vec<SealedValuePage>,
    chunk_statistic: Statistic,
    config: Arc<Config>,
}

impl ValueChunkWriter {
    pub fn new(
        measurement_name: String,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
        config: Arc<Config>,
    ) -> Result<Self> {
        let page_writer = ValuePageWriter::new(data_type, encoding, compression, config.clone())?;
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

    /// Write a null slot — row exists in the time column but has no value here.
    pub fn write_null(&mut self) -> Result<()> {
        self.page_writer.write_null();
        self.maybe_seal_page()
    }

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

    /// Write an optional typed value. None → null slot.
    pub fn write_option(&mut self, timestamp: i64, value: Option<&TsValue>) -> Result<()> {
        match value {
            None => self.write_null(),
            Some(v) => self.write_value(timestamp, v),
        }
    }

    /// Write a dynamically-typed non-null value.
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
    // State
    // -----------------------------------------------------------------------

    pub fn has_data(&self) -> bool {
        self.page_writer.has_data() || !self.sealed_pages.is_empty()
    }

    pub fn memory_estimate(&self) -> usize {
        self.page_writer.memory_estimate()
            + self
                .sealed_pages
                .iter()
                .map(|p| p.compressed_data.len())
                .sum::<usize>()
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Seal any in-progress page and write the complete value chunk to `io_writer`.
    pub fn flush_to(&mut self, io_writer: &mut TsFileIOWriter) -> Result<()> {
        if self.page_writer.has_data() {
            let sealed = self.page_writer.seal()?;
            self.push_sealed_page(sealed);
        }

        if self.sealed_pages.is_empty() {
            return Ok(());
        }

        let is_single = self.sealed_pages.len() == 1;
        let marker = if is_single {
            ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER
        } else {
            VALUE_CHUNK_HEADER_MARKER
        };

        let mut page_data: Vec<u8> = Vec::new();
        for page in &self.sealed_pages {
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

    fn maybe_seal_page(&mut self) -> Result<()> {
        if self.page_writer.is_full() {
            let sealed = self.page_writer.seal()?;
            self.push_sealed_page(sealed);
        }
        Ok(())
    }

    fn push_sealed_page(&mut self, page: SealedValuePage) {
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
    use crate::device_id::DeviceId;
    use tempfile::tempdir;

    fn make_config() -> Arc<Config> {
        let mut cfg = Config::default();
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        Arc::new(cfg)
    }

    #[test]
    fn write_and_flush_value_chunk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("val_chunk.tsfile");
        let config = make_config();
        let mut io_w = TsFileIOWriter::new(&path, config.clone()).unwrap();
        let dev = DeviceId::parse("d1").unwrap();
        io_w.start_chunk_group(&dev).unwrap();

        let mut vcw = ValueChunkWriter::new(
            "s1".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();

        vcw.write_i32(1000, 42).unwrap();
        vcw.write_null().unwrap();
        vcw.write_i32(3000, 44).unwrap();

        vcw.flush_to(&mut io_w).unwrap();
        assert!(!vcw.has_data());

        io_w.end_chunk_group().unwrap();
        io_w.end_file().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn type_mismatch_errors() {
        let config = make_config();
        let mut vcw = ValueChunkWriter::new(
            "s1".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        assert!(vcw.write_value(1, &TsValue::Float(1.5)).is_err());
    }

    #[test]
    fn flush_empty_is_noop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.tsfile");
        let config = make_config();
        let mut io_w = TsFileIOWriter::new(&path, config.clone()).unwrap();
        let dev = DeviceId::parse("d1").unwrap();
        io_w.start_chunk_group(&dev).unwrap();

        let mut vcw = ValueChunkWriter::new(
            "s1".into(),
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        vcw.flush_to(&mut io_w).unwrap();

        io_w.end_chunk_group().unwrap();
        io_w.end_file().unwrap();
    }
}
