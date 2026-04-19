// TimeChunkWriter aggregates time pages for the shared time column of an
// aligned timeseries group.
//
// C++ TimeChunkWriter is the aligned equivalent of ChunkWriter — it writes a
// "time chunk" (marker = TIME_CHUNK_HEADER_MARKER or
// ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER) that contains only timestamps.
// The reader uses the time chunk to decode row indices for aligned value chunks.
//
// The single-page optimisation applies here too: one page → statistics in
// ChunkMeta, not in PageHeader. Multiple pages → each PageHeader carries stats.

use crate::config::Config;
use crate::error::{Result, TsFileError};
use crate::io::io_writer::TsFileIOWriter;
use crate::statistic::Statistic;
use crate::tsfile_format::{
    ChunkHeader, PageHeader, ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER, TIME_CHUNK_HEADER_MARKER,
};
use crate::types::{CompressionType, TSDataType, TSEncoding};
use crate::writer::time_page_writer::{SealedTimePage, TimePageWriter};
use std::sync::Arc;

/// Writes the time column for one aligned chunk group.
///
/// Every write call appends one timestamp. Pages are sealed automatically
/// when they reach the configured threshold. Call `flush_to()` to write
/// the complete time chunk to disk.
pub struct TimeChunkWriter {
    encoding: TSEncoding,
    compression: CompressionType,
    page_writer: TimePageWriter,
    sealed_pages: Vec<SealedTimePage>,
    chunk_statistic: Statistic,
    config: Arc<Config>,
}

impl TimeChunkWriter {
    pub fn new(
        encoding: TSEncoding,
        compression: CompressionType,
        config: Arc<Config>,
    ) -> Result<Self> {
        let page_writer = TimePageWriter::new(encoding, compression, config.clone())?;
        Ok(Self {
            encoding,
            compression,
            page_writer,
            sealed_pages: Vec::new(),
            chunk_statistic: Statistic::new(TSDataType::Int64),
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    pub fn write(&mut self, timestamp: i64) -> Result<()> {
        self.page_writer.write(timestamp)?;
        self.maybe_seal_page()
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

    /// Seal any in-progress page and write the complete time chunk to `io_writer`.
    ///
    /// The time chunk uses measurement_id = "" (empty string) matching the
    /// C++ convention for aligned time columns — the reader identifies time
    /// chunks by their marker byte, not by measurement name.
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
            ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER
        } else {
            TIME_CHUNK_HEADER_MARKER
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

        // Time chunk measurement_id is "" (empty) per the aligned format spec.
        let chunk_header = ChunkHeader::new(
            marker,
            String::new(),
            page_data.len() as u32,
            TSDataType::Int64,
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

    fn push_sealed_page(&mut self, page: SealedTimePage) {
        self.chunk_statistic.merge(&page.statistic);
        self.sealed_pages.push(page);
    }

    fn reset_after_flush(&mut self) {
        self.sealed_pages.clear();
        self.chunk_statistic = Statistic::new(TSDataType::Int64);
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
        cfg.default_compression_type = CompressionType::Uncompressed;
        Arc::new(cfg)
    }

    #[test]
    fn single_page_time_chunk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("time_chunk.tsfile");
        let config = make_config();
        let mut io_w = TsFileIOWriter::new(&path, config.clone()).unwrap();
        let dev = DeviceId::parse("d1").unwrap();
        io_w.start_chunk_group(&dev).unwrap();

        let mut cw = TimeChunkWriter::new(
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();

        cw.write(1000).unwrap();
        cw.write(2000).unwrap();
        assert!(cw.has_data());

        cw.flush_to(&mut io_w).unwrap();
        assert!(!cw.has_data());

        io_w.end_chunk_group().unwrap();
        io_w.end_file().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn flush_empty_is_noop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.tsfile");
        let config = make_config();
        let mut io_w = TsFileIOWriter::new(&path, config.clone()).unwrap();
        let dev = DeviceId::parse("d1").unwrap();
        io_w.start_chunk_group(&dev).unwrap();

        let mut cw = TimeChunkWriter::new(TSEncoding::Plain, CompressionType::Uncompressed, config).unwrap();
        cw.flush_to(&mut io_w).unwrap(); // no-op

        io_w.end_chunk_group().unwrap();
        io_w.end_file().unwrap();
    }
}
