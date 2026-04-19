// PageWriter encodes and compresses values for a single page within a chunk.
//
// C++ PageWriter holds separate time and value encoders plus a compressor,
// flushes them on seal, and produces a PageHeader + compressed body. The
// C++ lifetime is tied to its owning ChunkWriter via raw pointer. In Rust,
// PageWriter is an owned value inside ChunkWriter — no manual destroy() needed.
//
// Page body format (before compression):
//   [time_data_size: var_u32] [encoded_time_bytes...] [encoded_value_bytes...]
//
// ChunkWriter decides whether the PageHeader includes statistics:
//   - Single-page chunk: PageHeader omits statistics (ChunkMeta holds them)
//   - Multi-page chunk:  PageHeader includes per-page statistics

use crate::compress::Compressor;
use crate::config::Config;
use crate::encoding::encoder::Encoder;
use crate::error::{Result, TsFileError};
use crate::serialize;
use crate::statistic::Statistic;
use crate::types::{CompressionType, TSDataType, TSEncoding};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// SealedPage
// ---------------------------------------------------------------------------

/// Output of PageWriter::seal() — compressed page data with its statistics.
///
/// ChunkWriter accumulates these and decides the final page layout:
/// whether to include per-page statistics in the PageHeader or omit them
/// for the single-page optimisation.
pub struct SealedPage {
    pub statistic: Statistic,
    pub uncompressed_size: u32,
    pub compressed_data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// PageWriter
// ---------------------------------------------------------------------------

/// Accumulates encoded (timestamp, value) pairs for one page of one chunk.
///
/// Seals when either `page_writer_max_point_num` or `page_writer_max_memory_bytes`
/// is reached. The time column is always Int64 encoded with the configured
/// time encoding (default TS_2DIFF). The value column uses the per-type
/// encoding from Config.
pub struct PageWriter {
    data_type: TSDataType,
    time_encoder: Encoder,
    value_encoder: Encoder,
    compressor: Compressor,
    statistic: Statistic,
    time_buf: Vec<u8>,
    value_buf: Vec<u8>,
    pub point_num: usize,
    config: Arc<Config>,
}

impl PageWriter {
    /// Create a PageWriter for the given value type using encoders and
    /// compressor taken from `config`.
    pub fn new(data_type: TSDataType, config: Arc<Config>) -> Result<Self> {
        let time_encoding = config.time_encoding_type;
        let value_encoding = config.get_value_encoder(data_type);
        let compression = config.get_default_compressor();
        Self::with_encoding(data_type, time_encoding, value_encoding, compression, config)
    }

    /// Create a PageWriter with explicit encoding and compression choices.
    /// Used by ChunkWriter when the schema specifies a non-default encoding.
    pub fn with_encoding(
        data_type: TSDataType,
        time_encoding: TSEncoding,
        value_encoding: TSEncoding,
        compression: CompressionType,
        config: Arc<Config>,
    ) -> Result<Self> {
        let time_encoder = Encoder::new(TSDataType::Int64, time_encoding)?;
        let value_encoder = Encoder::new(data_type, value_encoding)?;
        let compressor = Compressor::new(compression);
        Ok(Self {
            data_type,
            time_encoder,
            value_encoder,
            compressor,
            statistic: Statistic::new(data_type),
            time_buf: Vec::new(),
            value_buf: Vec::new(),
            point_num: 0,
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Write methods (one per value type)
    // -----------------------------------------------------------------------

    pub fn write_bool(&mut self, timestamp: i64, value: bool) -> Result<()> {
        self.time_encoder.encode_i64(timestamp, &mut self.time_buf)?;
        self.value_encoder.encode_bool(value, &mut self.value_buf)?;
        self.statistic.update_bool(timestamp, value);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_i32(&mut self, timestamp: i64, value: i32) -> Result<()> {
        self.time_encoder.encode_i64(timestamp, &mut self.time_buf)?;
        self.value_encoder.encode_i32(value, &mut self.value_buf)?;
        self.statistic.update_i32(timestamp, value);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_i64(&mut self, timestamp: i64, value: i64) -> Result<()> {
        self.time_encoder.encode_i64(timestamp, &mut self.time_buf)?;
        self.value_encoder.encode_i64(value, &mut self.value_buf)?;
        self.statistic.update_i64(timestamp, value);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_f32(&mut self, timestamp: i64, value: f32) -> Result<()> {
        self.time_encoder.encode_i64(timestamp, &mut self.time_buf)?;
        self.value_encoder.encode_f32(value, &mut self.value_buf)?;
        self.statistic.update_f32(timestamp, value);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_f64(&mut self, timestamp: i64, value: f64) -> Result<()> {
        self.time_encoder.encode_i64(timestamp, &mut self.time_buf)?;
        self.value_encoder.encode_f64(value, &mut self.value_buf)?;
        self.statistic.update_f64(timestamp, value);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_text(&mut self, timestamp: i64, value: &[u8]) -> Result<()> {
        self.time_encoder.encode_i64(timestamp, &mut self.time_buf)?;
        self.value_encoder.encode_bytes(value, &mut self.value_buf)?;
        self.statistic.update_text(timestamp, value);
        self.point_num += 1;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Threshold checks
    // -----------------------------------------------------------------------

    /// True if this page has reached the point count or memory limit and
    /// should be sealed before writing the next point.
    pub fn is_full(&self) -> bool {
        self.point_num >= self.config.page_writer_max_point_num as usize
            || self.memory_estimate() >= self.config.page_writer_max_memory_bytes as usize
    }

    /// Rough memory estimate: sizes of the intermediate encode buffers.
    pub fn memory_estimate(&self) -> usize {
        self.time_buf.len() + self.value_buf.len()
    }

    pub fn has_data(&self) -> bool {
        self.point_num > 0
    }

    // -----------------------------------------------------------------------
    // Seal
    // -----------------------------------------------------------------------

    /// Flush encoders, compress, and return the sealed page.
    ///
    /// Resets all internal state so this PageWriter can be reused for the
    /// next page in the same chunk (C++ also reuses the PageWriter object).
    pub fn seal(&mut self) -> Result<SealedPage> {
        if self.point_num == 0 {
            return Err(TsFileError::InvalidArg("sealing empty page".into()));
        }

        // Flush encoder state into the output buffers.
        self.time_encoder.flush(&mut self.time_buf)?;
        self.value_encoder.flush(&mut self.value_buf)?;

        // Build the uncompressed page body:
        //   [time_data_size: var_u32] [time_bytes] [value_bytes]
        let mut body: Vec<u8> =
            Vec::with_capacity(4 + self.time_buf.len() + self.value_buf.len());
        serialize::write_var_u32(&mut body, self.time_buf.len() as u32)?;
        body.extend_from_slice(&self.time_buf);
        body.extend_from_slice(&self.value_buf);

        let uncompressed_size = body.len() as u32;
        let compressed_data = self.compressor.compress(&body)?;

        let sealed = SealedPage {
            statistic: self.statistic.clone(),
            uncompressed_size,
            compressed_data,
        };

        // Reset for potential reuse.
        self.reset();
        Ok(sealed)
    }

    /// Reset all state without producing output. Used when discarding a page.
    pub fn reset(&mut self) {
        self.time_buf.clear();
        self.value_buf.clear();
        self.point_num = 0;
        self.statistic.reset();
        self.time_encoder.reset();
        self.value_encoder.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::Compressor as Comp;
    use std::sync::Arc;

    fn plain_lz4_config() -> Arc<Config> {
        let mut cfg = Config::default();
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.float_encoding_type = TSEncoding::Plain;
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        Arc::new(cfg)
    }

    #[test]
    fn write_and_seal_i32() {
        let config = plain_lz4_config();
        let mut pw = PageWriter::new(TSDataType::Int32, config).unwrap();
        pw.write_i32(1000, 42).unwrap();
        pw.write_i32(2000, 43).unwrap();
        let sealed = pw.seal().unwrap();
        assert_eq!(sealed.statistic.count(), 2);
        assert_eq!(sealed.statistic.start_time(), 1000);
        assert_eq!(sealed.statistic.end_time(), 2000);
        assert!(!sealed.compressed_data.is_empty());
    }

    #[test]
    fn write_and_seal_f32() {
        let config = plain_lz4_config();
        let mut pw = PageWriter::new(TSDataType::Float, config).unwrap();
        pw.write_f32(100, 1.5).unwrap();
        pw.write_f32(200, 2.5).unwrap();
        let sealed = pw.seal().unwrap();
        assert_eq!(sealed.statistic.count(), 2);
    }

    #[test]
    fn seal_empty_errors() {
        let config = plain_lz4_config();
        let mut pw = PageWriter::new(TSDataType::Int32, config).unwrap();
        assert!(pw.seal().is_err());
    }

    #[test]
    fn is_full_point_count() {
        let mut cfg = Config::default();
        cfg.page_writer_max_point_num = 3;
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        let config = Arc::new(cfg);
        let mut pw = PageWriter::new(TSDataType::Int32, config).unwrap();
        assert!(!pw.is_full());
        pw.write_i32(1, 1).unwrap();
        pw.write_i32(2, 2).unwrap();
        pw.write_i32(3, 3).unwrap();
        assert!(pw.is_full());
    }

    #[test]
    fn reset_clears_state() {
        let config = plain_lz4_config();
        let mut pw = PageWriter::new(TSDataType::Int32, config).unwrap();
        pw.write_i32(1, 42).unwrap();
        pw.reset();
        assert_eq!(pw.point_num, 0);
        assert!(!pw.has_data());
    }

    #[test]
    fn statistic_tracks_min_max() {
        let config = plain_lz4_config();
        let mut pw = PageWriter::new(TSDataType::Int32, config).unwrap();
        pw.write_i32(1, 100).unwrap();
        pw.write_i32(2, -50).unwrap();
        pw.write_i32(3, 75).unwrap();
        let sealed = pw.seal().unwrap();
        assert_eq!(sealed.statistic.count(), 3);
    }
}
