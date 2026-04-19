// TimePageWriter encodes only the time column for aligned timeseries.
//
// C++ AlignedChunkGroupWriter has a TimeChunkWriter that owns a
// TimePageWriter* which encodes timestamps with no value column.
// In Rust, TimePageWriter is an owned value with no raw pointers.
//
// Aligned page body format (before compression):
//   [encoded_time_bytes...]
//
// Note: unlike PageWriter (non-aligned), there is NO leading var_u32 for the
// time section size — the whole body IS the time column. The decompressed
// body length is already encoded in the PageHeader.uncompressed_size.

use crate::compress::Compressor;
use crate::config::Config;
use crate::encoding::encoder::Encoder;
use crate::error::{Result, TsFileError};
use crate::statistic::Statistic;
use crate::types::{CompressionType, TSDataType, TSEncoding};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// SealedTimePage
// ---------------------------------------------------------------------------

/// Output of TimePageWriter::seal() — compressed time page data.
pub struct SealedTimePage {
    pub statistic: Statistic,
    pub uncompressed_size: u32,
    pub compressed_data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// TimePageWriter
// ---------------------------------------------------------------------------

/// Encodes and compresses the time column for one aligned page.
///
/// C++ TimePageWriter holds only a time encoder + compressor. In Rust
/// we own all state by value. The statistic is Int64-typed because
/// timestamps are i64 — it tracks start_time, end_time, and count, which
/// is all the reader needs to apply time-range filters on aligned chunks.
pub struct TimePageWriter {
    time_encoder: Encoder,
    compressor: Compressor,
    /// Int64 statistic tracks start/end time and count for this page.
    statistic: Statistic,
    time_buf: Vec<u8>,
    pub point_num: usize,
    config: Arc<Config>,
}

impl TimePageWriter {
    pub fn new(time_encoding: TSEncoding, compression: CompressionType, config: Arc<Config>) -> Result<Self> {
        let time_encoder = Encoder::new(TSDataType::Int64, time_encoding)?;
        let compressor = Compressor::new(compression);
        Ok(Self {
            time_encoder,
            compressor,
            statistic: Statistic::new(TSDataType::Int64),
            time_buf: Vec::new(),
            point_num: 0,
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /// Encode one timestamp into the page buffer.
    pub fn write(&mut self, timestamp: i64) -> Result<()> {
        self.time_encoder.encode_i64(timestamp, &mut self.time_buf)?;
        self.statistic.update_i64(timestamp, timestamp);
        self.point_num += 1;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Threshold checks
    // -----------------------------------------------------------------------

    pub fn is_full(&self) -> bool {
        self.point_num >= self.config.page_writer_max_point_num as usize
            || self.time_buf.len() >= self.config.page_writer_max_memory_bytes as usize
    }

    pub fn memory_estimate(&self) -> usize {
        self.time_buf.len()
    }

    pub fn has_data(&self) -> bool {
        self.point_num > 0
    }

    // -----------------------------------------------------------------------
    // Seal
    // -----------------------------------------------------------------------

    /// Flush encoder, compress, and return the sealed time page.
    pub fn seal(&mut self) -> Result<SealedTimePage> {
        if self.point_num == 0 {
            return Err(TsFileError::InvalidArg("sealing empty time page".into()));
        }

        self.time_encoder.flush(&mut self.time_buf)?;

        let uncompressed_size = self.time_buf.len() as u32;
        let compressed_data = self.compressor.compress(&self.time_buf)?;

        let sealed = SealedTimePage {
            statistic: self.statistic.clone(),
            uncompressed_size,
            compressed_data,
        };

        self.reset();
        Ok(sealed)
    }

    pub fn reset(&mut self) {
        self.time_buf.clear();
        self.point_num = 0;
        self.statistic.reset();
        self.time_encoder.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_config() -> Arc<Config> {
        let mut cfg = Config::default();
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        Arc::new(cfg)
    }

    #[test]
    fn write_and_seal() {
        let config = make_config();
        let mut pw = TimePageWriter::new(TSEncoding::Plain, CompressionType::Uncompressed, config).unwrap();
        pw.write(1000).unwrap();
        pw.write(2000).unwrap();
        pw.write(3000).unwrap();
        let sealed = pw.seal().unwrap();
        assert_eq!(sealed.statistic.count(), 3);
        assert_eq!(sealed.statistic.start_time(), 1000);
        assert_eq!(sealed.statistic.end_time(), 3000);
        assert!(!sealed.compressed_data.is_empty());
    }

    #[test]
    fn seal_empty_errors() {
        let config = make_config();
        let mut pw = TimePageWriter::new(TSEncoding::Plain, CompressionType::Uncompressed, config).unwrap();
        assert!(pw.seal().is_err());
    }

    #[test]
    fn is_full_point_count() {
        let mut cfg = Config::default();
        cfg.page_writer_max_point_num = 2;
        cfg.time_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        let config = Arc::new(cfg);
        let mut pw = TimePageWriter::new(TSEncoding::Plain, CompressionType::Uncompressed, config).unwrap();
        pw.write(1).unwrap();
        assert!(!pw.is_full());
        pw.write(2).unwrap();
        assert!(pw.is_full());
    }

    #[test]
    fn reset_clears_state() {
        let config = make_config();
        let mut pw = TimePageWriter::new(TSEncoding::Plain, CompressionType::Uncompressed, config).unwrap();
        pw.write(1000).unwrap();
        pw.reset();
        assert_eq!(pw.point_num, 0);
        assert!(!pw.has_data());
    }
}
