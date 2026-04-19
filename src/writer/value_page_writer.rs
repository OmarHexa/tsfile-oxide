// ValuePageWriter encodes one value column for an aligned page.
//
// C++ ValuePageWriter holds a value encoder + compressor. A null bitmap
// tracks which positions have null values so the reader can skip null
// slots while decoding. C++ uses a raw bool* bitmap; in Rust we build
// the bitmap as a Vec<u8> bit array inline during serialization.
//
// Aligned value page body format (before compression):
//   [null_bitmap_bytes: ceil(point_num / 8) bytes]
//   [encoded_value_bytes...]
//
// A set bit at position r means row r is null (value absent). This matches
// the C++ convention (1 = null, 0 = present). The reader checks the bitmap
// before decoding each value slot.

use crate::compress::Compressor;
use crate::config::Config;
use crate::encoding::encoder::Encoder;
use crate::error::{Result, TsFileError};
use crate::statistic::Statistic;
use crate::types::{CompressionType, TSDataType, TSEncoding};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// SealedValuePage
// ---------------------------------------------------------------------------

/// Output of ValuePageWriter::seal() — compressed value page data.
pub struct SealedValuePage {
    pub statistic: Statistic,
    pub uncompressed_size: u32,
    pub compressed_data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// ValuePageWriter
// ---------------------------------------------------------------------------

/// Encodes and compresses the value column (with null bitmap) for one aligned page.
///
/// C++ ValuePageWriter is analogous to PageWriter but only covers the value
/// column — the time column is written by TimePageWriter. In Rust, the null
/// bitmap is a plain Vec<u8> built on seal() from the per-slot null flags
/// accumulated in a Vec<bool>. This avoids manual bit-twiddling during writes
/// at the cost of a small allocation on seal.
pub struct ValuePageWriter {
    data_type: TSDataType,
    value_encoder: Encoder,
    compressor: Compressor,
    statistic: Statistic,
    value_buf: Vec<u8>,
    /// One bool per row: true = null, false = has value.
    null_flags: Vec<bool>,
    pub point_num: usize,
    config: Arc<Config>,
}

impl ValuePageWriter {
    pub fn new(
        data_type: TSDataType,
        value_encoding: TSEncoding,
        compression: CompressionType,
        config: Arc<Config>,
    ) -> Result<Self> {
        let value_encoder = Encoder::new(data_type, value_encoding)?;
        let compressor = Compressor::new(compression);
        Ok(Self {
            data_type,
            value_encoder,
            compressor,
            statistic: Statistic::new(data_type),
            value_buf: Vec::new(),
            null_flags: Vec::new(),
            point_num: 0,
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Write methods
    // -----------------------------------------------------------------------

    /// Record a null slot (no value for this row).
    pub fn write_null(&mut self) {
        self.null_flags.push(true);
        self.point_num += 1;
    }

    pub fn write_bool(&mut self, timestamp: i64, value: bool) -> Result<()> {
        self.value_encoder.encode_bool(value, &mut self.value_buf)?;
        self.statistic.update_bool(timestamp, value);
        self.null_flags.push(false);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_i32(&mut self, timestamp: i64, value: i32) -> Result<()> {
        self.value_encoder.encode_i32(value, &mut self.value_buf)?;
        self.statistic.update_i32(timestamp, value);
        self.null_flags.push(false);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_i64(&mut self, timestamp: i64, value: i64) -> Result<()> {
        self.value_encoder.encode_i64(value, &mut self.value_buf)?;
        self.statistic.update_i64(timestamp, value);
        self.null_flags.push(false);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_f32(&mut self, timestamp: i64, value: f32) -> Result<()> {
        self.value_encoder.encode_f32(value, &mut self.value_buf)?;
        self.statistic.update_f32(timestamp, value);
        self.null_flags.push(false);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_f64(&mut self, timestamp: i64, value: f64) -> Result<()> {
        self.value_encoder.encode_f64(value, &mut self.value_buf)?;
        self.statistic.update_f64(timestamp, value);
        self.null_flags.push(false);
        self.point_num += 1;
        Ok(())
    }

    pub fn write_text(&mut self, timestamp: i64, value: &[u8]) -> Result<()> {
        self.value_encoder.encode_bytes(value, &mut self.value_buf)?;
        self.statistic.update_text(timestamp, value);
        self.null_flags.push(false);
        self.point_num += 1;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Threshold checks
    // -----------------------------------------------------------------------

    pub fn is_full(&self) -> bool {
        self.point_num >= self.config.page_writer_max_point_num as usize
            || self.memory_estimate() >= self.config.page_writer_max_memory_bytes as usize
    }

    pub fn memory_estimate(&self) -> usize {
        self.value_buf.len() + self.null_flags.len()
    }

    pub fn has_data(&self) -> bool {
        self.point_num > 0
    }

    // -----------------------------------------------------------------------
    // Seal
    // -----------------------------------------------------------------------

    /// Flush encoder, prepend null bitmap, compress, and return the sealed page.
    ///
    /// The null bitmap is `ceil(point_num / 8)` bytes where bit r is set if
    /// row r is null. We build this on seal rather than maintaining a byte
    /// array during writes to avoid bit manipulation overhead per write.
    pub fn seal(&mut self) -> Result<SealedValuePage> {
        if self.point_num == 0 {
            return Err(TsFileError::InvalidArg("sealing empty value page".into()));
        }

        self.value_encoder.flush(&mut self.value_buf)?;

        // Build null bitmap: ceil(point_num / 8) bytes, bit r set = null.
        let bitmap_bytes = (self.point_num + 7) / 8;
        let mut bitmap = vec![0u8; bitmap_bytes];
        for (r, &is_null) in self.null_flags.iter().enumerate() {
            if is_null {
                bitmap[r / 8] |= 1 << (r % 8);
            }
        }

        // Body = [bitmap] + [value_bytes]
        let mut body = Vec::with_capacity(bitmap_bytes + self.value_buf.len());
        body.extend_from_slice(&bitmap);
        body.extend_from_slice(&self.value_buf);

        let uncompressed_size = body.len() as u32;
        let compressed_data = self.compressor.compress(&body)?;

        let sealed = SealedValuePage {
            statistic: self.statistic.clone(),
            uncompressed_size,
            compressed_data,
        };

        self.reset();
        Ok(sealed)
    }

    pub fn reset(&mut self) {
        self.value_buf.clear();
        self.null_flags.clear();
        self.point_num = 0;
        self.statistic.reset();
        self.value_encoder.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_config() -> Arc<Config> {
        let mut cfg = Config::default();
        cfg.int32_encoding_type = TSEncoding::Plain;
        cfg.float_encoding_type = TSEncoding::Plain;
        cfg.default_compression_type = CompressionType::Uncompressed;
        Arc::new(cfg)
    }

    #[test]
    fn write_i32_and_seal() {
        let config = make_config();
        let mut pw = ValuePageWriter::new(
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        pw.write_i32(1000, 42).unwrap();
        pw.write_i32(2000, 43).unwrap();
        let sealed = pw.seal().unwrap();
        assert_eq!(sealed.statistic.count(), 2);
        assert!(!sealed.compressed_data.is_empty());
    }

    #[test]
    fn null_slots_do_not_count_in_statistic() {
        let config = make_config();
        let mut pw = ValuePageWriter::new(
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        pw.write_i32(1000, 10).unwrap();
        pw.write_null();
        pw.write_i32(3000, 30).unwrap();
        assert_eq!(pw.point_num, 3);
        let sealed = pw.seal().unwrap();
        // Statistic counts only non-null writes
        assert_eq!(sealed.statistic.count(), 2);
        assert!(!sealed.compressed_data.is_empty());
    }

    #[test]
    fn seal_empty_errors() {
        let config = make_config();
        let mut pw = ValuePageWriter::new(
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        assert!(pw.seal().is_err());
    }

    #[test]
    fn reset_clears_state() {
        let config = make_config();
        let mut pw = ValuePageWriter::new(
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
            config,
        )
        .unwrap();
        pw.write_i32(1, 99).unwrap();
        pw.reset();
        assert_eq!(pw.point_num, 0);
        assert!(!pw.has_data());
    }
}
