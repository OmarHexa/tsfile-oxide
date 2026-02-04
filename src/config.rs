use crate::error::{Result, TsFileError};
use crate::types::{CompressionType, TSDataType, TSEncoding};

pub struct Config {
    // tsblock memory self-increment step size
    pub tsblock_mem_inc_step_size: u32,
    // the maximum memory of a single tsblock
    pub tsblock_max_memory: u32,
    // Maximum number of data points per page before sealing.
    pub page_writer_max_point_num: u32,
    // Maximum memory bytes per page before sealing.
    pub page_writer_max_memory_bytes: u32,
    // Maximum fan-out of B-tree-like index nodes.
    pub max_degree_of_index_node: u32,
    // Bloom filter false positive rate.
    pub tsfile_index_bloom_filter_error_percent: f64,
    // Default encoding for timestamp columns.
    pub time_encoding_type: TSEncoding,
    pub time_data_type: TSDataType,
    pub time_compress_type: CompressionType,
    // Chunk group size threshold before flushing to disk.
    pub chunk_group_size_threshold: u32,
    pub record_count_for_next_mem_check: i32,
    pub encrypt_flag: bool,
    pub boolean_encoding_type: TSEncoding,
    pub int32_encoding_type: TSEncoding,
    pub int64_encoding_type: TSEncoding,
    pub float_encoding_type: TSEncoding,
    pub double_encoding_type: TSEncoding,
    pub string_encoding_type: TSEncoding,
    // Default compression for all columns.
    pub default_compression_type: CompressionType,
}

impl Config {
    pub fn get_value_encoder(&self, data_type: TSDataType) -> TSEncoding {
        match data_type {
            TSDataType::Boolean => self.boolean_encoding_type,
            TSDataType::Int32 => self.int32_encoding_type,
            TSDataType::Int64 => self.int64_encoding_type,
            TSDataType::Float => self.float_encoding_type,
            TSDataType::Double => self.double_encoding_type,
            TSDataType::Text => self.string_encoding_type,
            TSDataType::String => self.string_encoding_type,
        }
    }

    pub fn get_default_compressor(&self) -> CompressionType {
        self.default_compression_type
    }

    pub fn set_page_max_point_count(&mut self, page_writer_max_point_num: u32) {
        self.page_writer_max_point_num = page_writer_max_point_num;
    }

    pub fn set_max_degree_of_index_node(&mut self, max_degree_of_index_node: u32) {
        self.max_degree_of_index_node = max_degree_of_index_node;
    }

    /// Set the encoding type for a specific data type.
    ///
    /// This validates that the encoding is supported for the given data type
    /// and returns an error if the combination is not supported.
    ///
    /// # Encoding Support by Data Type:
    /// - Boolean: Only PLAIN
    /// - Int32/Int64: PLAIN, TS_2DIFF, GORILLA, ZIGZAG, RLE, SPRINTZ
    /// - Float/Double: PLAIN, TS_2DIFF, GORILLA, SPRINTZ
    /// - Text: PLAIN, DICTIONARY
    pub fn set_datatype_encoding(&mut self, data_type: u8, encoding: u8) -> Result<()> {
        let data_type: TSDataType = data_type.try_into().expect("Invalid data type");
        let encoding: TSEncoding = encoding.try_into().expect("Invalid encoding");
        match data_type {
            TSDataType::Boolean => {
                if encoding != TSEncoding::Plain {
                    return Err(TsFileError::Unsupported(format!(
                        "Boolean only supports PLAIN encoding, got {:?}",
                        encoding
                    )));
                }
                self.boolean_encoding_type = encoding;
            }

            TSDataType::Int32 | TSDataType::Int64 => match encoding {
                TSEncoding::Plain
                | TSEncoding::Ts2Diff
                | TSEncoding::Gorilla
                | TSEncoding::Zigzag
                | TSEncoding::Rle
                | TSEncoding::Sprintz => {
                    if data_type == TSDataType::Int32 {
                        self.int32_encoding_type = encoding;
                    } else {
                        self.int64_encoding_type = encoding;
                    }
                }
                _ => {
                    return Err(TsFileError::Unsupported(format!(
                        "Int32 does not support {:?} encoding",
                        encoding
                    )));
                }
            },

            TSDataType::Float | TSDataType::Double => match encoding {
                TSEncoding::Plain
                | TSEncoding::Ts2Diff
                | TSEncoding::Gorilla
                | TSEncoding::Sprintz => {
                    if data_type == TSDataType::Float {
                        self.float_encoding_type = encoding;
                    } else {
                        self.double_encoding_type = encoding;
                    }
                }
                _ => {
                    return Err(TsFileError::Unsupported(format!(
                        "Float does not support {:?} encoding",
                        encoding
                    )));
                }
            },

            TSDataType::Text | TSDataType::String => match encoding {
                TSEncoding::Plain | TSEncoding::Dictionary => {
                    self.string_encoding_type = encoding;
                }
                _ => {
                    return Err(TsFileError::Unsupported(format!(
                        "Text does not support {:?} encoding",
                        encoding
                    )));
                }
            },
        }

        Ok(())
    }
}

// Default values match C++ init_config_value() so files produced with
// default settings are compatible across implementations.
impl Default for Config {
    fn default() -> Self {
        Self {
            tsblock_mem_inc_step_size: 8000,
            tsblock_max_memory: 64000,
            page_writer_max_point_num: 10000,
            page_writer_max_memory_bytes: 128 * 1024,
            max_degree_of_index_node: 256,
            tsfile_index_bloom_filter_error_percent: 0.05,
            record_count_for_next_mem_check: 100,
            chunk_group_size_threshold: 128 * 1024 * 1024,
            time_encoding_type: TSEncoding::Ts2Diff,
            time_data_type: TSDataType::Int64,
            time_compress_type: CompressionType::Lz4,
            encrypt_flag: false,
            boolean_encoding_type: TSEncoding::Plain,
            int32_encoding_type: TSEncoding::Ts2Diff,
            int64_encoding_type: TSEncoding::Ts2Diff,
            float_encoding_type: TSEncoding::Gorilla,
            double_encoding_type: TSEncoding::Gorilla,
            // C++ maps TEXT/STRING/BLOB to string_encoding_type.
            string_encoding_type: TSEncoding::Plain,
            default_compression_type: CompressionType::Lz4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify default values match C++ init_config_value().
    /// If these change, files written with defaults will be incompatible.
    #[test]
    fn default_matches_cpp_init_config() {
        let cfg = Config::default();
        assert_eq!(cfg.tsblock_mem_inc_step_size, 8000);
        assert_eq!(cfg.tsblock_max_memory, 64000);
        assert_eq!(cfg.page_writer_max_point_num, 10_000);
        assert_eq!(cfg.page_writer_max_memory_bytes, 128 * 1024);
        assert_eq!(cfg.max_degree_of_index_node, 256);
        assert!((cfg.tsfile_index_bloom_filter_error_percent - 0.05).abs() < f64::EPSILON);
        assert_eq!(cfg.record_count_for_next_mem_check, 100);
        assert_eq!(cfg.chunk_group_size_threshold, 128 * 1024 * 1024);
        assert_eq!(cfg.time_encoding_type, TSEncoding::Ts2Diff);
        assert_eq!(cfg.time_data_type, TSDataType::Int64);
        assert_eq!(cfg.time_compress_type, CompressionType::Lz4);
        assert_eq!(cfg.encrypt_flag, false);
        assert_eq!(cfg.boolean_encoding_type, TSEncoding::Plain);
        assert_eq!(cfg.int32_encoding_type, TSEncoding::Ts2Diff);
        assert_eq!(cfg.int64_encoding_type, TSEncoding::Ts2Diff);
        assert_eq!(cfg.float_encoding_type, TSEncoding::Gorilla);
        assert_eq!(cfg.double_encoding_type, TSEncoding::Gorilla);
        assert_eq!(cfg.string_encoding_type, TSEncoding::Plain);
        assert_eq!(cfg.default_compression_type, CompressionType::Lz4);
    }

    #[test]
    fn config_fields_are_mutable() {
        let mut cfg = Config::default();
        cfg.set_page_max_point_count(5_000);
        cfg.default_compression_type = CompressionType::Snappy;
        assert_eq!(cfg.page_writer_max_point_num, 5_000);
        assert_eq!(cfg.get_default_compressor(), CompressionType::Snappy);
    }

    #[test]
    fn test_get_value_encoder() {
        let cfg = Config::default();
        assert_eq!(
            cfg.get_value_encoder(TSDataType::Boolean),
            TSEncoding::Plain
        );
        assert_eq!(
            cfg.get_value_encoder(TSDataType::Int32),
            TSEncoding::Ts2Diff
        );
        assert_eq!(
            cfg.get_value_encoder(TSDataType::Int64),
            TSEncoding::Ts2Diff
        );
        assert_eq!(
            cfg.get_value_encoder(TSDataType::Float),
            TSEncoding::Gorilla
        );
        assert_eq!(
            cfg.get_value_encoder(TSDataType::Double),
            TSEncoding::Gorilla
        );
        assert_eq!(cfg.get_value_encoder(TSDataType::Text), TSEncoding::Plain);
    }

    #[test]
    fn test_set_datatype_encoding_boolean() {
        let mut cfg = Config::default();

        // Boolean only supports PLAIN
        assert!(cfg.set_datatype_encoding(0u8, 0u8).is_ok());
        assert_eq!(cfg.boolean_encoding_type, TSEncoding::Plain);

        // All other encodings should fail
        assert!(cfg.set_datatype_encoding(0u8, 1u8).is_err());
        assert!(cfg.set_datatype_encoding(0u8, 2u8).is_err());
        assert!(cfg.set_datatype_encoding(0u8, 6u8).is_err());
    }

    #[test]
    fn test_set_datatype_encoding_int64() {
        let mut cfg = Config::default();

        // Int64 supports: PLAIN, TS_2DIFF, GORILLA, ZIGZAG, RLE, SPRINTZ
        assert!(cfg.set_datatype_encoding(2u8, 0u8).is_ok());
        assert!(cfg.set_datatype_encoding(2u8, 1u8).is_ok());
        assert!(cfg.set_datatype_encoding(2u8, 2u8).is_ok());
        assert!(cfg.set_datatype_encoding(2u8, 3u8).is_ok());
        assert!(cfg.set_datatype_encoding(2u8, 4u8).is_ok());
        assert!(cfg.set_datatype_encoding(2u8, 5u8).is_ok());
        assert_eq!(cfg.int64_encoding_type, TSEncoding::Sprintz);

        // Dictionary should fail
        assert!(cfg.set_datatype_encoding(2u8, 1u8).is_err());
    }

    #[test]
    fn test_set_datatype_encoding_float() {
        let mut cfg = Config::default();

        // Float supports: PLAIN, TS_2DIFF, GORILLA, SPRINTZ
        assert!(cfg.set_datatype_encoding(3u8, 0u8).is_ok());
        assert!(cfg.set_datatype_encoding(3u8, 1u8).is_ok());
        assert!(cfg.set_datatype_encoding(3u8, 2u8).is_ok());
        assert!(cfg.set_datatype_encoding(3u8, 5u8).is_ok());
        assert_eq!(cfg.float_encoding_type, TSEncoding::Sprintz);

        // ZIGZAG, RLE, Dictionary should fail
        assert!(cfg.set_datatype_encoding(3u8, 3u8).is_err());
        assert!(cfg.set_datatype_encoding(3u8, 4u8).is_err());
        assert!(cfg.set_datatype_encoding(3u8, 6u8).is_err());
    }

    #[test]
    fn test_set_datatype_encoding_double() {
        let mut cfg = Config::default();

        // Double supports: PLAIN, TS_2DIFF, GORILLA, SPRINTZ
        assert!(cfg.set_datatype_encoding(4u8, 0u8).is_ok());
        assert!(cfg.set_datatype_encoding(4u8, 1u8).is_ok());
        assert!(cfg.set_datatype_encoding(4u8, 2u8).is_ok());
        assert!(cfg.set_datatype_encoding(4u8, 5u8).is_ok());
        assert_eq!(cfg.double_encoding_type, TSEncoding::Sprintz);

        // ZIGZAG, RLE, Dictionary should fail
        assert!(cfg.set_datatype_encoding(4u8, 3u8).is_err());
        assert!(cfg.set_datatype_encoding(4u8, 4u8).is_err());
        assert!(cfg.set_datatype_encoding(4u8, 6u8).is_err());
    }
}
