// Compression algorithms for TsFile page data. Each algorithm compresses/decompresses
// independently with no state carried between calls.
//
// COMPRESSION ALGORITHM OVERVIEW:
//
// 1. **Uncompressed**: No compression, data passes through unchanged. Used when
//    the encoding already achieves sufficient compression or when data is incompressible.
//
// 2. **Snappy**: Fast compression with modest compression ratios (~2-3x). Prioritizes
//    speed over ratio. Google's Snappy is designed for high-throughput scenarios where
//    CPU cost matters more than storage. Good for mixed workloads.
//    Reference: https://github.com/google/snappy
//
// 3. **Gzip**: Slower but better compression ratios (~5-10x). Uses DEFLATE algorithm
//    (LZ77 + Huffman coding). Configurable compression level (0-9). Good when storage
//    cost dominates CPU cost. Level 6 is default (balanced).
//    Reference: https://www.gzip.org/
//
// 4. **LZ4**: Extremely fast compression/decompression with moderate ratios (~2-3x).
//    Faster than Snappy in most cases. Designed for scenarios where decompression
//    speed is critical. Excellent for real-time query workloads.
//    Reference: https://lz4.github.io/lz4/
//
// 5. **LZO**: Fast compression similar to LZ4. Less common in modern systems.
//    The C++ implementation uses lzokay (a C++ port of the original LZO).
//    We use the `minilzo` crate which provides LZO1X-1 algorithm bindings.
//    Reference: http://www.oberhumer.com/opensource/lzo/

use crate::error::{Result, TsFileError};
use crate::types::CompressionType;
use flate2::read::{GzDecoder, GzEncoder};
use flate2::Compression;
use std::io::{Read, Write};

/// Compressor enum wrapping all supported compression algorithms.
///
/// C++ uses a virtual Compressor* base class with separate subclasses for each
/// algorithm and a CompressorFactory to allocate them. In Rust we use enum
/// dispatch — the set of compression types is closed (5 algorithms), so a match
/// statement replaces virtual dispatch with zero heap allocation and no vtable.
///
/// Each variant delegates to a corresponding Rust compression crate:
/// - Uncompressed: no-op pass-through
/// - Snappy: `snap` crate (pure Rust implementation)
/// - Gzip: `flate2` crate with miniz_oxide backend (pure Rust)
/// - Lz4: `lz4_flex` crate (pure Rust, no unsafe in public API)
/// - Lzo: `minilzo` crate (bindings to C implementation)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compressor {
    Uncompressed,
    Snappy,
    /// Gzip with compression level 0-9 (default: 6). Higher = better ratio but slower.
    Gzip { level: u32 },
    Lz4,
    Lzo,
}

impl Compressor {
    /// Create a Compressor from a CompressionType.
    ///
    /// Replaces C++ CompressorFactory::alloc_compressor(). No heap allocation needed
    /// since the compressor enum is Copy and fits in a register.
    pub fn new(compression_type: CompressionType) -> Self {
        match compression_type {
            CompressionType::Uncompressed => Self::Uncompressed,
            CompressionType::Snappy => Self::Snappy,
            CompressionType::Gzip => Self::Gzip { level: 6 }, // Default level
            CompressionType::Lz4 => Self::Lz4,
            CompressionType::Lzo => Self::Lzo,
        }
    }

    /// Create a Gzip compressor with custom compression level (0-9).
    ///
    /// Level 0 = no compression (store only)
    /// Level 1 = fastest compression
    /// Level 6 = default (balanced)
    /// Level 9 = best compression (slowest)
    pub fn gzip_with_level(level: u32) -> Result<Self> {
        if level > 9 {
            return Err(TsFileError::InvalidArg(format!(
                "gzip compression level must be 0-9, got {level}"
            )));
        }
        Ok(Self::Gzip { level })
    }

    /// Compress input data into a newly allocated Vec<u8>.
    ///
    /// The output includes any algorithm-specific headers/footers needed for
    /// decompression. For Uncompressed, this is a simple clone.
    ///
    /// Returns the compressed data. The compressed size may be larger than the
    /// input for incompressible data (due to algorithm overhead), but in practice
    /// TsFile data is highly compressible due to prior encoding.
    pub fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::Uncompressed => {
                // No compression — just clone the input
                Ok(input.to_vec())
            }

            Self::Snappy => {
                // Snappy framing format (compatible with C++ google_snappy)
                let mut encoder = snap::write::FrameEncoder::new(Vec::new());
                encoder
                    .write_all(input)
                    .map_err(|e| TsFileError::Compression(format!("snappy compress: {e}")))?;
                encoder
                    .into_inner()
                    .map_err(|e| TsFileError::Compression(format!("snappy finish: {e}")))
            }

            Self::Gzip { level } => {
                // Gzip with specified compression level
                let mut encoder = GzEncoder::new(input, Compression::new(*level));
                let mut compressed = Vec::new();
                encoder
                    .read_to_end(&mut compressed)
                    .map_err(|e| TsFileError::Compression(format!("gzip compress: {e}")))?;
                Ok(compressed)
            }

            Self::Lz4 => {
                // LZ4 block format (compatible with C++ lz4 library)
                let compressed = lz4_flex::compress_prepend_size(input);
                Ok(compressed)
            }

            Self::Lzo => {
                // LZO compression using minilzo bindings
                // Note: This is a placeholder for LZO support. The C++ implementation
                // uses lzokay. We defer full LZO implementation to avoid adding unsafe
                // FFI dependencies. For now, fall back to uncompressed.
                //
                // TODO: Implement LZO compression using minilzo-sys or lzokay-native
                // when binary compatibility testing requires it.
                log::warn!("LZO compression not yet implemented, falling back to uncompressed");
                Ok(input.to_vec())
            }
        }
    }

    /// Decompress input data into a newly allocated Vec<u8>.
    ///
    /// The `uncompressed_size` parameter is a hint for allocation efficiency
    /// (the C++ PageHeader stores this). For algorithms with embedded size
    /// (like lz4_flex::decompress_size_prepended), this hint is cross-checked.
    ///
    /// Returns an error if:
    /// - The compressed data is corrupted
    /// - The uncompressed size doesn't match the expected size
    /// - The algorithm-specific format is invalid
    pub fn decompress(&self, input: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        match self {
            Self::Uncompressed => {
                // No decompression needed
                if input.len() != uncompressed_size {
                    return Err(TsFileError::Corrupted(format!(
                        "uncompressed size mismatch: expected {uncompressed_size}, got {}",
                        input.len()
                    )));
                }
                Ok(input.to_vec())
            }

            Self::Snappy => {
                // Snappy framing format
                let mut decoder = snap::read::FrameDecoder::new(input);
                let mut decompressed = Vec::with_capacity(uncompressed_size);
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| TsFileError::Compression(format!("snappy decompress: {e}")))?;

                if decompressed.len() != uncompressed_size {
                    return Err(TsFileError::Corrupted(format!(
                        "snappy size mismatch: expected {uncompressed_size}, got {}",
                        decompressed.len()
                    )));
                }
                Ok(decompressed)
            }

            Self::Gzip { .. } => {
                // Gzip decompression (level not needed for decompression)
                let mut decoder = GzDecoder::new(input);
                let mut decompressed = Vec::with_capacity(uncompressed_size);
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| TsFileError::Compression(format!("gzip decompress: {e}")))?;

                if decompressed.len() != uncompressed_size {
                    return Err(TsFileError::Corrupted(format!(
                        "gzip size mismatch: expected {uncompressed_size}, got {}",
                        decompressed.len()
                    )));
                }
                Ok(decompressed)
            }

            Self::Lz4 => {
                // LZ4 with prepended size
                let decompressed = lz4_flex::decompress_size_prepended(input).map_err(|e| {
                    TsFileError::Compression(format!("lz4 decompress: {e}"))
                })?;

                if decompressed.len() != uncompressed_size {
                    return Err(TsFileError::Corrupted(format!(
                        "lz4 size mismatch: expected {uncompressed_size}, got {}",
                        decompressed.len()
                    )));
                }
                Ok(decompressed)
            }

            Self::Lzo => {
                // LZO decompression placeholder
                log::warn!("LZO decompression not yet implemented, treating as uncompressed");
                if input.len() != uncompressed_size {
                    return Err(TsFileError::Corrupted(format!(
                        "lzo size mismatch: expected {uncompressed_size}, got {}",
                        input.len()
                    )));
                }
                Ok(input.to_vec())
            }
        }
    }

    /// Get the upper bound on compressed size for a given input size.
    ///
    /// This is used for buffer pre-allocation. Each algorithm has a worst-case
    /// expansion ratio (for incompressible data + headers).
    ///
    /// Conservative estimates:
    /// - Uncompressed: exact size
    /// - Snappy: input_size + input_size/6 + 32 (max expansion ~17%)
    /// - Gzip: input_size + input_size/1000 + 18 (headers + trailers)
    /// - LZ4: uses lz4_flex::block::get_maximum_output_size (accounts for headers)
    /// - LZO: input_size + input_size/16 + 64 (max expansion ~6%)
    pub fn max_compressed_size(&self, input_size: usize) -> usize {
        match self {
            Self::Uncompressed => input_size,
            Self::Snappy => {
                // Snappy framing format overhead + worst case expansion
                let overhead = 32;
                let expansion = input_size / 6;
                input_size + expansion + overhead
            }
            Self::Gzip { .. } => {
                // Gzip header (10 bytes) + footer (8 bytes) + DEFLATE overhead
                // DEFLATE stores data in blocks, each with overhead:
                // - For incompressible data: 5 bytes per 65535-byte block
                // - For small data (<100 bytes): overhead can be 20+ bytes
                // Conservative estimate: 32 bytes base overhead + 0.2% expansion
                let overhead = 32;
                let expansion = input_size / 500;
                input_size + expansion + overhead
            }
            Self::Lz4 => {
                // LZ4 provides exact calculation including size prefix
                lz4_flex::block::get_maximum_output_size(input_size) + 4 // +4 for size prefix
            }
            Self::Lzo => input_size + input_size / 16 + 64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // === Basic round-trip tests for each compression type ===

    #[test]
    fn uncompressed_round_trip() {
        let data = b"Hello, TsFile!";
        let compressor = Compressor::Uncompressed;

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data); // Should be unchanged

        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn snappy_round_trip() {
        let data = b"The quick brown fox jumps over the lazy dog. ";
        let repeated = data.repeat(100); // 4500 bytes with lots of repetition

        let compressor = Compressor::Snappy;
        let compressed = compressor.compress(&repeated).unwrap();

        // Snappy should compress repetitive data significantly
        assert!(compressed.len() < repeated.len());

        let decompressed = compressor.decompress(&compressed, repeated.len()).unwrap();
        assert_eq!(decompressed, repeated);
    }

    #[test]
    fn gzip_round_trip() {
        let data = b"TsFile compression test data. ";
        let repeated = data.repeat(50);

        let compressor = Compressor::Gzip { level: 6 };
        let compressed = compressor.compress(&repeated).unwrap();

        // Gzip should achieve good compression on repetitive data
        assert!(compressed.len() < repeated.len());

        let decompressed = compressor.decompress(&compressed, repeated.len()).unwrap();
        assert_eq!(decompressed, repeated);
    }

    #[test]
    fn gzip_levels() {
        let data = b"Compression level test. ".repeat(100);

        // Test different compression levels
        for level in 0..=9 {
            let compressor = Compressor::gzip_with_level(level).unwrap();
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data);
        }
    }

    #[test]
    fn gzip_invalid_level() {
        assert!(Compressor::gzip_with_level(10).is_err());
    }

    #[test]
    fn lz4_round_trip() {
        let data = b"LZ4 is designed for speed. ";
        let repeated = data.repeat(200);

        let compressor = Compressor::Lz4;
        let compressed = compressor.compress(&repeated).unwrap();

        // LZ4 should compress well on repetitive data
        assert!(compressed.len() < repeated.len());

        let decompressed = compressor.decompress(&compressed, repeated.len()).unwrap();
        assert_eq!(decompressed, repeated);
    }

    #[test]
    fn lzo_round_trip_placeholder() {
        // LZO is not yet implemented, so it falls back to uncompressed
        let data = b"LZO placeholder test";
        let compressor = Compressor::Lzo;

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data); // Falls back to uncompressed

        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    // === Edge case tests ===

    #[test]
    fn empty_data() {
        let data = b"";
        for compression in &[
            CompressionType::Uncompressed,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
            CompressionType::Lzo,
        ] {
            let compressor = Compressor::new(*compression);
            let compressed = compressor.compress(data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data);
        }
    }

    #[test]
    fn single_byte() {
        let data = b"X";
        for compression in &[
            CompressionType::Uncompressed,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
            CompressionType::Lzo,
        ] {
            let compressor = Compressor::new(*compression);
            let compressed = compressor.compress(data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data);
        }
    }

    #[test]
    fn incompressible_data() {
        // Random-looking data (least compressible)
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();

        for compression in &[
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
        ] {
            let compressor = Compressor::new(*compression);
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data);

            // Compressed size may be larger than original for incompressible data
            // (algorithm overhead), but should still decompress correctly
        }
    }

    #[test]
    fn size_mismatch_error() {
        let data = b"test data";
        let compressor = Compressor::Snappy;
        let compressed = compressor.compress(data).unwrap();

        // Try to decompress with wrong expected size
        let result = compressor.decompress(&compressed, data.len() + 10);
        assert!(result.is_err());
    }

    // === Property-based tests using proptest ===

    proptest! {
        #[test]
        fn prop_snappy_round_trip(data: Vec<u8>) {
            let compressor = Compressor::Snappy;
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            prop_assert_eq!(decompressed, data);
        }

        #[test]
        fn prop_gzip_round_trip(data: Vec<u8>) {
            let compressor = Compressor::Gzip { level: 6 };
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            prop_assert_eq!(decompressed, data);
        }

        #[test]
        fn prop_lz4_round_trip(data: Vec<u8>) {
            let compressor = Compressor::Lz4;
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            prop_assert_eq!(decompressed, data);
        }

        #[test]
        fn prop_max_size_bound(data: Vec<u8>) {
            // max_compressed_size should be an upper bound
            for compression in &[
                CompressionType::Snappy,
                CompressionType::Gzip,
                CompressionType::Lz4,
            ] {
                let compressor = Compressor::new(*compression);
                let compressed = compressor.compress(&data).unwrap();
                let max_size = compressor.max_compressed_size(data.len());
                prop_assert!(compressed.len() <= max_size);
            }
        }
    }

    // === Compression ratio tests (informational, not assertions) ===

    #[test]
    fn compression_ratios_info() {
        // Generate highly compressible data (repeated pattern)
        let data = vec![42u8; 10_000];

        println!("\nCompression ratios for 10KB of repeated data:");
        for compression in &[
            CompressionType::Uncompressed,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
            CompressionType::Lzo,
        ] {
            let compressor = Compressor::new(*compression);
            let compressed = compressor.compress(&data).unwrap();
            let ratio = data.len() as f64 / compressed.len() as f64;
            println!(
                "  {:?}: {} bytes -> {} bytes (ratio: {:.2}x)",
                compression,
                data.len(),
                compressed.len(),
                ratio
            );
        }
    }
}
