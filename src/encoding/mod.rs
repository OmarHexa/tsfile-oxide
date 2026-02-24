// C++ encoding/ is 34 header-only files with a virtual Encoder*/Decoder*
// hierarchy and EncoderFactory/DecoderFactory for allocation. In Rust we
// use enum dispatch — the set of encodings is closed (7 algorithms), so
// a match statement replaces virtual dispatch with zero heap allocation
// and no vtable indirection.
//
// ENCODING ALGORITHMS OVERVIEW:
//
// 1. **Plain**: Raw binary encoding. Int32 → 4 bytes little-endian, Float → 4 bytes IEEE-754, etc.
//    Simplest algorithm, no compression, used as fallback or for already-compressed data.
//
// 2. **Dictionary**: Build a dictionary of unique values, encode as indices.
//    Effective for low-cardinality string columns (e.g., status codes, categories).
//    Dictionary stored in page header, values encoded as var-length indices.
//
// 3. **RLE (Run-Length Encoding)**: Encode consecutive runs of identical values as (value, count) pairs.
//    Effective for data with long runs of repeated values (e.g., sensor status flags).
//    Works for int32/int64. Format: (value, run_length) pairs.
//
// 4. **TS2DIFF (Delta-of-Delta)**: Store first value, then delta-of-delta for subsequent values.
//    Optimized for monotonically increasing time series (timestamps, sequences).
//    Format: first_value + sequence of delta-of-deltas (stored as varints for small deltas).
//    Works for int32, int64, float, double.
//
// 5. **Gorilla**: Facebook's time series compression (XOR-based).
//    Stores first value, then XOR of each value with previous. Trailing/leading zeros
//    of XOR are compressed. Extremely effective for slow-changing floating-point data.
//    Reference: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
//    Works for int32, int64, float, double.
//
// 6. **ZigZag**: Variable-length integer encoding using zigzag transform.
//    Zigzag maps signed integers to unsigned: -1→1, 1→2, -2→3, 2→4, ...
//    Effective for small magnitude integers (both positive and negative).
//    Works for int32, int64.
//
// 7. **Sprintz**: Predictive encoding for time series.
//    Uses delta-of-delta prediction + bit-packing. Similar to TS2DIFF but with
//    more aggressive bit-packing. Less common than Gorilla/TS2DIFF.
//    Works for int32, int64, float, double.

mod plain;
mod rle;
mod zigzag;
mod gorilla;
mod ts2diff;
mod dictionary;
mod sprintz;
mod bit_packer;
mod utils;

pub mod encoder;
pub mod decoder;

// Re-export unified encoder/decoder enums for external use
pub use encoder::Encoder;
pub use decoder::Decoder;
