# TsFile C++ to Rust Rewrite Plan

## Overview

Rewrite the TsFile C++ library (`cpp/src/`, ~200 files) as an idiomatic Rust crate that produces and reads binary-compatible `.tsfile` files. The Rust implementation must match the on-disk format exactly so files written by C++ can be read by Rust and vice versa.

---

## A. Crate Structure: Cargo Workspace

```
tsfile-rs/
  Cargo.toml                      # [workspace]
  crates/
    tsfile-common/                 # types, config, schema, tablet, statistic, device_id, errors
    tsfile-encoding/               # encoder/decoder enums + all algorithm implementations
    tsfile-compress/               # compressor enum + snappy/gzip/lz4/lzo/uncompressed
    tsfile-io/                     # file I/O, TsFileIOReader/Writer, bloom filter
    tsfile-writer/                 # TsFileWriter, ChunkWriter, PageWriter pipelines
    tsfile-reader/                 # TsFileReader, ChunkReader, filters, ResultSet, query executors
    tsfile/                        # facade crate: pub use re-exports for single-dependency UX
    tsfile-ffi/                    # C FFI bindings via cbindgen
  tests/                           # cross-crate integration tests
  benches/                         # criterion benchmarks
  test-data/                       # reference .tsfile files from C++ for compat testing
```

**Dependency graph (each arrow = depends on):**
```
tsfile-ffi -> tsfile
tsfile -> tsfile-writer, tsfile-reader
tsfile-writer -> tsfile-io, tsfile-encoding, tsfile-compress, tsfile-common
tsfile-reader -> tsfile-io, tsfile-encoding, tsfile-compress, tsfile-common
tsfile-io -> tsfile-encoding, tsfile-compress, tsfile-common
tsfile-encoding -> tsfile-common
tsfile-compress -> tsfile-common
```

---

## B. Rust Dependencies

| Functionality | C++ Implementation | Rust Crate | Notes |
|---|---|---|---|
| Snappy compression | vendored `google_snappy/` | `snap` | Pure Rust |
| Gzip compression | vendored `zlib-1.3.1/` | `flate2` (miniz_oxide backend) | Pure Rust |
| LZ4 compression | vendored `lz4/` | `lz4_flex` | Pure Rust, no unsafe in public API |
| LZO compression | vendored `lzokay/` | `lzokay-native` or hand-port (~200 lines) | Less common; may need a small custom impl |
| MurmurHash3 | `common/container/murmur_hash3.cc` | `murmur3` crate or hand-port | Must produce identical hashes for bloom filter compat |
| Byte buffers | `ByteStream` + `SerializationUtil` | `bytes` + `std::io::Cursor` + custom varint | Varint encoding must match C++ exactly |
| Arena allocator | `PageArena` (custom) | `bumpalo` | Used in reader-side deserialization only |
| Bloom filter | `reader/bloom_filter.h` (~130 lines) | Implement directly | Must match C++ seed array `SEEDS[8]` and murmur3 |
| Path parsing | ANTLR4 (entire runtime vendored) | Hand-written parser (~50 lines) or `nom` | Grammar is trivial: split on `.` with quoting |
| Error handling | 54 int codes + `RET_FAIL` macro | `thiserror` | `TsFileError` enum with `?` propagation |
| Date/time | `DateConverter` | `chrono` | For DATE type only |
| Testing | Google Test | `cargo test` + `proptest` | Property-based testing for encoder round-trips |
| Benchmarking | custom `bench_mark/` | `criterion` | Compare vs C++ baseline |
| C FFI | manual `cwrapper/` | `cbindgen` | Auto-generates C header from Rust |

---

## C. Module-by-Module Rust Design

### C.1. `tsfile-common` — Foundation Types

**Replaces:** `cpp/src/common/` + `cpp/src/utils/` + `cpp/src/parser/`

```
src/
  lib.rs
  error.rs            # TsFileError enum (replaces errno_define.h, 54 codes)
  types.rs            # TSDataType, TSEncoding, CompressionType enums (#[repr(u8)])
  config.rs           # Config struct with Default (replaces global g_config_value_)
  schema.rs           # MeasurementSchema, TableSchema, ColumnSchema, ColumnCategory
  device_id.rs        # DeviceId struct (replaces IDeviceID virtual hierarchy)
  statistic.rs        # Statistic enum with typed variants (replaces Statistic class hierarchy)
  tablet.rs           # Tablet struct with ColumnData enum (replaces union-based value matrix)
  record.rs           # TsRecord, DataPoint
  value.rs            # TsValue enum for dynamic type dispatch
  bitmap.rs           # BitMap
  tsfile_format.rs    # ChunkHeader, PageHeader, ChunkMeta, ChunkGroupMeta, MetaIndexNode, TsFileMeta
  path.rs             # Path parser (replaces entire ANTLR4 runtime)
  serialize.rs        # WriteBuffer, ReadCursor with varint/string serialization
```

**Key idiomatic Rust transformations:**

1. **Error codes -> Result type:**
   ```rust
   // C++: int ret = E_OK; if (RET_FAIL(op())) return ret;
   // Rust:
   #[derive(Debug, thiserror::Error)]
   pub enum TsFileError {
       #[error("out of memory")]
       OutOfMemory,
       #[error("I/O error: {0}")]
       Io(#[from] std::io::Error),
       #[error("corrupted tsfile: {0}")]
       Corrupted(String),
       #[error("type mismatch: expected {expected:?}, got {actual:?}")]
       TypeMismatch { expected: TSDataType, actual: TSDataType },
       #[error("out of order timestamp: {ts} <= {last}")]
       OutOfOrder { ts: i64, last: i64 },
       #[error("encoding error: {0}")]
       Encoding(String),
       #[error("compression error: {0}")]
       Compression(String),
       #[error("not found: {0}")]
       NotFound(String),
       #[error("invalid argument: {0}")]
       InvalidArg(String),
       #[error("unsupported: {0}")]
       Unsupported(String),
   }
   pub type Result<T> = std::result::Result<T, TsFileError>;
   ```

2. **Global config -> explicit parameter:**
   ```rust
   // C++: reads g_config_value_.page_writer_max_point_num_ globally
   // Rust: Config passed at construction, stored as Arc<Config>
   pub struct Config {
       pub page_max_points: u32,           // default 10_000
       pub page_max_memory: u32,           // default 128KB
       pub max_index_node_degree: u32,     // default 256
       pub bloom_filter_error_pct: f64,    // default 0.05
       pub time_encoding: TSEncoding,      // default TS_2DIFF
       pub time_compression: CompressionType, // default LZ4
       pub chunk_group_size_threshold: u64,   // default 128MB
       // per-type default encodings...
   }
   impl Default for Config { /* matches init_config_value() */ }
   ```

3. **Virtual Statistic hierarchy -> enum:**
   ```rust
   // C++: Statistic* with BooleanStatistic, Int32Statistic, etc. (virtual dispatch)
   // Rust: enum by value, no heap allocation
   pub enum Statistic {
       Boolean { count: u64, start_time: i64, end_time: i64, sum: i64, first: bool, last: bool },
       Int32 { count: u64, start_time: i64, end_time: i64, min: i32, max: i32, sum: f64, first: i32, last: i32 },
       Int64 { count: u64, start_time: i64, end_time: i64, min: i64, max: i64, sum: f64, first: i64, last: i64 },
       Float { count: u64, start_time: i64, end_time: i64, min: f32, max: f32, sum: f64, first: f32, last: f32 },
       Double { count: u64, start_time: i64, end_time: i64, min: f64, max: f64, sum: f64, first: f64, last: f64 },
       String { count: u64, start_time: i64, end_time: i64, min: std::string::String, max: std::string::String, first: std::string::String, last: std::string::String },
       Time { count: u64, start_time: i64, end_time: i64 },
   }
   ```

4. **IDeviceID virtual hierarchy -> concrete struct:**
   ```rust
   // C++: IDeviceID* with StringArrayDeviceID (virtual, only one impl)
   // Rust: just a struct
   #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
   pub struct DeviceId {
       pub segments: Vec<String>,
   }
   impl DeviceId {
       pub fn parse(path: &str) -> Self { /* split on '.' with quoting */ }
       pub fn table_name(&self) -> &str { /* first N segments */ }
   }
   ```

5. **Tablet with union value matrix -> enum columns:**
   ```rust
   // C++: void* value_matrix with union ValueMatrixEntry + manual type dispatch
   // Rust:
   pub enum ColumnData {
       Boolean(Vec<bool>),
       Int32(Vec<i32>),
       Int64(Vec<i64>),
       Float(Vec<f32>),
       Double(Vec<f64>),
       Text(Vec<Vec<u8>>),
   }
   pub struct Tablet {
       pub table_name: String,
       pub schemas: Vec<MeasurementSchema>,
       pub timestamps: Vec<i64>,
       pub columns: Vec<ColumnData>,
       pub bitmaps: Vec<BitMap>,     // null tracking per column
       pub row_count: usize,
   }
   ```

6. **Custom mem_alloc/PageArena -> standard Rust allocation:**
   - `mem_alloc(size, MOD_*)` -> `Box::new()`, `Vec::new()`, standard allocator
   - `PageArena` for deserialization -> `bumpalo::Bump` where arena semantics needed
   - Placement new -> direct construction
   - Module-tagged tracking -> removed (Rust has no need for manual tracking)

7. **ANTLR4 path parser -> 50-line hand-written parser:**
   ```rust
   pub fn parse_path(input: &str) -> Vec<String> {
       input.split('.').map(|s| s.trim_matches('`').to_string()).collect()
   }
   ```

### C.2. `tsfile-encoding` — Encoders & Decoders

**Replaces:** `cpp/src/encoding/` (34 header-only files)

```
src/
  lib.rs
  encoder.rs          # Encoder enum (dispatch to concrete encoders)
  decoder.rs          # Decoder enum (dispatch to concrete decoders)
  plain.rs            # PlainEncoder/PlainDecoder
  dictionary.rs       # DictionaryEncoder/DictionaryDecoder
  rle.rs              # RleEncoder/RleDecoder (i32, i64)
  gorilla.rs          # GorillaEncoder/Decoder (i32, i64, f32, f64)
  ts2diff.rs          # Ts2DiffEncoder/Decoder (i32, i64, f32, f64)
  zigzag.rs           # ZigzagEncoder/Decoder (i32, i64)
  sprintz.rs          # SprintzEncoder/Decoder (i32, i64, f32, f64)
  bit_packer.rs       # Int32Packer, Int64Packer
  utils.rs            # bit manipulation helpers
```

**Key design: enum-based dispatch (closed set, no trait objects):**

```rust
// C++: Encoder* base class with 13+ virtual subclasses + EncoderFactory
// Rust: enum with match dispatch — zero heap allocation, no vtable
pub enum Encoder {
    Plain(PlainEncoder),
    Dictionary(DictionaryEncoder),
    Rle(RleEncoder),
    Gorilla(GorillaEncoder),
    Ts2Diff(Ts2DiffEncoder),
    Zigzag(ZigzagEncoder),
    Sprintz(SprintzEncoder),
}

impl Encoder {
    /// Replaces EncoderFactory::alloc_value_encoder()
    pub fn new(encoding: TSEncoding, data_type: TSDataType) -> Result<Self>;

    /// Type-specific encode methods (replaces virtual dispatch)
    pub fn encode_i32(&mut self, value: i32, out: &mut Vec<u8>) -> Result<()>;
    pub fn encode_i64(&mut self, value: i64, out: &mut Vec<u8>) -> Result<()>;
    pub fn encode_f32(&mut self, value: f32, out: &mut Vec<u8>) -> Result<()>;
    pub fn encode_f64(&mut self, value: f64, out: &mut Vec<u8>) -> Result<()>;
    pub fn encode_bool(&mut self, value: bool, out: &mut Vec<u8>) -> Result<()>;
    pub fn encode_bytes(&mut self, value: &[u8], out: &mut Vec<u8>) -> Result<()>;
    pub fn flush(&mut self, out: &mut Vec<u8>) -> Result<()>;
    pub fn reset(&mut self);
    pub fn max_byte_size(&self) -> usize;
}
```

**Template-based encoders -> Rust generics with sealed trait:**

```rust
// C++: GorillaEncoder<T> with T=uint32_t or T=uint64_t
// Rust: generic over a sealed trait
trait GorillaBits: Copy + Eq {
    const VALUE_BITS: u32;
    const LEADING_ZERO_BITS: u32;
    fn leading_zeros(self) -> u32;
    fn trailing_zeros(self) -> u32;
    fn xor(self, other: Self) -> Self;
}
impl GorillaBits for u32 { ... }
impl GorillaBits for u64 { ... }

struct GorillaCore<T: GorillaBits> {
    stored_leading_zeros: u32,
    stored_trailing_zeros: u32,
    stored_value: T,
    first_value_written: bool,
    buffer: u8,
    bits_left: u8,
}

pub enum GorillaEncoder {
    Int32(GorillaCore<u32>),
    Int64(GorillaCore<u64>),
    Float(GorillaCore<u32>),   // f32.to_bits() -> u32
    Double(GorillaCore<u64>),  // f64.to_bits() -> u64
}
```

**Decoder follows the same enum pattern** with `decode_*` methods.

### C.3. `tsfile-compress` — Compression

**Replaces:** `cpp/src/compress/` (14 files)

```rust
// C++: Compressor* base class + CompressorFactory with 5 implementations
// Rust: enum, no heap allocation, no factory
pub enum Compressor {
    Uncompressed,
    Snappy,
    Gzip { level: u32 },
    Lz4,
    Lzo,
}

impl Compressor {
    pub fn new(compression_type: CompressionType) -> Self;
    pub fn compress(&self, input: &[u8]) -> Result<Vec<u8>>;
    pub fn decompress(&self, input: &[u8], uncompressed_size: usize) -> Result<Vec<u8>>;
}
```

Each variant delegates to the corresponding crate (`snap`, `flate2`, `lz4_flex`, etc.). The C++ `reset()`, `after_compress()`, `after_uncompress()`, `destroy()` lifecycle methods are unnecessary — Rust handles resource cleanup via `Drop`.

### C.4. `tsfile-io` — File I/O and Format

**Replaces:** `cpp/src/file/` (11 files)

```
src/
  lib.rs
  read_file.rs        # ReadFile wrapping std::fs::File
  write_file.rs       # WriteFile wrapping BufWriter<File>
  io_reader.rs        # TsFileIOReader (metadata parsing, chunk location)
  io_writer.rs        # TsFileIOWriter (magic, chunk groups, index tree, footer)
  bloom_filter.rs     # BloomFilter (used by both reader and writer)
```

**TsFileIOWriter ownership (replaces raw pointer lifecycle):**

```rust
// C++: TsFileIOWriter holds ByteStream write_stream_, ChunkGroupMeta*, ChunkMeta* (raw ptrs)
// Rust: owned values, Option for nullable state
pub struct TsFileIOWriter {
    writer: BufWriter<File>,
    position: u64,
    chunk_group_metas: Vec<ChunkGroupMeta>,
    current_chunk_group: Option<ChunkGroupMeta>,
    current_chunk_meta: Option<ChunkMeta>,
    schema: Schema,
    config: Arc<Config>,
}

impl TsFileIOWriter {
    pub fn new(file: File, config: Arc<Config>) -> Result<Self>;
    pub fn start_file(&mut self) -> Result<()>;              // writes magic + version
    pub fn start_chunk_group(&mut self, device_id: &DeviceId) -> Result<()>;
    pub fn start_chunk(&mut self, header: &ChunkHeader) -> Result<()>;
    pub fn write_page_data(&mut self, data: &[u8]) -> Result<()>;
    pub fn end_chunk(&mut self, statistic: &Statistic) -> Result<()>;
    pub fn end_chunk_group(&mut self) -> Result<()>;
    pub fn end_file(&mut self) -> Result<()>;                // writes index + bloom + footer
}
```

### C.5. `tsfile-writer` — Write Pipeline

**Replaces:** `cpp/src/writer/` (19 files)

```
src/
  lib.rs
  tsfile_writer.rs        # TsFileWriter (tree model + table model unified)
  table_writer.rs         # TsFileTableWriter (table-only convenience wrapper)
  chunk_writer.rs         # ChunkWriter (non-aligned)
  page_writer.rs          # PageWriter
  time_chunk_writer.rs    # TimeChunkWriter (aligned time column)
  time_page_writer.rs     # TimePageWriter
  value_chunk_writer.rs   # ValueChunkWriter (aligned value columns)
  value_page_writer.rs    # ValuePageWriter
```

**Ownership model (all owned by value, no raw pointers):**

```rust
pub struct TsFileWriter {
    io_writer: TsFileIOWriter,
    device_schemas: BTreeMap<DeviceId, DeviceSchemaGroup>,
    table_schemas: HashMap<String, TableSchema>,
    config: Arc<Config>,
    record_count_since_flush: u64,
}

struct DeviceSchemaGroup {
    schemas: BTreeMap<String, MeasurementEntry>,
    is_aligned: bool,
    time_chunk_writer: Option<TimeChunkWriter>,  // for aligned
}

struct MeasurementEntry {
    schema: MeasurementSchema,
    chunk_writer: Option<ChunkWriter>,          // non-aligned, created lazily
    value_chunk_writer: Option<ValueChunkWriter>, // aligned, created lazily
}

pub struct ChunkWriter {
    data_type: TSDataType,
    page_writer: PageWriter,          // owned by value
    chunk_data: Vec<u8>,              // accumulated page data
    chunk_statistic: Statistic,       // owned by value (enum)
    first_page_data: Option<PageData>,
    chunk_header: ChunkHeader,
    num_pages: u32,
}

pub struct PageWriter {
    data_type: TSDataType,
    time_encoder: Encoder,    // owned by value (enum, not Box)
    value_encoder: Encoder,   // owned by value (enum, not Box)
    compressor: Compressor,   // owned by value (enum, not Box)
    statistic: Statistic,     // owned by value (enum)
    time_buf: Vec<u8>,
    value_buf: Vec<u8>,
}
// PageWriter implements Drop automatically — no manual destroy() needed
```

**Replacing the C++ CW_DO_WRITE_FOR_TYPE macro:**

```rust
// C++: CW_DO_WRITE_FOR_TYPE macro generates 6 overloaded write() methods
// Rust: individual methods + a dynamic dispatch method
impl ChunkWriter {
    pub fn write_i32(&mut self, timestamp: i64, value: i32) -> Result<()> { ... }
    pub fn write_i64(&mut self, timestamp: i64, value: i64) -> Result<()> { ... }
    pub fn write_f32(&mut self, timestamp: i64, value: f32) -> Result<()> { ... }
    pub fn write_f64(&mut self, timestamp: i64, value: f64) -> Result<()> { ... }
    pub fn write_bool(&mut self, timestamp: i64, value: bool) -> Result<()> { ... }
    pub fn write_bytes(&mut self, timestamp: i64, value: &[u8]) -> Result<()> { ... }

    /// Dynamic dispatch for API boundaries
    pub fn write_value(&mut self, timestamp: i64, value: &TsValue) -> Result<()> {
        match (self.data_type, value) {
            (TSDataType::Int32, TsValue::Int32(v)) => self.write_i32(timestamp, *v),
            (TSDataType::Float, TsValue::Float(v)) => self.write_f32(timestamp, *v),
            // ...
            _ => Err(TsFileError::TypeMismatch { .. }),
        }
    }
}
```

**Public writer API:**

```rust
impl TsFileWriter {
    pub fn new(path: impl AsRef<Path>, config: Config) -> Result<Self>;

    // Schema registration
    pub fn register_timeseries(&mut self, device: &DeviceId, schema: MeasurementSchema) -> Result<()>;
    pub fn register_aligned_timeseries(&mut self, device: &DeviceId, schema: MeasurementSchema) -> Result<()>;
    pub fn register_table(&mut self, schema: TableSchema) -> Result<()>;

    // Write operations
    pub fn write_record(&mut self, record: &TsRecord) -> Result<()>;
    pub fn write_tablet(&mut self, tablet: &Tablet) -> Result<()>;
    pub fn write_aligned_record(&mut self, record: &TsRecord) -> Result<()>;
    pub fn write_aligned_tablet(&mut self, tablet: &Tablet) -> Result<()>;
    pub fn write_table(&mut self, tablet: &Tablet) -> Result<()>;

    // Lifecycle
    pub fn flush(&mut self) -> Result<()>;
    pub fn close(self) -> Result<()>;  // consumes self, ensures file is finalized
}
```

### C.6. `tsfile-reader` — Read Pipeline

**Replaces:** `cpp/src/reader/` (35+ files)

```
src/
  lib.rs
  tsfile_reader.rs         # TsFileReader facade
  chunk_reader.rs          # ChunkReader enum (Regular | Aligned)
  metadata_querier.rs      # MetadataQuerier with LRU cache
  scan_iterator.rs         # SeriesScanIterator
  query_executor.rs        # TreeQueryExecutor
  table_query_executor.rs  # TableQueryExecutor
  result_set.rs            # ResultSet implementations
  expression.rs            # QueryExpression, Expression tree
  filter/
    mod.rs                 # Filter trait (dyn dispatch — open set)
    time.rs                # TimeGt, TimeLt, TimeBetween, TimeEq, etc.
    value.rs               # ValueGt<T>, ValueEq<T>, etc.
    logical.rs             # AndFilter, OrFilter
  block/
    mod.rs
    single_device.rs       # SingleDeviceTsBlockReader
    device_ordered.rs      # DeviceOrderedTsBlockReader
```

**Filter system — trait objects (open set, runtime composition):**

```rust
// C++: Filter* base class with 20+ virtual subclasses, BinaryFilter holds Filter* left/right
// Rust: trait objects for runtime tree composition
pub trait Filter: Send + Sync {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool;
    fn satisfy(&self, time: i64, value: &TsValue) -> bool;
    fn satisfy_time_range(&self, start: i64, end: i64) -> bool;
    fn contain_time_range(&self, start: i64, end: i64) -> bool;
}

pub struct AndFilter {
    left: Box<dyn Filter>,
    right: Box<dyn Filter>,
}
impl Filter for AndFilter { ... }

pub struct TimeGt { pub value: i64 }
impl Filter for TimeGt {
    fn satisfy(&self, time: i64, _value: &TsValue) -> bool { time > self.value }
    fn satisfy_time_range(&self, _start: i64, end: i64) -> bool { end > self.value }
    fn satisfy_statistic(&self, stat: &Statistic) -> bool { stat.end_time() > self.value }
    ...
}
```

**ChunkReader — enum (closed set, 2 variants):**

```rust
// C++: IChunkReader* with ChunkReader and AlignedChunkReader
// Rust: enum
pub enum ChunkReader {
    Regular(RegularChunkReader),
    Aligned(AlignedChunkReader),
}

impl ChunkReader {
    pub fn load(file: &File, meta: &ChunkMeta, filter: Option<&dyn Filter>) -> Result<Self>;
    pub fn has_more_data(&self) -> bool;
    pub fn next_page(&mut self) -> Result<Option<TsBlock>>;
}
```

**Iterator-based result reading:**

```rust
// C++: ResultSet::next(bool& has_next) with out-parameter
// Rust: implement Iterator trait
pub struct ResultSet { ... }

impl Iterator for ResultSet {
    type Item = Result<RowRecord>;
    fn next(&mut self) -> Option<Self::Item> { ... }
}

// Also provide batch access
impl ResultSet {
    pub fn next_block(&mut self) -> Result<Option<TsBlock>>;
    pub fn metadata(&self) -> &ResultSetMetadata;
}
```

**Public reader API:**

```rust
impl TsFileReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;

    // Tree model queries
    pub fn query(
        &self, paths: &[&str], start_time: i64, end_time: i64
    ) -> Result<ResultSet>;

    // Table model queries
    pub fn query_table(
        &self, table: &str, columns: &[&str], start: i64, end: i64
    ) -> Result<TableResultSet>;

    // Schema introspection
    pub fn all_devices(&self) -> Result<Vec<DeviceId>>;
    pub fn device_schema(&self, device: &DeviceId) -> Result<Vec<MeasurementSchema>>;
    pub fn all_tables(&self) -> Result<Vec<TableSchema>>;
}
```

### C.7. `tsfile-ffi` — C Bindings

**Replaces:** `cpp/src/cwrapper/` (4 files)

Uses `cbindgen` to auto-generate `tsfile.h` from Rust `extern "C"` functions. Opaque handles via `Box::into_raw` / `Box::from_raw`. Matches the API surface of the existing `tsfile_cwrapper.h`.

---

## D. C++ Pattern -> Rust Idiom Summary

| C++ Pattern | Rust Idiom |
|---|---|
| Virtual base class (closed set: Encoder, Decoder, Compressor, ChunkReader, ResultSet, Statistic) | `enum` with match dispatch |
| Virtual base class (open set: Filter) | `dyn Trait` with `Box<dyn Filter>` |
| Template classes (GorillaEncoder<T>, TS2DIFFEncoder<T>) | Generic struct with sealed trait bound |
| Factory pattern (EncoderFactory, CompressorFactory) | `Encoder::new(encoding, data_type)` constructor |
| CW_DO_WRITE_FOR_TYPE macro (6 overloaded methods) | Individual typed methods + `write_value(&TsValue)` |
| `int ret; if (RET_FAIL(op())) return ret;` | `op()?;` with `Result<T, TsFileError>` |
| `mem_alloc(size, MOD_*) / mem_free(ptr)` | `Box::new()`, `Vec::new()`, standard allocator |
| PageArena (arena allocator) | `bumpalo::Bump` where needed, standard alloc otherwise |
| Placement new | Direct construction |
| Raw pointer ownership + manual delete | `Box<T>`, `Option<Box<T>>` |
| `shared_ptr<T>` | `Arc<T>` or `Rc<T>` |
| Conditional ownership (`write_file_created_` flag) | `Option<Box<T>>` vs `&T` (type-enforced) |
| `destroy()` method (manual cleanup) | `impl Drop` (automatic) |
| `g_config_value_` (mutable global) | `Arc<Config>` passed explicitly |
| `init_common()` (global init) | `Config::default()` at construction |
| `MutexGuard` (RAII for mutex only) | `std::sync::MutexGuard` (standard) |
| ANTLR4 full runtime (~100 files) | ~50-line hand-written path splitter |
| `ByteStream` (custom growable buffer) | `Vec<u8>` (write) / `&[u8]` with `Cursor` (read) |

---

## E. Implementation Order

### Phase 1: Foundation
1. `tsfile-common/error.rs` — `TsFileError` enum, `Result<T>` type alias
2. `tsfile-common/types.rs` — `TSDataType`, `TSEncoding`, `CompressionType` with `#[repr(u8)]` matching on-disk values
3. `tsfile-common/config.rs` — `Config` struct with `Default`
4. `tsfile-common/bitmap.rs` — `BitMap`
5. `tsfile-common/serialize.rs` — `WriteBuffer` and `ReadCursor` with varint/string serialization matching C++ `SerializationUtil`
6. `tsfile-common/value.rs` — `TsValue` enum
7. `tsfile-common/statistic.rs` — `Statistic` enum with serialize/deserialize
8. `tsfile-common/device_id.rs` — `DeviceId` with serialize/deserialize
9. `tsfile-common/path.rs` — path parser

**Milestone 1:** All common types compile. Serialization round-trips pass. Varint encoding matches C++ byte output.

### Phase 2: Encoding + Compression
10. `tsfile-encoding` — PlainEncoder/Decoder first (simplest)
11. `tsfile-encoding` — Gorilla, TS2Diff, RLE, Zigzag, Dictionary, Sprintz
12. `tsfile-encoding` — Encoder/Decoder enum dispatch
13. `tsfile-compress` — All 5 compressor variants
14. Cross-tests: encode -> compress -> decompress -> decode == original

**Milestone 2:** All encoders produce byte-identical output to C++ for same inputs. Compression round-trips pass.

### Phase 3: Format Structures + I/O
15. `tsfile-common/schema.rs` — MeasurementSchema, TableSchema
16. `tsfile-common/tsfile_format.rs` — ChunkHeader, PageHeader, ChunkMeta, MetaIndexNode, TsFileMeta with serialize/deserialize
17. `tsfile-io/bloom_filter.rs` — matching C++ seeds + murmur3
18. `tsfile-io/write_file.rs`, `read_file.rs`
19. `tsfile-io/io_writer.rs` — magic bytes, chunk flushing, index tree, footer

**Milestone 3:** Can write valid TsFile header + footer. All format struct serialization matches C++.

### Phase 4: Writer Pipeline
20. `tsfile-writer/page_writer.rs` — PageWriter
21. `tsfile-writer/chunk_writer.rs` — ChunkWriter
22. `tsfile-common/tablet.rs`, `tsfile-common/record.rs`
23. `tsfile-writer/tsfile_writer.rs` — TsFileWriter with `write_record()`, `write_tablet()`, `flush()`, `close()`
24. `tsfile-writer/time_chunk_writer.rs`, `value_chunk_writer.rs` — aligned writers
25. `tsfile-writer/table_writer.rs` — TsFileTableWriter

**Milestone 4:** Can write complete `.tsfile` readable by C++ reader. Binary compatibility verified.

### Phase 5: Reader Pipeline
26. `tsfile-reader/filter/` — Filter trait + all concrete filters
27. `tsfile-reader/chunk_reader.rs` — ChunkReader with page decode
28. `tsfile-reader/aligned_chunk_reader.rs`
29. `tsfile-reader/metadata_querier.rs` — B-tree index navigation
30. `tsfile-reader/scan_iterator.rs` — SeriesScanIterator
31. `tsfile-reader/result_set.rs` — ResultSet with Iterator impl
32. `tsfile-reader/tsfile_reader.rs` — TsFileReader facade
33. `tsfile-reader/table_query_executor.rs`, `table_result_set.rs`

**Milestone 5:** Full round-trip: write with Rust -> read with Rust. Read C++-written files. Read Rust-written files with C++.

### Phase 6: FFI + Polish
34. `tsfile-ffi/` — C bindings matching `tsfile_cwrapper.h`
35. `tsfile/` — facade crate with documentation
36. Benchmarks with `criterion`
37. Examples, README

**Milestone 6:** Published crate with C FFI, examples, benchmarks.

---

## F. Testing Strategy

### Binary Compatibility (highest priority)
- Generate reference `.tsfile` files from C++ covering every data type, encoding, compression combination
- Store in `test-data/`
- Test: Rust reader reads C++-written files and verifies all values
- Test: C++ reader reads Rust-written files and verifies all values

### Serialization Compatibility
- For every serializable struct (ChunkHeader, PageHeader, ChunkMeta, Statistic, MetaIndexNode, BloomFilter): verify byte-identical output vs C++ reference bytes
- Varint encoding must match exactly — extract known pairs from C++ for targeted tests

### Encoder/Decoder Property Testing (`proptest`)
- Round-trip: encode -> decode == original for all algorithm x type combinations
- Edge cases: empty, single value, INT32_MIN/MAX, NaN, +/-Infinity
- Monotonic sequences (important for TS_2DIFF)

### Compression Property Testing
- Round-trip for all 5 compression types with random data 0-10KB

### End-to-End Tests
- Multi-device, multi-measurement, multi-type files
- Aligned + non-aligned timeseries
- Table model with TAG + FIELD columns
- Time range filter queries with verification
- Large tablets (100K+ rows)

### Performance Benchmarks (`criterion`)
- Encode/decode throughput per algorithm
- Compress/decompress throughput
- Full write pipeline (MB/s)
- Full read pipeline (rows/s)
- Compare against C++ baseline

---

## G. Critical C++ Files for Reference During Implementation

These files define the on-disk format and must be studied carefully for byte compatibility:

| File | What it defines |
|---|---|
| `cpp/src/common/db_common.h` | Enum discriminant values (TSDataType, TSEncoding, CompressionType) |
| `cpp/src/common/tsfile_common.h` | On-disk structures: ChunkHeader, PageHeader, ChunkMeta, MetaIndexNode, TsFileMeta serialization |
| `cpp/src/common/statistic.h` | Per-type statistic serialization format |
| `cpp/src/encoding/encoder_factory.h` | Valid (encoding x data_type) combinations |
| `cpp/src/file/tsfile_io_writer.h` | File structure: magic, chunk groups, index building, footer |
| `cpp/src/file/tsfile_io_reader.h` | Metadata parsing and index navigation |
| `cpp/src/reader/bloom_filter.h` | Bloom filter seed array and hash function |
| `cpp/src/common/allocator/byte_stream.h` | Varint encoding format (used throughout serialization) |