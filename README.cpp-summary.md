# TsFile C++ Implementation Reference

> Version: 2.2.0.dev | C++11 | CMake 3.11+ | License: Apache 2.0

This project implements the TsFile time-series file format. The C++ implementation lives in `cpp/`. Java and Python wrappers exist but are not the focus.

## Architecture Overview

TsFile stores time-series data in a columnar format with hierarchical indexing. The C++ library supports two data models:

- **Tree model**: Device/measurement hierarchy (e.g., `root.sg1.d1.temperature`)
- **Table model**: Relational-style with TAG (identity) and FIELD (measurement) columns

### Data Flow

```
WRITE PATH:
  Tablet/TsRecord
    -> TsFileWriter (schema routing, memory management)
      -> ChunkWriter / TimeChunkWriter+ValueChunkWriter[] (page management)
        -> PageWriter / TimePageWriter+ValuePageWriter (encoding + statistics)
          -> Encoder (Plain, Gorilla, RLE, TS2Diff, Sprintz, Dictionary, ZigZag)
            -> Compressor (Snappy, GZIP, LZ4, LZO, Uncompressed)
              -> TsFileIOWriter -> WriteFile (disk)

READ PATH:
  ReadFile -> TsFileIOReader (metadata + index loading)
    -> MetadataQuerier (LRU-cached chunk metadata)
      -> TsFileSeriesScanIterator (per-series chunk iteration)
        -> ChunkReader / AlignedChunkReader (page decoding)
          -> Compressor::uncompress -> Decoder
            -> TsBlock (columnar in-memory result)
              -> ResultSet / TableResultSet (user-facing iteration)
```

### Module Dependency Graph

```
              +------------------+
              |    C Wrapper     |  (cwrapper/)
              +--------+---------+
                       |
          +------------+------------+
          |                         |
  +-------v-------+       +--------v--------+
  | TsFileWriter   |       | TsFileExecutor   |
  | TreeWriter     |       | TableQueryExec   |  (reader/)
  | TableWriter    |       | TreeReader       |
  +-------+-------+       +--------+---------+
          |                         |
  +-------v-------+       +--------v---------+
  | ChunkWriter    |       | ChunkReader       |
  | TimeChunkWriter|       | AlignedChunkReader|
  | ValueChunkWriter|      | SeriesScanIterator|
  +-------+-------+       +--------+---------+
          |                         |
  +-------v-------+       +--------v---------+
  | PageWriter     |       | Decoder           |  (encoding/)
  | TimePageWriter |       | DecoderFactory    |
  | ValuePageWriter|       +---------+---------+
  +-------+-------+                 |
          |                         |
  +-------v-------+       +--------v---------+
  | Encoder        |       | Compressor        |  (compress/)
  | EncoderFactory |       | CompressorFactory |
  +-------+-------+       +---------+---------+
          |                         |
  +-------v-------------------------v---------+
  |            TsFileIOWriter / Reader         |  (file/)
  |            WriteFile / ReadFile             |
  +--------------------+----------------------+
                       |
  +--------------------v----------------------+
  |              Common Module                 |  (common/)
  |  Schema, Tablet, TsBlock, Statistic,       |
  |  DeviceID, ByteStream, PageArena, Config   |
  +-------------------------------------------+
```

## Directory Structure

```
cpp/
  src/
    common/          # Shared types, containers, memory management
    compress/        # Compression algorithms (GZIP, LZ4, LZO, Snappy)
    encoding/        # Encoding algorithms (Plain, RLE, Gorilla, etc.)
    file/            # Low-level file I/O and TsFile format handling
    reader/          # Query execution, chunk reading, filtering
    writer/          # Data writing pipeline (chunk/page writers)
    parser/          # ANTLR4-based path expression parsing
    cwrapper/        # C language API bindings
    utils/           # Error codes, utilities
  test/              # Google Test unit tests (mirrors src/ structure)
  examples/          # C and C++ usage examples
  bench_mark/        # Performance benchmarks
  third_party/       # vendored: ANTLR4, LZ4, lzokay, Snappy, zlib-1.3.1
```

## Module Details

### 1. Common (`src/common/`)

Core data structures and utilities shared across all modules.

**Type System** (`db_common.h`):
- `TSDataType`: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, DATE, TIMESTAMP, BLOB, VECTOR
- `TSEncoding`: PLAIN, DICTIONARY, RLE, TS_2DIFF, GORILLA, ZIGZAG, SPRINTZ
- `CompressionType`: UNCOMPRESSED, SNAPPY, GZIP, LZO, LZ4

**Schema** (`schema.h`):
- `MeasurementSchema` - column-level: name, data type, encoding, compression
- `MeasurementSchemaGroup` - groups measurements per device, tracks alignment
- `TableSchema` - table-level: table name, column schemas with TAG/FIELD categories, position index
- `ColumnCategory`: TAG (identity/key columns) vs FIELD (measurement/value columns)

**Data Containers**:
- `Tablet` (`tablet.h`) - batch insert container with timestamps[], value_matrix[] (union arrays per type), and null bitmaps. Supports both device-id based and table-name based addressing.
- `TsRecord` / `DataPoint` (`record.h`) - single-row record with typed data points
- `TsBlock` (`tsblock/tsblock.h`) - columnar in-memory batch with RowAppender, ColAppender, RowIterator, ColIterator. Capacity-limited by config.
- `RowRecord` / `Field` (`row_record.h`) - row-oriented result container with union-based typed fields

**Device Identity** (`device_id.h`):
- `IDeviceID` interface with `StringArrayDeviceID` implementation
- Parses dot-separated paths into segments via ANTLR4 parser
- Supports comparison operators for use in ordered maps
- Segments stored as `vector<string*>` with proper lifecycle management

**Statistics** (`statistic.h`):
- Base `Statistic`: count, start_time, end_time
- Typed subclasses: BooleanStatistic, Int32Statistic, Int64Statistic, FloatStatistic, DoubleStatistic, StringStatistic, TextStatistic, TimeStatistic
- Each tracks type-appropriate min/max/sum/first/last values
- Created via `StatisticFactory::alloc_statistic(TSDataType)`

**Memory Management**:
- `PageArena` (`allocator/page_arena.h`) - page-based arena allocator
- `ByteStream` (`allocator/byte_stream.h`) - growable byte buffer for serialization
- `mem_alloc`/`mem_free` with module-tagged allocation tracking

**Configuration** (`config/config.h`):
- Global `g_config_value_` struct with tunable parameters:
  - `page_writer_max_point_num_` / `page_writer_max_memory_bytes_` - page flush thresholds
  - `max_degree_of_index_node_` - B-tree index node fan-out
  - `tsfile_index_bloom_filter_error_percent_` - bloom filter FP rate
  - Per-type default encodings and default compression type

**Containers** (`container/`): Array, BitMap, BlockingQueue, ByteBuffer, HashTable (with MurmurHash3), List, SimpleVector, Slice, SortedArray

**Other**: LRU cache (`cache/lru_cache.h`), mutex wrappers, logging (`logger/elog.h`)

### 2. Encoding (`src/encoding/`)

Header-only module (34 files). All encoders/decoders implement virtual interfaces.

**Encoder interface** (`encoder.h`):
- `encode(value, ByteStream&)` - type-overloaded for bool/int32/int64/float/double/String
- `flush(ByteStream&)`, `reset()`, `destroy()`, `get_max_byte_size()`

**Decoder interface** (`decoder.h`):
- `read_boolean/int32/int64/float/double/String()` - type-specific reads
- `has_remaining()`, `reset()`

**Encoding algorithms**:

| Encoding | Types Supported | Strategy |
|----------|----------------|----------|
| PLAIN | All | Direct serialization, var-int for int32 |
| RLE | INT32, INT64 | Run-length encoding with bit-packing |
| GORILLA | INT32, INT64, FLOAT, DOUBLE | XOR-based delta with leading/trailing zero compression |
| TS_2DIFF | INT32, INT64, FLOAT, DOUBLE | Second-order delta encoding |
| DICTIONARY | STRING/TEXT | String-to-index map + RLE-encoded indices |
| ZIGZAG | INT32, INT64 | Variable-length signed integer encoding |
| SPRINTZ | INT32, INT64, FLOAT, DOUBLE | Predictive delta with bit-packing |

**Factories** (`encoder_factory.h`, `decoder_factory.h`):
- `EncoderFactory::alloc_time_encoder(TSEncoding)` - for timestamp columns
- `EncoderFactory::alloc_value_encoder(TSEncoding, TSDataType)` - for value columns
- `DecoderFactory::alloc_time_decoder(TSEncoding)` / `alloc_value_decoder(...)`
- Memory allocated via `MOD_ENCODER_OBJ` / `MOD_DECODER_OBJ` tags

### 3. Compression (`src/compress/`)

**Compressor interface** (`compressor.h`):
- `reset(bool for_compress)`, `compress(buf, size, out, out_size)`, `uncompress(...)`, `destroy()`

**Implementations**: GzipCompressor (zlib), LZ4Compressor, LZOCompressor (lzokay), SnappyCompressor, UncompressedCompressor

**Factory** (`compressor_factory.h`):
- `CompressorFactory::alloc_compressor(CompressionType)` / `free(Compressor*)`

### 4. Writer (`src/writer/`)

Hierarchical write pipeline with two paths: non-aligned and aligned.

**High-level APIs**:
- `TsFileWriter` - core writer with schema registration, memory-threshold flushing, dual path routing
- `TsFileTreeWriter` - thin wrapper for tree-model (device/measurement) writes
- `TsFileTableWriter` - thin wrapper for table-model writes, enforces single table per writer

**Non-aligned path** (one ChunkWriter per measurement):
- `ChunkWriter` -> `PageWriter` -> time_encoder + value_encoder + compressor
- ChunkWriter manages page lifecycle: fills pages, seals when full (point count or memory threshold), buffers into chunk ByteStream

**Aligned path** (shared time column + separate value columns):
- `TimeChunkWriter` -> `TimePageWriter` -> time_encoder only, enforces time ordering
- `ValueChunkWriter` -> `ValuePageWriter` -> value_encoder + null bitmap tracking
- Aligned writes share a single time column across multiple value columns

**Page sealing triggers**: `page_writer_max_point_num_` (point count) or `page_writer_max_memory_bytes_` (memory)

**Memory management**: `check_memory_size_and_may_flush_chunks()` called periodically based on `record_count_for_next_mem_check_` config. When chunk group size exceeds threshold, flushes to disk via TsFileIOWriter.

### 5. Reader (`src/reader/`)

Largest module (30+ files). Supports tree-model and table-model queries.

**Query execution**:
- `TsFileExecutor` - tree-model query orchestrator. Takes `QueryExpression` (selected series + filter tree), creates per-series iterators.
- `TableQueryExecutor` - table-model orchestrator. Takes table name, columns, time/id/field filters. Returns `TableResultSet`.
- `TsFileTreeReader` - high-level tree-model API: `open()`, `query(devices, measurements, time_range)`, `close()`

**Chunk reading**:
- `ChunkReader` (implements `IChunkReader`) - reads non-aligned chunks. Loads chunk by metadata, decodes pages into TsBlock.
- `AlignedChunkReader` (implements `IChunkReader`) - reads aligned chunks with separate time/value streams and null bitmap handling.

**Series scanning**:
- `TsFileSeriesScanIterator` - iterates all chunks for a single measurement within a device. Creates appropriate ChunkReader type.
- `DataScanIterator` / `DataRun` - manages multiple data sources (in-memory TVList or on-disk TsFile)

**Filter system** (`reader/filter/`):
- Base `Filter` class with `satisfy(Statistic*)`, `satisfy(time, value)`, `satisfy_start_end_time()`
- Comparison filters: Eq, NotEq, Gt, GtEq, Lt, LtEq
- Logical filters: AndFilter, OrFilter
- Range filters: Between, In
- Time-specific: TimeFilter, TimeOperator
- Filters applied at three levels: chunk statistics, page statistics, individual rows

**Expression system** (`expression.h`):
- Tree-based: AND_EXPR, OR_EXPR, SERIES_EXPR, GLOBALTIME_EXPR
- `QueryExpression` wraps selected series paths + filter expression tree
- Supports `optimize()` for expression simplification

**Metadata** (`meta_data_querier.h`):
- `MetadataQuerier` (implements `IMetadataQuerier`) - LRU-cached (1000 entries) chunk metadata lookup
- Provides `get_chunk_metadata_list(Path)`, `device_iterator()`, `get_whole_file_metadata()`

**Block readers** (`reader/block/`):
- `SingleDeviceTsBlockReader` - reads time-aligned blocks from multiple measurements for one device
- `DeviceOrderedTsBlockReader` - reads blocks in device order across devices
- `MeasurementColumnContext` / `IdColumnContext` - column value sources for table model

**Bloom filter** (`bloom_filter.h`): Murmur3-based probabilistic path existence check for fast filtering.

### 6. File I/O (`src/file/`)

**Low-level**:
- `ReadFile` - POSIX read with offset-based random access, validates TsFile magic bytes and version
- `WriteFile` - POSIX sequential write with sync support

**High-level**:
- `TsFileIOReader` - lazy metadata loading, device index navigation (binary search), series scan iterator allocation via `alloc_ssi(device_id, measurement_name)`
- `TsFileIOWriter` - manages file lifecycle: `start_file()` (writes magic+version), chunk group flush, index tree construction, bloom filter, `end_file()` (writes metadata footer). Uses 512-byte buffered `ByteStream`.

**TsFile format on disk**:
```
[Magic "TsFile"] [Version]
[ChunkGroup 1: ChunkHeader + Pages ...]
[ChunkGroup 2: ...]
...
[MetaIndexTree: device nodes -> measurement nodes -> chunk offsets]
[TsFileMeta: table schemas, bloom filter, meta_offset]
[Magic "TsFile"]
```

### 7. Parser (`src/parser/`)

ANTLR4-based path expression parser for hierarchical device/measurement names.

- Grammar files: `PathLexer.g4`, `PathParser.g4`
- `PathNodesGenerator::invokeParser(path)` - tokenizes "root.sg1.d1" into `["root", "sg1", "d1"]`
- `PathVisitor` - ANTLR visitor implementing semantic validation
- Generated code in `parser/generated/` - auto-generated lexer/parser/visitor C++ code

### 8. C Wrapper (`src/cwrapper/`)

C-compatible API for FFI integration (used by Python bindings).

**Key functions** (`tsfile_cwrapper.h`):
- Configuration: `ts_config_set_*()` for default encoding/compression
- File management: `write_file_new()`, `tsfile_reader_new()`, `tsfile_writer_new()`
- Schema: `column_schema_new()`, `table_schema_new()`, `tsfile_writer_register_table_schema()`
- Data: `tablet_new()`, `tablet_add_timestamp()`, `tablet_add_value_by_name_*()`, `tsfile_writer_write_tablet()`
- Query: `tsfile_query_table()`, `tsfile_result_set_next()`, `tsfile_result_set_get_value_by_name_*()`
- Memory: `tsfile_writer_close()`, `tsfile_reader_close()`, `tablet_destroy()`

All functions return `ERRNO` (int32_t) status codes. Opaque pointer handles for C++ objects.

Expression support (`tsfile_cwrapper_expression.h`) is currently commented out / disabled.

### 9. Utils (`src/utils/`)

- `errno_define.h` - 54 error codes: E_OK(0), E_OOM(1), E_NOT_EXIST(2), E_INVALID_ARG(4), E_OUT_OF_ORDER(22), E_TSFILE_CORRUPTED(35), E_ENCODE_ERR(53), E_DECODE_ERR(54), etc.
- `db_utils.h`, `storage_utils.h` - database and storage helpers
- `util_define.h` - general macros and definitions
- `injection.h` - fault injection hooks for testing

## Key Data Structures (TsFile Format)

| Structure | Location | Purpose |
|-----------|----------|---------|
| `PageHeader` | `tsfile_common.h` | Page metadata: compressed/uncompressed sizes, optional statistics |
| `ChunkHeader` | `tsfile_common.h` | Chunk metadata: measurement name, data type, encoding, compression, page count |
| `ChunkMeta` | `tsfile_common.h` | Chunk index entry: header offset, statistics, mask |
| `ChunkGroupMeta` | `tsfile_common.h` | Groups chunk metadata by device |
| `TimeseriesIndex` | `tsfile_common.h` | Maps measurement to its chunk metadata list |
| `AlignedTimeseriesIndex` | `tsfile_common.h` | Paired time + value timeseries indices |
| `MetaIndexNode/Entry` | `tsfile_common.h` | B-tree-like index nodes (INTERNAL/LEAF for device/measurement) |
| `TsFileMeta` | `tsfile_common.h` | File-level: index tree root, table schemas, bloom filter, meta offset |

## Error Handling Pattern

Functions return `int` error codes (from `utils/errno_define.h`). Common pattern:

```cpp
int ret = E_OK;
if (RET_FAIL(some_operation())) {
    return ret;
}
```

No exceptions. Error propagation via return values throughout.

## Memory Allocation Pattern

Custom allocator with module-tagged tracking:

```cpp
void* buf = common::mem_alloc(sizeof(T), common::MOD_*_OBJ);
T* obj = new(buf) T(args...);
// ...
obj->~T();
common::mem_free(buf);
```

Factory classes (EncoderFactory, DecoderFactory, CompressorFactory, StatisticFactory) encapsulate alloc/free pairs.

## Build System

```bash
cd cpp && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

Output: `build/lib/libtsfile.so`

Options:
- `-DCOV_ENABLED=ON` - code coverage
- Address sanitizer support available
- Build types: Debug, Release, RelWithDebInfo, MinSizeRel

Tests: Google Test framework, run via `./test_all.sh` or `ctest`

## Third-Party Dependencies (vendored in `third_party/`)

| Library | Purpose | Used By |
|---------|---------|---------|
| ANTLR4 C++ Runtime | Parser generator runtime | parser/ module |
| LZ4 | Fast compression | compress/ |
| lzokay | LZO-compatible compression | compress/ |
| Google Snappy | Fast compression | compress/ |
| zlib 1.3.1 | GZIP/DEFLATE compression | compress/ |

## Conventions

- Code style: Google C++ style (`.clang-format`)
- Header-only modules: encoding/ (all 34 files are headers)
- Macro-based write dispatch: `CW_DO_WRITE_FOR_TYPE`, `PW_DO_WRITE_FOR_TYPE` for type-safe inlined writes
- FORCE_INLINE on hot-path methods
- Two data models coexist: tree (device hierarchy) and table (relational) with shared underlying storage
