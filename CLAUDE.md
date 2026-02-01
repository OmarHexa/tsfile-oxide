# CLAUDE.md — tsfile-oxide

## Purpose

This is an **educational rewrite** of the Apache TsFile C++ library in idiomatic Rust.
The developer's primary goals are:

1. **Learning how C++ patterns translate to Rust** — every module should make clear
   what the original C++ did and why the Rust version differs.
2. **Understanding how a large codebase integrates** — the TsFile format spans
   encoding, compression, file I/O, indexing, query execution, and schema management.
   Each module should document how it fits into the whole.
3. **Producing binary-compatible `.tsfile` output** — files written by this crate
   must be readable by the C++ implementation and vice versa.

## Project Context

- **Reference implementation:** The C++ source lives outside this repo. See
  `README.cpp-summary.md` for a complete architectural reference.
- **Rewrite roadmap:** See `README.roadmap.md` for the phased implementation plan,
  module-by-module Rust designs, and the C++ pattern -> Rust idiom mapping table.
- **This is a single-crate design** (not the workspace layout from the roadmap).
  All modules live under `src/` as a flat crate for simplicity during development.

## Architecture

```
src/
  lib.rs                 # crate root — module declarations, re-exports
  error.rs               # TsFileError enum, Result<T> alias
  types.rs               # TSDataType, TSEncoding, CompressionType (#[repr(u8)])
  config.rs              # Config struct (replaces C++ global g_config_value_)
  bitmap.rs              # BitMap for null tracking
  serialize.rs           # varint/string serialization (must match C++ byte layout)
  value.rs               # TsValue enum for dynamic type dispatch
  statistic.rs           # Statistic enum with typed variants
  device_id.rs           # DeviceId (replaces C++ IDeviceID virtual hierarchy)
  path.rs                # Hand-written path parser (replaces ANTLR4 runtime)
  schema.rs              # MeasurementSchema, TableSchema, ColumnCategory
  tsfile_format.rs       # On-disk structures: ChunkHeader, PageHeader, ChunkMeta, etc.

  encoding/              # Encoder/Decoder enums + algorithm implementations
    mod.rs
    encoder.rs
    decoder.rs
    plain.rs
    rle.rs
    gorilla.rs
    ts2diff.rs
    zigzag.rs
    sprintz.rs
    dictionary.rs
    bit_packer.rs

  compress/              # Compressor enum + algorithm wrappers
    mod.rs
    compressor.rs

  io/                    # File I/O and TsFile format handling
    mod.rs
    read_file.rs
    write_file.rs
    io_reader.rs
    io_writer.rs
    bloom_filter.rs

  writer/                # Write pipeline: TsFileWriter -> ChunkWriter -> PageWriter
    mod.rs
    tsfile_writer.rs
    chunk_writer.rs
    page_writer.rs
    time_chunk_writer.rs
    time_page_writer.rs
    value_chunk_writer.rs
    value_page_writer.rs
    table_writer.rs

  reader/                # Read pipeline: TsFileReader -> ChunkReader -> Decoder
    mod.rs
    tsfile_reader.rs
    chunk_reader.rs
    metadata_querier.rs
    scan_iterator.rs
    query_executor.rs
    table_query_executor.rs
    result_set.rs
    expression.rs
    filter/
      mod.rs
      time.rs
      value.rs
      logical.rs
    block/
      mod.rs
      single_device.rs
      device_ordered.rs
```

## Data Flow

```
WRITE: Tablet -> TsFileWriter -> ChunkWriter -> PageWriter -> Encoder -> Compressor -> disk
READ:  disk -> TsFileIOReader -> ChunkReader -> Decoder -> Decompressor -> TsBlock -> ResultSet
```

## Code Conventions

### Comment Philosophy

Every non-trivial implementation should include a brief comment explaining the
**design choice**, not just what the code does. The reader is learning how C++
idioms map to Rust. Examples of good comments:

```rust
// C++ uses a virtual Statistic* hierarchy with 8 subclasses. In Rust we use an
// enum — the set of types is closed and known at compile time, so enum dispatch
// avoids heap allocation and vtable overhead while keeping exhaustive matching.
pub enum Statistic {
    Boolean { count: u64, ... },
    Int32 { count: u64, ... },
    ...
}

// C++ global g_config_value_ is a mutable static accessed everywhere. In Rust
// we pass Config explicitly via Arc<Config> to avoid global mutable state and
// make dependencies visible in function signatures.
pub struct Config { ... }
```

Bad comments (avoid these):
```rust
// Create a new Config  <-- just restates the code
pub fn new() -> Config { ... }
```

### Error Handling

Use `Result<T, TsFileError>` with the `?` operator everywhere. This replaces the
C++ pattern of `int ret = E_OK; if (RET_FAIL(op())) return ret;`. The `thiserror`
crate generates Display/Error impls from the enum.

### Naming

- Rust naming conventions: `snake_case` for functions/variables, `CamelCase` for types.
- Keep names close to the C++ originals where it aids cross-referencing
  (e.g., `ChunkWriter`, `PageWriter`, `TsFileIOWriter`).
- Use Rust module paths instead of C++ name prefixes
  (e.g., `encoding::Encoder` not `EncodingEncoder`).

### C++ to Rust Patterns (Quick Reference)

| When you see in C++              | Use in Rust                                    |
|----------------------------------|------------------------------------------------|
| Virtual base class (closed set)  | `enum` with match dispatch                     |
| Virtual base class (open set)    | `dyn Trait` with `Box<dyn Trait>`              |
| Template class `Foo<T>`          | Generic struct with trait bound                |
| Factory pattern                  | `Type::new(variant, ...)` constructor          |
| `int` error codes + `RET_FAIL`  | `Result<T>` + `?` operator                    |
| `mem_alloc` / `mem_free`        | Standard allocation (`Box`, `Vec`)             |
| Placement new                    | Direct construction                            |
| Raw pointer ownership            | `Box<T>`, `Option<T>`                          |
| `shared_ptr`                     | `Arc<T>`                                       |
| `destroy()` method               | `impl Drop` (automatic)                        |
| Mutable global config            | `Arc<Config>` passed explicitly                |
| ANTLR4 parser runtime           | Hand-written parser (~50 lines)                |
| `ByteStream` (custom buffer)    | `Vec<u8>` (write) / `Cursor<&[u8]>` (read)    |

### Binary Compatibility

The on-disk `.tsfile` format must match the C++ implementation byte-for-byte.
Critical areas:

- **Magic bytes:** `"TsFile"` at file start and end
- **Varint encoding:** Must match `SerializationUtil` exactly
- **Enum discriminants:** `#[repr(u8)]` values must match `db_common.h`
- **Bloom filter:** Same MurmurHash3 seeds, same bit layout
- **Statistics serialization:** Per-type format must match `statistic.h`
- **Index tree:** MetaIndexNode structure must match for cross-reader compatibility

### Test-Driven Development

This project follows **test-driven development**. Every module is implemented
alongside its unit tests — write tests for the core functionality *before or
during* implementation, not as an afterthought. No phase is considered complete
until its tests pass.

**Rules:**

1. Every public function and every non-trivial internal function gets at least
   one test covering its expected behavior.
2. Every error path that returns `Err(...)` gets a test proving it triggers.
3. Tests live in a `#[cfg(test)] mod tests` block at the bottom of the file
   they test — keep tests co-located with the code they exercise.
4. Tests should be self-contained: no shared mutable state, no ordering
   dependencies between tests.

**What to test per phase:**

- **Phase 1 (Foundation):** Enum discriminant values match C++ (`#[repr(u8)]`),
  Config defaults match `init_config_value()`, BitMap get/set/clear round-trips,
  varint encode/decode round-trips with known C++ reference bytes, Statistic
  construction and serialization, DeviceId parsing and ordering, path parser
  with quoting edge cases.
- **Phase 2 (Encoding + Compression):** Encode -> decode round-trip for every
  (algorithm x data type) combination, edge cases (empty input, single value,
  MIN/MAX, NaN, +/-Infinity, monotonic sequences for TS_2DIFF), property-based
  tests with `proptest` for randomized round-trips, compress -> decompress
  round-trip for all 5 compression types.
- **Phase 3 (Format + I/O):** Serialization round-trip for every on-disk struct
  (ChunkHeader, PageHeader, ChunkMeta, MetaIndexNode, TsFileMeta), bloom filter
  hash output matches C++ reference values, file magic/version write and read-back.
- **Phase 4 (Writer Pipeline):** PageWriter seals at configured thresholds,
  ChunkWriter accumulates pages correctly, TsFileWriter produces valid file
  structure (magic + chunks + index + footer), type mismatch and out-of-order
  timestamp errors trigger correctly.
- **Phase 5 (Reader Pipeline):** Filter satisfy/reject on known inputs, chunk
  reader decodes pages written by the writer, full round-trip (write -> read ->
  compare values), cross-compatibility with C++ reference `.tsfile` files in
  `test-data/`.
- **Phase 6 (FFI + Polish):** C FFI functions return correct error codes, opaque
  handle lifecycle (create -> use -> destroy) doesn't leak.

**Testing tools:**

- `cargo test` — inline `#[cfg(test)]` modules for unit tests
- `proptest` — property-based testing for encoder/decoder/compression round-trips
- `pretty_assertions` — readable diff output on test failures
- `tempfile` — temporary files for I/O integration tests
- `criterion` — benchmarks in `benches/`

## Build & Run

```bash
cargo build
cargo test
cargo bench
```

## Implementation Phases

We follow the roadmap in `README.roadmap.md`. Each phase ships with its unit
tests — a phase is **not complete** until `cargo test` passes with coverage of
every core function introduced in that phase.

1. **Foundation** — error, types, config, bitmap, serialize, value, statistic, device_id, path
   - Tests: repr values, config defaults, bitmap ops, varint round-trips, path parsing
2. **Encoding + Compression** — all encoder/decoder algorithms, compressor wrappers
   - Tests: round-trip per (algorithm x type), proptest randomized, edge cases, compression round-trip
3. **Format + I/O** — schema, on-disk structures, bloom filter, file reader/writer
   - Tests: struct serialization round-trips, bloom filter hash match, magic/version I/O
4. **Writer Pipeline** — page/chunk/file writers for tree and table models
   - Tests: page seal thresholds, chunk accumulation, valid file structure, error paths
5. **Reader Pipeline** — filters, chunk readers, query executors, result sets
   - Tests: filter logic, chunk decode, full write->read round-trip, C++ file compat
6. **FFI + Polish** — C bindings, facade, benchmarks, examples
   - Tests: FFI error codes, handle lifecycle, no leaks
