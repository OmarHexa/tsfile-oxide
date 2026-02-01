// tsfile-oxide: Idiomatic Rust implementation of the Apache TsFile format.
//
// This crate is structured as a single flat crate (not a workspace) for
// simplicity during development. Modules mirror the C++ directory layout
// (common/, encoding/, compress/, file/, writer/, reader/) but use Rust
// idioms throughout — enums over virtual dispatch, Result<T> over error
// codes, and owned values over raw pointers.

// === Foundation modules (Phase 1) ===
// These correspond to C++ src/common/ and src/utils/.

pub mod bitmap;
pub mod config;
pub mod device_id;
pub mod error;
pub mod path;
pub mod schema;
pub mod serialize;
pub mod statistic;
pub mod tsfile_format;
pub mod types;
pub mod value;

// === Encoding module (Phase 2) ===
// Replaces C++ src/encoding/ (34 header-only files).
// Uses enum dispatch instead of virtual Encoder*/Decoder* hierarchies.
pub mod encoding;

// === Compression module (Phase 2) ===
// Replaces C++ src/compress/ (14 files).
// Wraps snap/flate2/lz4_flex behind a Compressor enum.
pub mod compress;

// === File I/O module (Phase 3) ===
// Replaces C++ src/file/ (11 files).
// Handles TsFile on-disk format: magic bytes, chunk groups, index tree, footer.
pub mod io;

// === Writer module (Phase 4) ===
// Replaces C++ src/writer/ (19 files).
// Pipeline: TsFileWriter -> ChunkWriter -> PageWriter -> Encoder -> Compressor -> disk
pub mod writer;

// === Reader module (Phase 5) ===
// Replaces C++ src/reader/ (35+ files).
// Pipeline: disk -> TsFileIOReader -> ChunkReader -> Decoder -> TsBlock -> ResultSet
pub mod reader;
