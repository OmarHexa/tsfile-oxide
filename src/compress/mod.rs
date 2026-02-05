// C++ compress/ has a Compressor* base class with 5 implementations and
// a CompressorFactory. The C++ compressors carry mutable state and require
// reset()/destroy() lifecycle methods. In Rust, the compression crates
// (snap, flate2, lz4_flex) are stateless per-call, so we use a simple
// enum with compress/decompress methods — no factory, no lifecycle.
//
// The Compressor enum replaces:
// - C++ Compressor* virtual base class
// - C++ CompressorFactory::alloc_compressor()
// - C++ lifecycle methods: reset(), after_compress(), after_uncompress(), destroy()
//
// Rust handles resource cleanup automatically via Drop, and the compression
// libraries are stateless (no need to carry state between calls).

mod compressor;

pub use compressor::Compressor;
