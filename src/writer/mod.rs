// C++ writer/ has 19 files implementing a hierarchical write pipeline:
// TsFileWriter -> ChunkWriter -> PageWriter with separate aligned
// (TimeChunkWriter + ValueChunkWriter[]) and non-aligned paths.
//
// Key C++ -> Rust difference: C++ uses raw pointers for ownership
// (ChunkWriter* held by TsFileWriter, PageWriter* held by ChunkWriter).
// In Rust, all components are owned by value — no heap indirection,
// no manual delete, and Drop handles cleanup automatically.
