// C++ writer/ has 19 files implementing a hierarchical write pipeline:
// TsFileWriter -> ChunkWriter -> PageWriter with separate aligned
// (TimeChunkWriter + ValueChunkWriter[]) and non-aligned paths.
//
// Key C++ -> Rust difference: C++ uses raw pointers for ownership
// (ChunkWriter* held by TsFileWriter, PageWriter* held by ChunkWriter).
// In Rust, all components are owned by value — no heap indirection,
// no manual delete, and Drop handles cleanup automatically.

pub mod chunk_writer;
pub mod page_writer;
pub mod time_chunk_writer;
pub mod time_page_writer;
pub mod tsfile_writer;
pub mod table_writer;
pub mod value_chunk_writer;
pub mod value_page_writer;

pub use chunk_writer::ChunkWriter;
pub use page_writer::{PageWriter, SealedPage};
pub use time_chunk_writer::TimeChunkWriter;
pub use time_page_writer::{SealedTimePage, TimePageWriter};
pub use tsfile_writer::TsFileWriter;
pub use table_writer::TsFileTableWriter;
pub use value_chunk_writer::ValueChunkWriter;
pub use value_page_writer::{SealedValuePage, ValuePageWriter};
