// C++ reader/ is the largest module (35+ files) with query execution,
// chunk reading, filtering, and result iteration. Two key design choices
// differ from C++:
//
// 1. Filters use dyn Trait (open set) — users can define custom filters,
//    unlike ChunkReader which uses enum (closed set, 2 variants).
// 2. ResultSet implements Iterator — Rust's for-loop syntax replaces
//    the C++ pattern of `while (result_set.next(has_next)) { ... }`.

pub mod block;
pub mod chunk_reader;
pub mod filter;
pub mod metadata_querier;
pub mod result_set;
pub mod row_record;
pub mod scan_iterator;
pub mod tsblock;
pub mod tsfile_reader;

// Re-enabled as each type is introduced by subsequent tasks.
// pub use chunk_reader::ChunkReader;
// pub use filter::Filter;
// pub use metadata_querier::MetadataQuerier;
// pub use result_set::ResultSet;
// pub use row_record::RowRecord;
// pub use scan_iterator::{AlignedSeriesScan, SeriesScanIterator};
pub use tsblock::{Column, ColumnMeta, TsBlock};
// pub use tsfile_reader::TsFileReader;
