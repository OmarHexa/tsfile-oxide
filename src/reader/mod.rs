// C++ reader/ is the largest module (35+ files) with query execution,
// chunk reading, filtering, and result iteration. Two key design choices
// differ from C++:
//
// 1. Filters use dyn Trait (open set) — users can define custom filters,
//    unlike ChunkReader which uses enum (closed set, 2 variants).
// 2. ResultSet implements Iterator — Rust's for-loop syntax replaces
//    the C++ pattern of `while (result_set.next(has_next)) { ... }`.

pub mod filter;
pub mod block;
