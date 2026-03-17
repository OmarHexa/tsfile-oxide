// C++ file/ module handles low-level I/O (ReadFile, WriteFile) and
// high-level format management (TsFileIOReader, TsFileIOWriter). The C++
// writer uses a 512-byte ByteStream buffer and manual position tracking.
// In Rust, BufWriter<File> provides buffering and a running position counter
// replaces custom buffer management. The reader uses File + Seek directly.

pub mod bloom_filter;
pub mod io_reader;
pub mod io_writer;
pub mod read_file;
pub mod write_file;

pub use bloom_filter::BloomFilter;
pub use io_reader::TsFileIOReader;
pub use io_writer::TsFileIOWriter;
pub use read_file::ReadFile;
pub use write_file::WriteFile;
