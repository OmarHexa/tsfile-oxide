// C++ file/ module handles low-level I/O (ReadFile, WriteFile) and
// high-level format management (TsFileIOReader, TsFileIOWriter). The C++
// writer uses a 512-byte ByteStream buffer and manual position tracking.
// In Rust, BufWriter<File> provides buffering and std::io::Seek handles
// position tracking, replacing custom buffer management.
