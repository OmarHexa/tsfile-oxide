// ReadFile wraps std::fs::File and provides convenience methods used by
// TsFileIOReader. The C++ ReadFile class adds an internal buffer and manual
// position tracking; here std::fs::File + Seek covers both needs.

use crate::error::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Thin wrapper around `File` for reading TsFile data.
///
/// C++ ReadFile manages an internal read buffer and exposes `read()` /
/// `seek()` / `get_pos()`. In Rust, `std::fs::File` already supports
/// all of these via `Read + Seek`, so this struct is a minimal adaptor
/// that surfaces a TsFile-oriented API and converts IO errors to `TsFileError`.
pub struct ReadFile {
    file: File,
}

impl ReadFile {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Current byte position in the file.
    pub fn position(&mut self) -> Result<u64> {
        Ok(self.file.stream_position()?)
    }

    /// Seek to an absolute byte position.
    pub fn seek_to(&mut self, pos: u64) -> Result<()> {
        self.file.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    /// Total file size in bytes.
    pub fn file_size(&mut self) -> Result<u64> {
        let saved = self.file.stream_position()?;
        let size = self.file.seek(SeekFrom::End(0))?;
        self.file.seek(SeekFrom::Start(saved))?;
        Ok(size)
    }

    /// Read exactly `buf.len()` bytes into `buf`.
    pub fn read_bytes(&mut self, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact(buf)?;
        Ok(())
    }
}

// Implement std traits so ReadFile can be passed directly to serialize::read_*
// and tsfile_format deserialize_from() methods.
impl Read for ReadFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for ReadFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn make_temp_file(data: &[u8]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f
    }

    #[test]
    fn open_and_read() {
        let tmp = make_temp_file(b"hello world");
        let mut rf = ReadFile::open(tmp.path()).unwrap();
        let mut buf = [0u8; 5];
        rf.read_bytes(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn file_size() {
        let tmp = make_temp_file(b"abcdefgh");
        let mut rf = ReadFile::open(tmp.path()).unwrap();
        assert_eq!(rf.file_size().unwrap(), 8);
    }

    #[test]
    fn seek_to() {
        let tmp = make_temp_file(b"hello world");
        let mut rf = ReadFile::open(tmp.path()).unwrap();
        rf.seek_to(6).unwrap();
        let mut buf = [0u8; 5];
        rf.read_bytes(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn position_tracking() {
        let tmp = make_temp_file(b"abcde");
        let mut rf = ReadFile::open(tmp.path()).unwrap();
        assert_eq!(rf.position().unwrap(), 0);
        rf.seek_to(3).unwrap();
        assert_eq!(rf.position().unwrap(), 3);
    }
}
