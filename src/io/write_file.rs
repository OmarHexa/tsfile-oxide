// WriteFile wraps BufWriter<File> for buffered sequential writes.
// The C++ WriteFile uses a 512-byte ByteStream buffer and manually tracks
// the write position. Here BufWriter handles buffering and a running
// `position` counter replaces manual tracking — no seek-based position
// queries needed during writing (we always write forward).

use crate::error::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Buffered sequential writer for TsFile output.
///
/// Wraps `BufWriter<File>` and maintains a `position` counter that is
/// incremented on every write. This avoids `seek()`-based position queries
/// (which require flushing the buffer first) and is correct because TsFile
/// writes are always strictly forward.
pub struct WriteFile {
    inner: BufWriter<File>,
    position: u64,
}

impl WriteFile {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            inner: BufWriter::new(file),
            position: 0,
        })
    }

    /// Current logical write position (bytes written so far).
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Flush buffered bytes to the OS.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}

impl Write for WriteFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.position += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn write_and_position() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tsfile");
        let mut wf = WriteFile::create(&path).unwrap();

        assert_eq!(wf.position(), 0);
        wf.write_all(b"hello").unwrap();
        assert_eq!(wf.position(), 5);
        wf.write_all(b" world").unwrap();
        assert_eq!(wf.position(), 11);

        wf.flush().unwrap();
        let contents = fs::read(&path).unwrap();
        assert_eq!(contents, b"hello world");
    }

    #[test]
    fn position_counts_partial_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test2.tsfile");
        let mut wf = WriteFile::create(&path).unwrap();
        wf.write_all(&[0u8; 100]).unwrap();
        assert_eq!(wf.position(), 100);
    }
}
