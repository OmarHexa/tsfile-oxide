// C++ uses ByteStream (allocator/byte_stream.h) and SerializationUtil for
// varint and string encoding. The varint format is NOT standard protobuf —
// it's a custom variable-length encoding that must be matched byte-for-byte
// for binary compatibility.
//
// In Rust we use std::io::Read/Write traits so serialization works with any
// I/O target (files, Vec<u8>, Cursor, etc.). This replaces the C++ approach
// of a single custom ByteStream class, leveraging Rust's trait system to
// decouple serialization logic from buffer management.
//
// Varint format (matches C++ SerializationUtil / Java ReadWriteIOUtils):
//   Unsigned: 7 bits per byte, MSB is continuation bit (1 = more bytes).
//   Signed i32/i64: zigzag-encode first, then write as unsigned varint.
//   Zigzag: (n << 1) ^ (n >> 31) for i32, (n << 1) ^ (n >> 63) for i64.
//
// String format: i32 length prefix (big-endian fixed) + raw UTF-8 bytes.
// This matches the Java TsFile convention used by the C++ implementation.

use std::io::{Read, Write};
use crate::error::{TsFileError, Result};

// ---------------------------------------------------------------------------
// Unsigned varint
// ---------------------------------------------------------------------------

/// Write a u32 as a variable-length integer.
/// Returns the number of bytes written (1-5).
pub fn write_var_u32(writer: &mut impl Write, mut value: u32) -> Result<usize> {
    let mut bytes_written = 0;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        bytes_written += 1;
        if value == 0 {
            break;
        }
    }
    Ok(bytes_written)
}

/// Read a variable-length u32. Consumes 1-5 bytes.
pub fn read_var_u32(reader: &mut impl Read) -> Result<u32> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    loop {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 35 {
            return Err(TsFileError::Corrupted("varint u32 too long".into()));
        }
    }
}

/// Write a u64 as a variable-length integer.
/// Returns the number of bytes written (1-10).
pub fn write_var_u64(writer: &mut impl Write, mut value: u64) -> Result<usize> {
    let mut bytes_written = 0;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        bytes_written += 1;
        if value == 0 {
            break;
        }
    }
    Ok(bytes_written)
}

/// Read a variable-length u64. Consumes 1-10 bytes.
pub fn read_var_u64(reader: &mut impl Read) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 70 {
            return Err(TsFileError::Corrupted("varint u64 too long".into()));
        }
    }
}

// ---------------------------------------------------------------------------
// Signed varint (zigzag encoding)
// ---------------------------------------------------------------------------

/// Zigzag-encode a signed i32 into an unsigned u32.
/// Maps negative numbers to odd positives: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, ...
#[inline]
fn zigzag_encode_i32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

/// Zigzag-decode a u32 back to signed i32.
#[inline]
fn zigzag_decode_i32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

/// Zigzag-encode a signed i64 into an unsigned u64.
#[inline]
fn zigzag_encode_i64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// Zigzag-decode a u64 back to signed i64.
#[inline]
fn zigzag_decode_i64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

/// Write a signed i32 as a zigzag-encoded varint.
pub fn write_var_i32(writer: &mut impl Write, value: i32) -> Result<usize> {
    write_var_u32(writer, zigzag_encode_i32(value))
}

/// Read a zigzag-encoded signed i32.
pub fn read_var_i32(reader: &mut impl Read) -> Result<i32> {
    Ok(zigzag_decode_i32(read_var_u32(reader)?))
}

/// Write a signed i64 as a zigzag-encoded varint.
pub fn write_var_i64(writer: &mut impl Write, value: i64) -> Result<usize> {
    write_var_u64(writer, zigzag_encode_i64(value))
}

/// Read a zigzag-encoded signed i64.
pub fn read_var_i64(reader: &mut impl Read) -> Result<i64> {
    Ok(zigzag_decode_i64(read_var_u64(reader)?))
}

// ---------------------------------------------------------------------------
// Fixed-width primitives (big-endian, matching Java/C++ TsFile convention)
// ---------------------------------------------------------------------------

pub fn write_i32(writer: &mut impl Write, value: i32) -> Result<()> {
    writer.write_all(&value.to_be_bytes())?;
    Ok(())
}

pub fn read_i32(reader: &mut impl Read) -> Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

pub fn write_i64(writer: &mut impl Write, value: i64) -> Result<()> {
    writer.write_all(&value.to_be_bytes())?;
    Ok(())
}

pub fn read_i64(reader: &mut impl Read) -> Result<i64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes(buf))
}

pub fn write_f32(writer: &mut impl Write, value: f32) -> Result<()> {
    writer.write_all(&value.to_be_bytes())?;
    Ok(())
}

pub fn read_f32(reader: &mut impl Read) -> Result<f32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(f32::from_be_bytes(buf))
}

pub fn write_f64(writer: &mut impl Write, value: f64) -> Result<()> {
    writer.write_all(&value.to_be_bytes())?;
    Ok(())
}

pub fn read_f64(reader: &mut impl Read) -> Result<f64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(f64::from_be_bytes(buf))
}

pub fn write_bool(writer: &mut impl Write, value: bool) -> Result<()> {
    writer.write_all(&[value as u8])?;
    Ok(())
}

pub fn read_bool(reader: &mut impl Read) -> Result<bool> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0] != 0)
}

pub fn write_u8(writer: &mut impl Write, value: u8) -> Result<()> {
    writer.write_all(&[value])?;
    Ok(())
}

pub fn read_u8(reader: &mut impl Read) -> Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

// ---------------------------------------------------------------------------
// String serialization: i32 length prefix (big-endian) + raw UTF-8 bytes
// ---------------------------------------------------------------------------

/// Write a string as a length-prefixed byte sequence.
/// Format: 4-byte big-endian i32 length + raw bytes.
pub fn write_string(writer: &mut impl Write, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    write_i32(writer, bytes.len() as i32)?;
    writer.write_all(bytes)?;
    Ok(())
}

/// Read a length-prefixed string.
pub fn read_string(reader: &mut impl Read) -> Result<String> {
    let len = read_i32(reader)?;
    if len < 0 {
        return Err(TsFileError::Corrupted(format!(
            "negative string length: {len}"
        )));
    }
    let len = len as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| TsFileError::Corrupted(format!("invalid UTF-8: {e}")))
}

/// Write raw bytes with a varint length prefix.
/// Used for binary data (TEXT/BLOB columns) where i32 prefix is wasteful.
pub fn write_bytes(writer: &mut impl Write, value: &[u8]) -> Result<()> {
    write_var_u32(writer, value.len() as u32)?;
    writer.write_all(value)?;
    Ok(())
}

/// Read raw bytes with a varint length prefix.
pub fn read_bytes(reader: &mut impl Read) -> Result<Vec<u8>> {
    let len = read_var_u32(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // --- Varint unsigned round-trips ---

    fn var_u32_round_trip(value: u32) -> u32 {
        let mut buf = Vec::new();
        write_var_u32(&mut buf, value).unwrap();
        let mut cursor = Cursor::new(&buf);
        read_var_u32(&mut cursor).unwrap()
    }

    #[test]
    fn var_u32_zero() {
        assert_eq!(var_u32_round_trip(0), 0);
    }

    #[test]
    fn var_u32_single_byte_max() {
        // 127 fits in one byte (0x7F)
        let mut buf = Vec::new();
        let n = write_var_u32(&mut buf, 127).unwrap();
        assert_eq!(n, 1);
        assert_eq!(buf, vec![0x7F]);
        assert_eq!(var_u32_round_trip(127), 127);
    }

    #[test]
    fn var_u32_two_bytes() {
        // 128 requires two bytes: 0x80 0x01
        let mut buf = Vec::new();
        write_var_u32(&mut buf, 128).unwrap();
        assert_eq!(buf, vec![0x80, 0x01]);
        assert_eq!(var_u32_round_trip(128), 128);
    }

    #[test]
    fn var_u32_300() {
        // 300 = 0b100101100 -> 0xAC 0x02
        let mut buf = Vec::new();
        write_var_u32(&mut buf, 300).unwrap();
        assert_eq!(buf, vec![0xAC, 0x02]);
        assert_eq!(var_u32_round_trip(300), 300);
    }

    #[test]
    fn var_u32_max() {
        assert_eq!(var_u32_round_trip(u32::MAX), u32::MAX);
        let mut buf = Vec::new();
        let n = write_var_u32(&mut buf, u32::MAX).unwrap();
        assert_eq!(n, 5);
    }

    fn var_u64_round_trip(value: u64) -> u64 {
        let mut buf = Vec::new();
        write_var_u64(&mut buf, value).unwrap();
        let mut cursor = Cursor::new(&buf);
        read_var_u64(&mut cursor).unwrap()
    }

    #[test]
    fn var_u64_basic() {
        assert_eq!(var_u64_round_trip(0), 0);
        assert_eq!(var_u64_round_trip(1), 1);
        assert_eq!(var_u64_round_trip(127), 127);
        assert_eq!(var_u64_round_trip(128), 128);
        assert_eq!(var_u64_round_trip(u64::MAX), u64::MAX);
    }

    #[test]
    fn var_u64_max_uses_10_bytes() {
        let mut buf = Vec::new();
        let n = write_var_u64(&mut buf, u64::MAX).unwrap();
        assert_eq!(n, 10);
    }

    // --- Varint signed round-trips (zigzag) ---

    fn var_i32_round_trip(value: i32) -> i32 {
        let mut buf = Vec::new();
        write_var_i32(&mut buf, value).unwrap();
        let mut cursor = Cursor::new(&buf);
        read_var_i32(&mut cursor).unwrap()
    }

    #[test]
    fn var_i32_zigzag_encoding() {
        // Zigzag maps: 0->0, -1->1, 1->2, -2->3, 2->4
        assert_eq!(zigzag_encode_i32(0), 0);
        assert_eq!(zigzag_encode_i32(-1), 1);
        assert_eq!(zigzag_encode_i32(1), 2);
        assert_eq!(zigzag_encode_i32(-2), 3);
        assert_eq!(zigzag_encode_i32(2), 4);
    }

    #[test]
    fn var_i32_round_trips() {
        assert_eq!(var_i32_round_trip(0), 0);
        assert_eq!(var_i32_round_trip(1), 1);
        assert_eq!(var_i32_round_trip(-1), -1);
        assert_eq!(var_i32_round_trip(i32::MIN), i32::MIN);
        assert_eq!(var_i32_round_trip(i32::MAX), i32::MAX);
    }

    #[test]
    fn var_i32_small_negatives_are_compact() {
        // -1 zigzags to 1, which is a single byte
        let mut buf = Vec::new();
        write_var_i32(&mut buf, -1).unwrap();
        assert_eq!(buf.len(), 1);
    }

    fn var_i64_round_trip(value: i64) -> i64 {
        let mut buf = Vec::new();
        write_var_i64(&mut buf, value).unwrap();
        let mut cursor = Cursor::new(&buf);
        read_var_i64(&mut cursor).unwrap()
    }

    #[test]
    fn var_i64_round_trips() {
        assert_eq!(var_i64_round_trip(0), 0);
        assert_eq!(var_i64_round_trip(1), 1);
        assert_eq!(var_i64_round_trip(-1), -1);
        assert_eq!(var_i64_round_trip(i64::MIN), i64::MIN);
        assert_eq!(var_i64_round_trip(i64::MAX), i64::MAX);
    }

    // --- Fixed-width primitives ---

    #[test]
    fn i32_round_trip() {
        for value in [0, 1, -1, i32::MIN, i32::MAX, 42, -42] {
            let mut buf = Vec::new();
            write_i32(&mut buf, value).unwrap();
            assert_eq!(buf.len(), 4);
            let mut cursor = Cursor::new(&buf);
            assert_eq!(read_i32(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn i32_big_endian_bytes() {
        let mut buf = Vec::new();
        write_i32(&mut buf, 0x01020304).unwrap();
        assert_eq!(buf, vec![0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn i64_round_trip() {
        for value in [0, 1, -1, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            write_i64(&mut buf, value).unwrap();
            assert_eq!(buf.len(), 8);
            let mut cursor = Cursor::new(&buf);
            assert_eq!(read_i64(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn f32_round_trip() {
        for value in [0.0f32, 1.0, -1.0, f32::MIN, f32::MAX, f32::INFINITY, f32::NEG_INFINITY] {
            let mut buf = Vec::new();
            write_f32(&mut buf, value).unwrap();
            let mut cursor = Cursor::new(&buf);
            assert_eq!(read_f32(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn f32_nan_round_trip() {
        let mut buf = Vec::new();
        write_f32(&mut buf, f32::NAN).unwrap();
        let mut cursor = Cursor::new(&buf);
        assert!(read_f32(&mut cursor).unwrap().is_nan());
    }

    #[test]
    fn f64_round_trip() {
        for value in [0.0f64, 1.0, -1.0, f64::MIN, f64::MAX, f64::INFINITY] {
            let mut buf = Vec::new();
            write_f64(&mut buf, value).unwrap();
            let mut cursor = Cursor::new(&buf);
            assert_eq!(read_f64(&mut cursor).unwrap(), value);
        }
    }

    #[test]
    fn bool_round_trip() {
        for value in [true, false] {
            let mut buf = Vec::new();
            write_bool(&mut buf, value).unwrap();
            assert_eq!(buf.len(), 1);
            let mut cursor = Cursor::new(&buf);
            assert_eq!(read_bool(&mut cursor).unwrap(), value);
        }
    }

    // --- String serialization ---

    #[test]
    fn string_round_trip() {
        let test_cases = ["", "hello", "root.sg1.d1.temperature"];
        for s in test_cases {
            let mut buf = Vec::new();
            write_string(&mut buf, s).unwrap();
            let mut cursor = Cursor::new(&buf);
            assert_eq!(read_string(&mut cursor).unwrap(), s);
        }
    }

    #[test]
    fn string_format_is_i32_len_plus_bytes() {
        let mut buf = Vec::new();
        write_string(&mut buf, "abc").unwrap();
        // 4 bytes for i32 length (3) + 3 bytes for "abc"
        assert_eq!(buf.len(), 7);
        assert_eq!(&buf[0..4], &[0, 0, 0, 3]); // big-endian 3
        assert_eq!(&buf[4..7], b"abc");
    }

    #[test]
    fn string_empty() {
        let mut buf = Vec::new();
        write_string(&mut buf, "").unwrap();
        // 4 bytes for length 0, no content
        assert_eq!(buf.len(), 4);
        assert_eq!(&buf, &[0, 0, 0, 0]);
    }

    // --- Bytes serialization ---

    #[test]
    fn bytes_round_trip() {
        let data = b"binary\x00data\xFF";
        let mut buf = Vec::new();
        write_bytes(&mut buf, data).unwrap();
        let mut cursor = Cursor::new(&buf);
        assert_eq!(read_bytes(&mut cursor).unwrap(), data);
    }

    #[test]
    fn bytes_empty() {
        let mut buf = Vec::new();
        write_bytes(&mut buf, b"").unwrap();
        let mut cursor = Cursor::new(&buf);
        assert_eq!(read_bytes(&mut cursor).unwrap(), b"");
    }

    // --- Error cases ---

    #[test]
    fn read_var_u32_from_empty_stream() {
        let mut cursor = Cursor::new(&[] as &[u8]);
        assert!(read_var_u32(&mut cursor).is_err());
    }

    #[test]
    fn read_string_negative_length() {
        let mut buf = Vec::new();
        write_i32(&mut buf, -1).unwrap();
        let mut cursor = Cursor::new(&buf);
        let err = read_string(&mut cursor).unwrap_err();
        assert!(err.to_string().contains("negative string length"));
    }

    // --- Multiple values in sequence ---

    #[test]
    fn sequential_writes_and_reads() {
        let mut buf = Vec::new();
        write_var_u32(&mut buf, 42).unwrap();
        write_i64(&mut buf, -1000).unwrap();
        write_string(&mut buf, "test").unwrap();
        write_bool(&mut buf, true).unwrap();

        let mut cursor = Cursor::new(&buf);
        assert_eq!(read_var_u32(&mut cursor).unwrap(), 42);
        assert_eq!(read_i64(&mut cursor).unwrap(), -1000);
        assert_eq!(read_string(&mut cursor).unwrap(), "test");
        assert_eq!(read_bool(&mut cursor).unwrap(), true);
    }
}
