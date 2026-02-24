// Bit-packing utilities for integer compression.
//
// ALGORITHM EXPLANATION:
// Bit-packing compresses integers by storing them using only the minimum number
// of bits required for the largest value in a block. For example:
//
//   Values: [5, 12, 7, 3, 9]  (all fit in 4 bits, max = 12 = 0b1100)
//   Instead of: 5×32 bits = 160 bits
//   Store as:   5×4 bits = 20 bits (8x compression!)
//
// TYPICAL USAGE:
//   1. Buffer a block of values (e.g., 128 values)
//   2. Compute maximum value in block → determine bits needed
//   3. Write: [bits_per_value: u8] [packed_bits...]
//   4. Read: extract bits_per_value, unpack N values
//
// WHEN TO USE:
// - Delta-encoded data where deltas are small (TS2DIFF, Sprintz)
// - Sorted integers with small ranges
// - NOT effective for: random data, large ranges, negative numbers (use zigzag first)
//
// C++ COMPARISON:
// The C++ BitPacker uses manual bit manipulation with uint8_t* pointers.
// In Rust we use safe abstractions with Vec<u8> and careful bounds checking,
// achieving the same performance without unsafe code.

use crate::error::{Result, TsFileError};

/// Pack i32 values into minimal bits.
#[derive(Debug, Clone)]
pub struct Int32Packer {
    /// Buffered values to pack
    values: Vec<u32>, // Store as unsigned for bit operations
}

impl Int32Packer {
    /// Create a new packer.
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Add a value to the buffer (converts i32 to u32 for bit operations).
    pub fn add(&mut self, value: i32) {
        self.values.push(value as u32);
    }

    /// Pack all buffered values into output.
    ///
    /// Format: [bits_per_value: u8] [count: varint] [packed_bits...]
    ///
    /// Special case: if all values are 0, writes bits_per_value=0 with no data.
    pub fn pack(&self, out: &mut Vec<u8>) -> Result<()> {
        if self.values.is_empty() {
            // Empty pack: 0 bits, 0 count
            out.push(0);
            out.push(0);
            return Ok(());
        }

        // Determine bits needed (based on max value)
        let max_value = *self.values.iter().max().unwrap();
        let bits_per_value = if max_value == 0 {
            0
        } else {
            32 - max_value.leading_zeros()
        };

        if bits_per_value > 32 {
            return Err(TsFileError::InvalidArg(format!(
                "bits_per_value exceeds 32: {}",
                bits_per_value
            )));
        }

        // Write header: bits_per_value + count
        out.push(bits_per_value as u8);
        crate::serialize::write_var_u32(out, self.values.len() as u32)?;

        // If bits_per_value is 0, all values are 0, no data to write
        if bits_per_value == 0 {
            return Ok(());
        }

        // Pack values bit by bit
        let mut bit_buffer: u64 = 0;
        let mut bits_in_buffer = 0u32;

        for &value in &self.values {
            // Mask value to bits_per_value bits
            let mask = if bits_per_value >= 32 {
                u32::MAX
            } else {
                (1u32 << bits_per_value) - 1
            };
            let masked = value & mask;

            // Add to buffer
            bit_buffer = (bit_buffer << bits_per_value) | (masked as u64);
            bits_in_buffer += bits_per_value;

            // Flush complete bytes
            while bits_in_buffer >= 8 {
                bits_in_buffer -= 8;
                let byte = (bit_buffer >> bits_in_buffer) as u8;
                out.push(byte);
                // Clear written bits
                if bits_in_buffer > 0 {
                    bit_buffer &= (1u64 << bits_in_buffer) - 1;
                }
            }
        }

        // Flush remaining bits (pad to byte boundary)
        if bits_in_buffer > 0 {
            let byte = (bit_buffer << (8 - bits_in_buffer)) as u8;
            out.push(byte);
        }

        Ok(())
    }

    /// Unpack values from input.
    ///
    /// Returns the unpacked values as i32.
    pub fn unpack(input: &[u8]) -> Result<Vec<i32>> {
        if input.len() < 2 {
            return Err(TsFileError::Encoding(
                "bit-packed data too short".to_string(),
            ));
        }

        let bits_per_value = input[0];
        if bits_per_value > 32 {
            return Err(TsFileError::Encoding(format!(
                "invalid bits_per_value: {}",
                bits_per_value
            )));
        }

        // Read count
        let mut cursor = std::io::Cursor::new(&input[1..]);
        let count = crate::serialize::read_var_u32(&mut cursor)? as usize;

        if count == 0 || bits_per_value == 0 {
            return Ok(vec![0; count]);
        }

        // Unpack values
        let data_start = 1 + cursor.position() as usize;
        let data = &input[data_start..];

        let mut values = Vec::with_capacity(count);
        let mut bit_buffer: u64 = 0;
        let mut bits_in_buffer = 0u32;
        let mut data_idx = 0;

        for _ in 0..count {
            // Refill buffer if needed
            while bits_in_buffer < bits_per_value as u32 && data_idx < data.len() {
                bit_buffer = (bit_buffer << 8) | (data[data_idx] as u64);
                bits_in_buffer += 8;
                data_idx += 1;
            }

            if bits_in_buffer < bits_per_value as u32 {
                return Err(TsFileError::Encoding(
                    "not enough bits to unpack value".to_string(),
                ));
            }

            // Extract value
            bits_in_buffer -= bits_per_value as u32;
            let mask = if bits_per_value >= 32 {
                u32::MAX as u64
            } else {
                (1u64 << bits_per_value) - 1
            };
            let value = ((bit_buffer >> bits_in_buffer) & mask) as u32;

            // Clear extracted bits
            if bits_in_buffer > 0 {
                bit_buffer &= (1u64 << bits_in_buffer) - 1;
            }

            values.push(value as i32);
        }

        Ok(values)
    }

    /// Reset the packer for reuse.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Get current number of buffered values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if packer is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Default for Int32Packer {
    fn default() -> Self {
        Self::new()
    }
}

/// Pack i64 values into minimal bits (same logic as Int32Packer but for u64).
#[derive(Debug, Clone)]
pub struct Int64Packer {
    values: Vec<u64>,
}

impl Int64Packer {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn add(&mut self, value: i64) {
        self.values.push(value as u64);
    }

    pub fn pack(&self, out: &mut Vec<u8>) -> Result<()> {
        if self.values.is_empty() {
            out.push(0);
            out.push(0);
            return Ok(());
        }

        let max_value = *self.values.iter().max().unwrap();
        let bits_per_value = if max_value == 0 {
            0
        } else {
            64 - max_value.leading_zeros()
        };

        if bits_per_value > 64 {
            return Err(TsFileError::InvalidArg(format!(
                "bits_per_value exceeds 64: {}",
                bits_per_value
            )));
        }

        out.push(bits_per_value as u8);
        crate::serialize::write_var_u32(out, self.values.len() as u32)?;

        if bits_per_value == 0 {
            return Ok(());
        }

        // For 64-bit values, use u128 buffer to avoid overflow
        let mut bit_buffer: u128 = 0;
        let mut bits_in_buffer = 0u32;

        for &value in &self.values {
            let mask = if bits_per_value >= 64 {
                u64::MAX
            } else {
                (1u64 << bits_per_value) - 1
            };
            let masked = value & mask;

            bit_buffer = (bit_buffer << bits_per_value) | (masked as u128);
            bits_in_buffer += bits_per_value;

            while bits_in_buffer >= 8 {
                bits_in_buffer -= 8;
                let byte = (bit_buffer >> bits_in_buffer) as u8;
                out.push(byte);
                if bits_in_buffer > 0 {
                    bit_buffer &= (1u128 << bits_in_buffer) - 1;
                }
            }
        }

        if bits_in_buffer > 0 {
            let byte = (bit_buffer << (8 - bits_in_buffer)) as u8;
            out.push(byte);
        }

        Ok(())
    }

    pub fn unpack(input: &[u8]) -> Result<Vec<i64>> {
        if input.len() < 2 {
            return Err(TsFileError::Encoding(
                "bit-packed data too short".to_string(),
            ));
        }

        let bits_per_value = input[0];
        if bits_per_value > 64 {
            return Err(TsFileError::Encoding(format!(
                "invalid bits_per_value: {}",
                bits_per_value
            )));
        }

        let mut cursor = std::io::Cursor::new(&input[1..]);
        let count = crate::serialize::read_var_u32(&mut cursor)? as usize;

        if count == 0 || bits_per_value == 0 {
            return Ok(vec![0; count]);
        }

        let data_start = 1 + cursor.position() as usize;
        let data = &input[data_start..];

        let mut values = Vec::with_capacity(count);
        let mut bit_buffer: u128 = 0;
        let mut bits_in_buffer = 0u32;
        let mut data_idx = 0;

        for _ in 0..count {
            while bits_in_buffer < bits_per_value as u32 && data_idx < data.len() {
                bit_buffer = (bit_buffer << 8) | (data[data_idx] as u128);
                bits_in_buffer += 8;
                data_idx += 1;
            }

            if bits_in_buffer < bits_per_value as u32 {
                return Err(TsFileError::Encoding(
                    "not enough bits to unpack value".to_string(),
                ));
            }

            bits_in_buffer -= bits_per_value as u32;
            let mask = if bits_per_value >= 64 {
                u64::MAX as u128
            } else {
                (1u128 << bits_per_value) - 1
            };
            let value = ((bit_buffer >> bits_in_buffer) & mask) as u64;

            if bits_in_buffer > 0 {
                bit_buffer &= (1u128 << bits_in_buffer) - 1;
            }

            values.push(value as i64);
        }

        Ok(values)
    }

    pub fn reset(&mut self) {
        self.values.clear();
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Default for Int64Packer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Int32Packer tests ===

    #[test]
    fn i32_pack_small_values() {
        let mut packer = Int32Packer::new();
        for value in [1, 2, 3, 4, 5] {
            packer.add(value);
        }
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        // Should use 3 bits per value (max=5=0b101)
        assert_eq!(encoded[0], 3, "bits_per_value should be 3");

        let decoded = Int32Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn i32_pack_all_zeros() {
        let mut packer = Int32Packer::new();
        for _ in 0..10 {
            packer.add(0);
        }
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        // Should use 0 bits per value
        assert_eq!(encoded[0], 0, "bits_per_value should be 0 for all zeros");

        let decoded = Int32Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, vec![0; 10]);
    }

    #[test]
    fn i32_pack_single_bit() {
        let mut packer = Int32Packer::new();
        for value in [0, 1, 0, 1, 1, 0, 1, 0] {
            packer.add(value);
        }
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        // Should use 1 bit per value (max=1)
        assert_eq!(encoded[0], 1, "bits_per_value should be 1");

        let decoded = Int32Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, vec![0, 1, 0, 1, 1, 0, 1, 0]);
    }

    #[test]
    fn i32_pack_max_value() {
        let mut packer = Int32Packer::new();
        packer.add(i32::MAX);
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        // Should use 31 bits (i32::MAX = 2^31-1)
        assert_eq!(encoded[0], 31, "bits_per_value should be 31 for i32::MAX");

        let decoded = Int32Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, vec![i32::MAX]);
    }

    #[test]
    fn i32_compression_ratio() {
        let mut packer = Int32Packer::new();
        // 100 values in range [0, 15] (4 bits each)
        for i in 0..100 {
            packer.add(i % 16);
        }
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        let plain_size = 100 * 4; // 100 i32s = 400 bytes
        let ratio = plain_size as f64 / encoded.len() as f64;
        println!(
            "i32: plain {} bytes, packed {} bytes, ratio {:.2}x",
            plain_size,
            encoded.len(),
            ratio
        );
        assert!(ratio > 6.0, "expected >6x compression");
    }

    // === Int64Packer tests ===

    #[test]
    fn i64_pack_small_values() {
        let mut packer = Int64Packer::new();
        for value in [1i64, 2, 3, 4, 5] {
            packer.add(value);
        }
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        assert_eq!(encoded[0], 3);

        let decoded = Int64Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, vec![1i64, 2, 3, 4, 5]);
    }

    #[test]
    fn i64_pack_all_zeros() {
        let mut packer = Int64Packer::new();
        for _ in 0..10 {
            packer.add(0);
        }
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        assert_eq!(encoded[0], 0);

        let decoded = Int64Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, vec![0i64; 10]);
    }

    #[test]
    fn i64_pack_max_value() {
        let mut packer = Int64Packer::new();
        packer.add(i64::MAX);
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        // Should use 63 bits (i64::MAX = 2^63-1)
        assert_eq!(encoded[0], 63);

        let decoded = Int64Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, vec![i64::MAX]);
    }

    #[test]
    fn i64_compression_ratio() {
        let mut packer = Int64Packer::new();
        // 100 values in range [0, 255] (8 bits each)
        for i in 0..100 {
            packer.add(i % 256);
        }
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        let plain_size = 100 * 8; // 100 i64s = 800 bytes
        let ratio = plain_size as f64 / encoded.len() as f64;
        println!(
            "i64: plain {} bytes, packed {} bytes, ratio {:.2}x",
            plain_size,
            encoded.len(),
            ratio
        );
        assert!(ratio > 6.0, "expected >6x compression");
    }

    #[test]
    fn empty_packer() {
        let packer = Int32Packer::new();
        let mut encoded = Vec::new();
        packer.pack(&mut encoded).unwrap();

        let decoded = Int32Packer::unpack(&encoded).unwrap();
        assert_eq!(decoded, Vec::<i32>::new());
    }

    #[test]
    fn reset_and_reuse() {
        let mut packer = Int32Packer::new();
        packer.add(1);
        packer.add(2);
        let mut encoded1 = Vec::new();
        packer.pack(&mut encoded1).unwrap();

        packer.reset();
        assert!(packer.is_empty());

        packer.add(3);
        packer.add(4);
        let mut encoded2 = Vec::new();
        packer.pack(&mut encoded2).unwrap();

        assert_eq!(Int32Packer::unpack(&encoded1).unwrap(), vec![1, 2]);
        assert_eq!(Int32Packer::unpack(&encoded2).unwrap(), vec![3, 4]);
    }
}
