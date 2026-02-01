// C++ BitMap (common/container/bit_map.h) tracks null values per column
// using a byte array where each bit represents one row. The Rust version
// keeps the same bit layout for serialization compatibility but wraps it
// in a safe API that prevents out-of-bounds access.
//
// Bit layout matches C++: byte_index = bit_index / 8, within each byte
// the MSB (bit 7) corresponds to the lowest bit index. A set bit (1)
// means the value is marked (null), cleared (0) means present.

/// Compact bit vector for tracking null values in columnar data.
///
/// Each bit represents one row. Set (1) = null/marked, clear (0) = present.
/// The byte layout matches C++ `BitMap` for serialization compatibility (LSB-first).
#[derive(Debug, Clone)]
pub struct BitMap {
    /// Number of logical bits this bitmap tracks.
    size: usize,
    /// Backing storage — ceil(size / 8) bytes, LSB-first within each byte.
    data: Vec<u8>,
}

impl BitMap {
    /// Create a new bitmap with all bits cleared (no nulls).
    pub fn new(size: usize) -> Self {
        let byte_count = (size + 7) / 8;
        Self {
            size,
            data: vec![0u8; byte_count],
        }
    }

    /// Returns the number of logical bits.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the bitmap tracks zero bits.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns true if the bit at `index` is set (marked/null).
    ///
    /// # Panics
    /// Panics if `index >= self.len()`.
    pub fn get(&self, index: usize) -> bool {
        assert!(
            index < self.size,
            "bitmap index {index} out of bounds (size {})",
            self.size
        );
        let byte_index = index / 8; // index >> 3 (which byte or index of the byte array)
        let bit_offset = index % 8; // index & 7
        (self.data[byte_index] & (1 << bit_offset)) != 0
    }

    /// Set the bit at `index` (mark as null).
    ///
    /// # Panics
    /// Panics if `index >= self.len()`.
    pub fn set(&mut self, index: usize) {
        assert!(
            index < self.size,
            "bitmap index {index} out of bounds (size {})",
            self.size
        );
        let byte_index = index / 8;
        let bit_offset = index % 8;
        self.data[byte_index] |= 1 << bit_offset;
    }

    /// Clear the bit at `index` (mark as present/not-null).
    ///
    /// # Panics
    /// Panics if `index >= self.len()`.
    pub fn clear(&mut self, index: usize) {
        assert!(
            index < self.size,
            "bitmap index {index} out of bounds (size {})",
            self.size
        );
        let byte_index = index / 8;
        let bit_offset = index % 8;
        self.data[byte_index] &= !(1 << bit_offset);
    }

    /// Returns true if all bits are cleared (no nulls).
    pub fn is_all_unmarked(&self) -> bool {
        self.data.iter().all(|&byte| byte == 0)
    }

    /// Returns the number of set bits (null count).
    pub fn count_marked(&self) -> usize {
        self.data.iter().map(|b| b.count_ones() as usize).sum()
    }

    /// Read-only access to the underlying byte slice (for serialization).
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Create a bitmap from raw bytes (for deserialization).
    pub fn from_bytes(size: usize, data: Vec<u8>) -> Self {
        debug_assert_eq!(data.len(), (size + 7) / 8);
        Self { size, data }
    }

    /// Reset all bits to cleared.
    pub fn clear_all(&mut self) {
        self.data.fill(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_bitmap_is_all_clear() {
        let bm = BitMap::new(64);
        assert_eq!(bm.len(), 64);
        assert!(bm.is_all_unmarked());
        for i in 0..64 {
            assert!(!bm.get(i));
        }
    }

    #[test]
    fn new_empty_bitmap() {
        let bm = BitMap::new(0);
        assert!(bm.is_empty());
        assert!(bm.is_all_unmarked());
        assert_eq!(bm.as_bytes().len(), 0);
    }

    #[test]
    fn set_and_get() {
        let mut bm = BitMap::new(16);
        bm.set(0);
        bm.set(7);
        bm.set(8);
        bm.set(15);

        assert!(bm.get(0));
        assert!(!bm.get(1));
        assert!(bm.get(7));
        assert!(bm.get(8));
        assert!(!bm.get(9));
        assert!(bm.get(15));
    }

    #[test]
    fn set_then_clear() {
        let mut bm = BitMap::new(8);
        bm.set(3);
        assert!(bm.get(3));
        bm.clear(3);
        assert!(!bm.get(3));
        assert!(bm.is_all_unmarked());
    }

    #[test]
    fn count_marked() {
        let mut bm = BitMap::new(16);
        assert_eq!(bm.count_marked(), 0);
        bm.set(0);
        bm.set(5);
        bm.set(10);
        assert_eq!(bm.count_marked(), 3);
    }

    #[test]
    fn non_byte_aligned_size() {
        // 10 bits requires 2 bytes
        let mut bm = BitMap::new(10);
        assert_eq!(bm.as_bytes().len(), 2);
        bm.set(9); // last valid bit
        assert!(bm.get(9));
    }

    #[test]
    fn single_bit_bitmap() {
        let mut bm = BitMap::new(1);
        assert_eq!(bm.as_bytes().len(), 1);
        assert!(!bm.get(0));
        bm.set(0);
        assert!(bm.get(0));
        assert_eq!(bm.count_marked(), 1);
    }

    #[test]
    fn clear_all_resets_everything() {
        let mut bm = BitMap::new(32);
        for i in 0..32 {
            bm.set(i);
        }
        assert_eq!(bm.count_marked(), 32);
        bm.clear_all();
        assert!(bm.is_all_unmarked());
    }

    #[test]
    fn from_bytes_round_trip() {
        let mut bm = BitMap::new(16);
        bm.set(0);
        bm.set(15);
        let bytes = bm.as_bytes().to_vec();
        let bm2 = BitMap::from_bytes(16, bytes);
        assert!(bm2.get(0));
        assert!(!bm2.get(1));
        assert!(bm2.get(15));
    }

    #[test]
    fn lsb_first_byte_layout() {
        // Verify the LSB-first layout matches C++ convention:
        // Bit 0 is the LSB (0x01) of byte 0.
        let mut bm = BitMap::new(8);
        bm.set(0);
        assert_eq!(bm.as_bytes()[0], 0b0000_0001);

        bm.clear_all();
        bm.set(7);
        assert_eq!(bm.as_bytes()[0], 0b1000_0000);

        bm.clear_all();
        bm.set(3);
        assert_eq!(bm.as_bytes()[0], 0b0000_1000);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn get_out_of_bounds_panics() {
        let bm = BitMap::new(8);
        bm.get(8);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn set_out_of_bounds_panics() {
        let mut bm = BitMap::new(8);
        bm.set(8);
    }
}
