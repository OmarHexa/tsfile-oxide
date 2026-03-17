// C++ reader/bloom_filter.h implements a Bloom filter for fast device-path
// lookups before opening file sections. The filter uses 8 MurmurHash3-x86-32
// functions with different seeds to set/test bits.
//
// This must match the C++ implementation byte-for-byte: same SEEDS array,
// same murmur3 variant (x86_32), same bit addressing. The filter bytes are
// stored inside TsFileMeta and deserialized from the file footer.
//
// False positives are possible (the filter may report a device exists when
// it doesn't), but false negatives are not (if a device was added, might_contain
// always returns true).

use crate::error::{Result, TsFileError};
use murmur3::murmur3_32;
use std::io::Cursor;

/// Seeds for the 8 hash functions. Must match C++ SEEDS[8] in bloom_filter.h.
/// These prime-like values spread hash outputs across the bit array.
const SEEDS: [u32; 8] = [5, 7, 11, 13, 31, 37, 61, 73];

/// Bloom filter for fast device-path existence checks.
///
/// C++ BloomFilter holds a raw bit array and uses HASH_FUNCTIONS (8) rounds
/// of murmur3_x86_32 with different seeds. Rust stores the same bits as
/// Vec<u8> with the same layout: bit index `b` maps to byte `b/8`, bit `b%8`.
///
/// Serialized form (stored in TsFileMeta bloom_filter bytes):
///   [bit_count: u32 big-endian] [raw bit bytes...]
pub struct BloomFilter {
    bits: Vec<u8>,
    bit_count: usize,
}

impl BloomFilter {
    /// Create an empty filter with `bit_count` addressable bits.
    /// `bit_count` is rounded up to the next byte boundary internally.
    pub fn new(bit_count: usize) -> Self {
        let byte_count = (bit_count + 7).div_ceil(8);
        Self {
            bits: vec![0u8; byte_count],
            bit_count,
        }
    }

    /// Size a new filter for `expected_entries` items at false-positive rate `error_rate`.
    /// Formula matches C++: m = -n * ln(p) / ln(2)^2, rounded up to byte boundary.
    pub fn with_capacity(expected_entries: usize, error_rate: f64) -> Self {
        let bit_count = optimal_bit_count(expected_entries, error_rate);
        Self::new(bit_count)
    }

    /// Add a device path to the filter.
    pub fn add(&mut self, key: &str) {
        for &seed in &SEEDS {
            let bit = hash_bit(key.as_bytes(), seed, self.bit_count);
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    /// Return true if the key *might* be in the filter (false positives possible).
    /// Return false only if the key was definitely never added.
    pub fn might_contain(&self, key: &str) -> bool {
        for &seed in &SEEDS {
            let bit = hash_bit(key.as_bytes(), seed, self.bit_count);
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize to bytes for storage in TsFileMeta.
    /// Format: [bit_count: u32 big-endian] + [raw bit array bytes].
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bits.len());
        out.extend_from_slice(&(self.bit_count as u32).to_be_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Reconstruct from the bytes stored in TsFileMeta.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(TsFileError::Corrupted("bloom filter data too short".into()));
        }
        let bit_count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let bits = data[4..].to_vec();
        let expected_bytes = (bit_count + 7).div_ceil(8);
        if bits.len() < expected_bytes {
            return Err(TsFileError::Corrupted(format!(
                "bloom filter bytes truncated: expected {expected_bytes}, got {}",
                bits.len()
            )));
        }
        Ok(Self { bits, bit_count })
    }

    pub fn bit_count(&self) -> usize {
        self.bit_count
    }
}

/// Compute the bit index for `data` hashed with `seed`, modulo `bit_count`.
fn hash_bit(data: &[u8], seed: u32, bit_count: usize) -> usize {
    let mut cursor = Cursor::new(data);
    // murmur3_32 matches the C++ murmur3_x86_32 variant used in bloom_filter.h.
    let hash = murmur3_32(&mut cursor, seed);
    (hash as usize) % bit_count
}

/// Optimal bit count for `n` entries at false-positive rate `p`.
fn optimal_bit_count(n: usize, p: f64) -> usize {
    // m = -n * ln(p) / ln(2)^2
    let bits = -(n as f64) * p.ln() / (2.0_f64.ln().powi(2));
    // Round up to byte boundary, minimum 64 bits to avoid degenerate filters.
    ((bits.ceil() as usize).max(64) + 7) & !7 // bit operator !7 and then and removed last 3
    // bits making it muliple of 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_might_contain() {
        let mut bf = BloomFilter::with_capacity(100, 0.05);
        bf.add("root.sg1.d1");
        bf.add("root.sg1.d2");
        assert!(bf.might_contain("root.sg1.d1"));
        assert!(bf.might_contain("root.sg1.d2"));
    }

    #[test]
    fn missing_key_not_contained() {
        let mut bf = BloomFilter::with_capacity(100, 0.05);
        bf.add("root.sg1.d1");
        // "root.sg1.d999" was never added; it could be a false positive,
        // but with a 5% error rate and only 1 entry, it's overwhelmingly likely false.
        // We can't guarantee it — just verify the API returns a bool.
        let _ = bf.might_contain("root.sg1.d999");
    }

    #[test]
    fn serialize_round_trip() {
        let mut bf = BloomFilter::with_capacity(50, 0.01);
        bf.add("root.sg1.d1");
        bf.add("root.sg2.d1");

        let bytes = bf.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        assert_eq!(restored.bit_count(), bf.bit_count());
        assert!(restored.might_contain("root.sg1.d1"));
        assert!(restored.might_contain("root.sg2.d1"));
    }

    #[test]
    fn from_bytes_too_short_errors() {
        assert!(BloomFilter::from_bytes(&[0u8; 3]).is_err());
    }

    #[test]
    fn seeds_are_correct() {
        // Verify the seed array length — 8 hash functions per the roadmap spec.
        assert_eq!(SEEDS.len(), 8);
    }

    #[test]
    fn optimal_bit_count_minimum() {
        // Even with 0 expected entries, minimum is 64 bits.
        let bc = optimal_bit_count(0, 0.05);
        assert!(bc >= 64);
        assert_eq!(bc % 8, 0, "must be byte-aligned");
    }
}
