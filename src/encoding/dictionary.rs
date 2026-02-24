// Dictionary encoding: Maps repeated values to integer IDs for compression.
//
// ALGORITHM EXPLANATION:
// Dictionary encoding is highly effective for low-cardinality data — data where
// the same values appear many times. Instead of storing each value repeatedly,
// we build a dictionary mapping unique values to small integer IDs:
//
//   Example: ["apple", "banana", "apple", "apple", "banana"]
//   Dictionary: {"apple": 0, "banana": 1}
//   Encoded: [0, 1, 0, 0, 1]  (5 varint IDs instead of 5 strings)
//
// ON-DISK FORMAT:
//   [dict_size: varint] [value_0] [value_1] ... [value_N-1]
//   [count: varint] [id_0: varint] [id_1: varint] ... [id_M-1: varint]
//
// WHEN TO USE:
// - String columns with limited unique values (e.g., status codes, country names)
// - Enum-like data stored as strings
// - Binary data with repeated patterns
// - NOT effective for: high-cardinality data (many unique values), random strings
//
// COMPRESSION RATIO:
// For N values with K unique strings (K << N):
//   Plain: N * avg_string_length bytes
//   Dictionary: K * avg_string_length + N * ~1.5 bytes (varint IDs)
//   Example: 1000 values, 10 unique, avg 20 bytes → 20KB → 0.4KB (50x compression!)
//
// C++ COMPARISON:
// The C++ DictionaryEncoder uses std::unordered_map<string, int> for encoding
// and std::vector<string> for decoding. In Rust we use HashMap and Vec respectively,
// which provide identical semantics without manual memory management.

use crate::error::{Result, TsFileError};
use crate::serialize::{read_var_u32, write_var_u32};
use std::collections::HashMap;
use std::io::Read;

/// Dictionary encoder for String and Bytes values.
///
/// Builds a dictionary of unique values during encoding, then outputs:
/// 1. The dictionary itself (size + all unique values)
/// 2. The encoded data (count + varint IDs referencing dictionary entries)
#[derive(Debug, Clone)]
pub struct DictionaryEncoder {
    /// Maps value -> ID. IDs are assigned in insertion order.
    dictionary: HashMap<Vec<u8>, u32>,
    /// Next available ID (= dictionary.len())
    next_id: u32,
    /// Encoded IDs for all values seen so far
    encoded_ids: Vec<u32>,
}

impl DictionaryEncoder {
    /// Create a new dictionary encoder.
    pub fn new() -> Self {
        Self {
            dictionary: HashMap::new(),
            next_id: 0,
            encoded_ids: Vec::new(),
        }
    }

    /// Encode a string value.
    ///
    /// Adds the value to the dictionary if not present, records its ID.
    pub fn encode_string(&mut self, value: &str) -> Result<()> {
        self.encode_bytes(value.as_bytes())
    }

    /// Encode a bytes value.
    ///
    /// Adds the value to the dictionary if not present, records its ID.
    pub fn encode_bytes(&mut self, value: &[u8]) -> Result<()> {
        let id = match self.dictionary.get(value) {
            Some(&id) => id,
            None => {
                let id = self.next_id;
                self.dictionary.insert(value.to_vec(), id);
                self.next_id += 1;
                id
            }
        };
        self.encoded_ids.push(id);
        Ok(())
    }

    /// Flush the dictionary and encoded IDs to output.
    ///
    /// Format: [dict_size] [value_0] ... [value_N-1] [count] [id_0] ... [id_M-1]
    pub fn flush(&mut self, out: &mut Vec<u8>) -> Result<()> {
        // Write dictionary size
        write_var_u32(out, self.dictionary.len() as u32)?;

        // Write dictionary entries in ID order
        // Build reverse map: ID -> value
        let mut id_to_value: Vec<(&[u8], u32)> = self
            .dictionary
            .iter()
            .map(|(value, &id)| (value.as_slice(), id))
            .collect();
        id_to_value.sort_by_key(|&(_, id)| id);

        for (value, _) in id_to_value {
            // Write value: length (varint) + bytes
            write_var_u32(out, value.len() as u32)?;
            out.extend_from_slice(value);
        }

        // Write encoded IDs count
        write_var_u32(out, self.encoded_ids.len() as u32)?;

        // Write all IDs
        for &id in &self.encoded_ids {
            write_var_u32(out, id)?;
        }

        Ok(())
    }

    /// Reset encoder state for reuse.
    pub fn reset(&mut self) {
        self.dictionary.clear();
        self.next_id = 0;
        self.encoded_ids.clear();
    }

    /// Get compression ratio (encoded size / plain size estimate).
    ///
    /// This is informational only, not used in encoding.
    pub fn compression_ratio(&self) -> f64 {
        if self.encoded_ids.is_empty() {
            return 1.0;
        }

        // Estimate plain size: count + sum of all value lengths
        let plain_size: usize = self
            .dictionary
            .iter()
            .map(|(value, &id)| {
                // Each occurrence: length prefix + bytes
                let count = self.encoded_ids.iter().filter(|&&x| x == id).count();
                count * (5 + value.len()) // Conservative: 5 bytes for length varint
            })
            .sum();

        // Encoded size: dict overhead + IDs
        let dict_size: usize = self.dictionary.iter().map(|(v, _)| 5 + v.len()).sum();
        let ids_size = self.encoded_ids.len() * 2; // Conservative: 2 bytes per ID
        let encoded_size = dict_size + ids_size;

        plain_size as f64 / encoded_size as f64
    }
}

impl Default for DictionaryEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Dictionary decoder for String and Bytes values.
///
/// Reads the dictionary from the input stream, then decodes IDs back to values.
#[derive(Debug, Clone)]
pub struct DictionaryDecoder {
    /// Dictionary: ID -> value. Loaded during first decode.
    dictionary: Vec<Vec<u8>>,
    /// Remaining IDs to decode
    remaining_count: usize,
}

impl DictionaryDecoder {
    /// Create a new dictionary decoder.
    pub fn new() -> Self {
        Self {
            dictionary: Vec::new(),
            remaining_count: 0,
        }
    }

    /// Decode a string value.
    ///
    /// On first call, reads the dictionary. Then decodes IDs to strings.
    pub fn decode_string(&mut self, input: &mut impl Read) -> Result<String> {
        let bytes = self.decode_bytes(input)?;
        String::from_utf8(bytes).map_err(|e| {
            TsFileError::InvalidArg(format!("invalid UTF-8 in dictionary string: {}", e))
        })
    }

    /// Decode a bytes value.
    ///
    /// On first call, reads the dictionary. Then decodes IDs to bytes.
    pub fn decode_bytes(&mut self, input: &mut impl Read) -> Result<Vec<u8>> {
        // Load dictionary on first decode
        if self.dictionary.is_empty() && self.remaining_count == 0 {
            self.load_dictionary(input)?;
        }

        if self.remaining_count == 0 {
            return Err(TsFileError::Encoding(
                "no more values to decode".to_string(),
            ));
        }

        // Read ID and look up value
        let id = read_var_u32(input)? as usize;
        if id >= self.dictionary.len() {
            return Err(TsFileError::Encoding(format!(
                "invalid dictionary ID: {} (dictionary size: {})",
                id,
                self.dictionary.len()
            )));
        }

        self.remaining_count -= 1;
        Ok(self.dictionary[id].clone())
    }

    /// Load the dictionary from the input stream.
    ///
    /// Format: [dict_size] [value_0] ... [value_N-1] [count]
    fn load_dictionary(&mut self, input: &mut impl Read) -> Result<()> {
        // Read dictionary size
        let dict_size = read_var_u32(input)? as usize;
        self.dictionary.clear();
        self.dictionary.reserve(dict_size);

        // Read all dictionary entries
        for _ in 0..dict_size {
            let len = read_var_u32(input)? as usize;
            let mut value = vec![0u8; len];
            input.read_exact(&mut value)?;
            self.dictionary.push(value);
        }

        // Read count of encoded values
        self.remaining_count = read_var_u32(input)? as usize;

        Ok(())
    }

    /// Reset decoder state for reuse.
    pub fn reset(&mut self) {
        self.dictionary.clear();
        self.remaining_count = 0;
    }
}

impl Default for DictionaryDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // === Basic round-trip tests ===

    #[test]
    fn string_single_value() {
        let mut encoder = DictionaryEncoder::new();
        let mut decoder = DictionaryDecoder::new();

        encoder.encode_string("hello").unwrap();
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let mut cursor = Cursor::new(encoded);
        assert_eq!(decoder.decode_string(&mut cursor).unwrap(), "hello");
    }

    #[test]
    fn string_repeated_values() {
        let mut encoder = DictionaryEncoder::new();
        let values = vec!["apple", "banana", "apple", "apple", "banana", "cherry"];

        for value in &values {
            encoder.encode_string(value).unwrap();
        }
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        // Dictionary encoding should compress repeated strings
        let plain_size = values.iter().map(|s| 4 + s.len()).sum::<usize>();
        assert!(
            encoded.len() < plain_size,
            "encoded {} bytes, plain {} bytes",
            encoded.len(),
            plain_size
        );

        let mut decoder = DictionaryDecoder::new();
        let mut cursor = Cursor::new(encoded);
        for expected in &values {
            assert_eq!(decoder.decode_string(&mut cursor).unwrap(), *expected);
        }
    }

    #[test]
    fn bytes_round_trip() {
        let mut encoder = DictionaryEncoder::new();
        let values = vec![
            vec![0x01, 0x02],
            vec![0x03, 0x04],
            vec![0x01, 0x02],
            vec![0x05],
            vec![0x03, 0x04],
        ];

        for value in &values {
            encoder.encode_bytes(value).unwrap();
        }
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let mut decoder = DictionaryDecoder::new();
        let mut cursor = Cursor::new(encoded);
        for expected in &values {
            assert_eq!(decoder.decode_bytes(&mut cursor).unwrap(), *expected);
        }
    }

    #[test]
    fn empty_strings() {
        let mut encoder = DictionaryEncoder::new();
        let values = vec!["", "hello", "", "world", ""];

        for value in &values {
            encoder.encode_string(value).unwrap();
        }
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let mut decoder = DictionaryDecoder::new();
        let mut cursor = Cursor::new(encoded);
        for expected in &values {
            assert_eq!(decoder.decode_string(&mut cursor).unwrap(), *expected);
        }
    }

    #[test]
    fn unicode_strings() {
        let mut encoder = DictionaryEncoder::new();
        let values = vec!["こんにちは", "世界", "こんにちは", "🚀", "世界"];

        for value in &values {
            encoder.encode_string(value).unwrap();
        }
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let mut decoder = DictionaryDecoder::new();
        let mut cursor = Cursor::new(encoded);
        for expected in &values {
            assert_eq!(decoder.decode_string(&mut cursor).unwrap(), *expected);
        }
    }

    #[test]
    fn high_cardinality() {
        // Dictionary encoding still works but isn't efficient for unique values
        let mut encoder = DictionaryEncoder::new();
        let values: Vec<String> = (0..100).map(|i| format!("unique_{}", i)).collect();

        for value in &values {
            encoder.encode_string(value).unwrap();
        }
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let mut decoder = DictionaryDecoder::new();
        let mut cursor = Cursor::new(encoded);
        for expected in &values {
            assert_eq!(decoder.decode_string(&mut cursor).unwrap(), *expected);
        }
    }

    // === Error cases ===

    #[test]
    fn decode_too_many() {
        let mut encoder = DictionaryEncoder::new();
        encoder.encode_string("test").unwrap();
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let mut decoder = DictionaryDecoder::new();
        let mut cursor = Cursor::new(encoded);
        decoder.decode_string(&mut cursor).unwrap(); // First decode OK
        assert!(decoder.decode_string(&mut cursor).is_err()); // Second decode fails
    }

    #[test]
    fn invalid_utf8() {
        let mut encoder = DictionaryEncoder::new();
        encoder.encode_bytes(&[0xFF, 0xFE, 0xFD]).unwrap(); // Invalid UTF-8
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let mut decoder = DictionaryDecoder::new();
        let mut cursor = Cursor::new(encoded);
        assert!(decoder.decode_string(&mut cursor).is_err());
    }

    // === Compression efficiency tests ===

    #[test]
    fn compression_ratio_low_cardinality() {
        let mut encoder = DictionaryEncoder::new();
        // 100 values, 5 unique strings
        let unique_values = vec!["status_ok", "status_error", "status_pending", "status_complete", "status_cancelled"];
        for _ in 0..20 {
            for value in &unique_values {
                encoder.encode_string(value).unwrap();
            }
        }
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let plain_size = 100 * (4 + 13); // 100 values * (length prefix + ~13 chars avg)
        let ratio = plain_size as f64 / encoded.len() as f64;
        println!(
            "Low cardinality: plain {} bytes, encoded {} bytes, ratio {:.2}x",
            plain_size,
            encoded.len(),
            ratio
        );
        assert!(ratio > 5.0, "expected >5x compression, got {:.2}x", ratio);
    }

    #[test]
    fn compression_ratio_medium_cardinality() {
        let mut encoder = DictionaryEncoder::new();
        // 100 values, 20 unique strings (each appears 5 times)
        for i in 0..100 {
            let value = format!("value_{}", i % 20);
            encoder.encode_string(&value).unwrap();
        }
        let mut encoded = Vec::new();
        encoder.flush(&mut encoded).unwrap();

        let plain_size = 100 * (4 + 8); // 100 * (length + "value_XX")
        let ratio = plain_size as f64 / encoded.len() as f64;
        println!(
            "Medium cardinality: plain {} bytes, encoded {} bytes, ratio {:.2}x",
            plain_size,
            encoded.len(),
            ratio
        );
        assert!(ratio > 2.0, "expected >2x compression, got {:.2}x", ratio);
    }

    #[test]
    fn reset_and_reuse() {
        let mut encoder = DictionaryEncoder::new();
        let mut decoder = DictionaryDecoder::new();

        // First encoding
        encoder.encode_string("first").unwrap();
        let mut encoded1 = Vec::new();
        encoder.flush(&mut encoded1).unwrap();

        // Reset and encode again
        encoder.reset();
        encoder.encode_string("second").unwrap();
        let mut encoded2 = Vec::new();
        encoder.flush(&mut encoded2).unwrap();

        // Decode both
        let mut cursor1 = Cursor::new(encoded1);
        assert_eq!(decoder.decode_string(&mut cursor1).unwrap(), "first");

        decoder.reset();
        let mut cursor2 = Cursor::new(encoded2);
        assert_eq!(decoder.decode_string(&mut cursor2).unwrap(), "second");
    }
}
