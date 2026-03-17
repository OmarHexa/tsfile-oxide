// C++ models statistics as a Statistic* base class with 8 virtual subclasses
// (BooleanStatistic, Int32Statistic, etc.) allocated on the heap via
// StatisticFactory. In Rust the set of types is closed and known at compile
// time, so we use an enum. This avoids heap allocation, eliminates vtable
// indirection, and lets the compiler verify exhaustive handling.
//
// Each variant tracks the same fields as its C++ counterpart:
// - count, start_time, end_time (common to all)
// - min, max, sum, first, last (type-specific for numerics)
//
// The serialize/deserialize methods write fields in a fixed order matching
// the C++ on-disk format for binary compatibility.

use crate::error::Result;
use crate::serialize;
use crate::types::TSDataType;
use std::io::{Read, Write};

/// Per-chunk or per-page statistics, parameterized by data type.
///
/// Replaces the C++ virtual Statistic* hierarchy (BooleanStatistic,
/// Int32Statistic, etc.) with a single enum. The enum is stack-allocated
/// and pattern-matched exhaustively.
#[derive(Debug, Clone, PartialEq)]
pub enum Statistic {
    Boolean {
        count: u64,
        start_time: i64,
        end_time: i64,
        first: bool,
        last: bool,
        /// Sum of boolean values (count of `true`).
        sum: i64,
    },
    Int32 {
        count: u64,
        start_time: i64,
        end_time: i64,
        min: i32,
        max: i32,
        first: i32,
        last: i32,
        sum: f64,
    },
    Int64 {
        count: u64,
        start_time: i64,
        end_time: i64,
        min: i64,
        max: i64,
        first: i64,
        last: i64,
        sum: f64,
    },
    Float {
        count: u64,
        start_time: i64,
        end_time: i64,
        min: f32,
        max: f32,
        first: f32,
        last: f32,
        sum: f64,
    },
    Double {
        count: u64,
        start_time: i64,
        end_time: i64,
        min: f64,
        max: f64,
        first: f64,
        last: f64,
        sum: f64,
    },
    /// Text statistics only track count and time range plus first/last values.
    /// Min/max/sum are not meaningful for arbitrary byte sequences.
    Text {
        count: u64,
        start_time: i64,
        end_time: i64,
        first: Vec<u8>,
        last: Vec<u8>,
    },
    // TODO: Statistics for other data types (String).
}

impl Statistic {
    /// Create a new empty Statistic for the given data type.
    /// Replaces C++ `StatisticFactory::alloc_statistic(TSDataType)`.
    pub fn new(data_type: TSDataType) -> Self {
        match data_type {
            TSDataType::Boolean => Statistic::Boolean {
                count: 0,
                start_time: i64::MAX,
                end_time: i64::MIN,
                first: false,
                last: false,
                sum: 0,
            },
            TSDataType::Int32 => Statistic::Int32 {
                count: 0,
                start_time: i64::MAX,
                end_time: i64::MIN,
                min: i32::MAX,
                max: i32::MIN,
                first: 0,
                last: 0,
                sum: 0.0,
            },
            TSDataType::Int64 => Statistic::Int64 {
                count: 0,
                start_time: i64::MAX,
                end_time: i64::MIN,
                min: i64::MAX,
                max: i64::MIN,
                first: 0,
                last: 0,
                sum: 0.0,
            },
            TSDataType::Float => Statistic::Float {
                count: 0,
                start_time: i64::MAX,
                end_time: i64::MIN,
                min: f32::MAX,
                max: f32::MIN,
                first: 0.0,
                last: 0.0,
                sum: 0.0,
            },
            TSDataType::Double => Statistic::Double {
                count: 0,
                start_time: i64::MAX,
                end_time: i64::MIN,
                min: f64::MAX,
                max: f64::MIN,
                first: 0.0,
                last: 0.0,
                sum: 0.0,
            },
            TSDataType::Text | TSDataType::String => Statistic::Text {
                count: 0,
                start_time: i64::MAX,
                end_time: i64::MIN,
                first: Vec::new(),
                last: Vec::new(),
            },
        }
    }

    /// Returns the data type this statistic tracks.
    pub fn data_type(&self) -> TSDataType {
        match self {
            Statistic::Boolean { .. } => TSDataType::Boolean,
            Statistic::Int32 { .. } => TSDataType::Int32,
            Statistic::Int64 { .. } => TSDataType::Int64,
            Statistic::Float { .. } => TSDataType::Float,
            Statistic::Double { .. } => TSDataType::Double,
            Statistic::Text { .. } => TSDataType::Text,
        }
    }

    pub fn count(&self) -> u64 {
        match self {
            Statistic::Boolean { count, .. }
            | Statistic::Int32 { count, .. }
            | Statistic::Int64 { count, .. }
            | Statistic::Float { count, .. }
            | Statistic::Double { count, .. }
            | Statistic::Text { count, .. } => *count,
        }
    }

    pub fn start_time(&self) -> i64 {
        match self {
            Statistic::Boolean { start_time, .. }
            | Statistic::Int32 { start_time, .. }
            | Statistic::Int64 { start_time, .. }
            | Statistic::Float { start_time, .. }
            | Statistic::Double { start_time, .. }
            | Statistic::Text { start_time, .. } => *start_time,
        }
    }

    pub fn end_time(&self) -> i64 {
        match self {
            Statistic::Boolean { end_time, .. }
            | Statistic::Int32 { end_time, .. }
            | Statistic::Int64 { end_time, .. }
            | Statistic::Float { end_time, .. }
            | Statistic::Double { end_time, .. }
            | Statistic::Text { end_time, .. } => *end_time,
        }
    }

    /// Returns true if no values have been recorded.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    // -----------------------------------------------------------------------
    // Typed update methods
    // -----------------------------------------------------------------------

    pub fn update_bool(&mut self, timestamp: i64, value: bool) {
        if let Statistic::Boolean {
            count,
            start_time,
            end_time,
            first,
            last,
            sum,
        } = self
        {
            if *count == 0 {
                *first = value;
                *start_time = timestamp;
            }
            *last = value;
            *end_time = timestamp;
            *sum += value as i64;
            *count += 1;
        }
    }

    pub fn update_i32(&mut self, timestamp: i64, value: i32) {
        if let Statistic::Int32 {
            count,
            start_time,
            end_time,
            min,
            max,
            first,
            last,
            sum,
        } = self
        {
            if *count == 0 {
                *first = value;
                *start_time = timestamp;
            }
            *last = value;
            *end_time = timestamp;
            if value < *min {
                *min = value;
            }
            if value > *max {
                *max = value;
            }
            *sum += value as f64;
            *count += 1;
        }
    }

    pub fn update_i64(&mut self, timestamp: i64, value: i64) {
        if let Statistic::Int64 {
            count,
            start_time,
            end_time,
            min,
            max,
            first,
            last,
            sum,
        } = self
        {
            if *count == 0 {
                *first = value;
                *start_time = timestamp;
            }
            *last = value;
            *end_time = timestamp;
            if value < *min {
                *min = value;
            }
            if value > *max {
                *max = value;
            }
            *sum += value as f64;
            *count += 1;
        }
    }

    pub fn update_f32(&mut self, timestamp: i64, value: f32) {
        if let Statistic::Float {
            count,
            start_time,
            end_time,
            min,
            max,
            first,
            last,
            sum,
        } = self
        {
            if *count == 0 {
                *first = value;
                *start_time = timestamp;
            }
            *last = value;
            *end_time = timestamp;
            if value < *min {
                *min = value;
            }
            if value > *max {
                *max = value;
            }
            *sum += value as f64;
            *count += 1;
        }
    }

    pub fn update_f64(&mut self, timestamp: i64, value: f64) {
        if let Statistic::Double {
            count,
            start_time,
            end_time,
            min,
            max,
            first,
            last,
            sum,
        } = self
        {
            if *count == 0 {
                *first = value;
                *start_time = timestamp;
            }
            *last = value;
            *end_time = timestamp;
            if value < *min {
                *min = value;
            }
            if value > *max {
                *max = value;
            }
            *sum += value;
            *count += 1;
        }
    }

    pub fn update_text(&mut self, timestamp: i64, value: &[u8]) {
        if let Statistic::Text {
            count,
            start_time,
            end_time,
            first,
            last,
        } = self
        {
            if *count == 0 {
                *first = value.to_vec();
                *start_time = timestamp;
            }
            *last = value.to_vec();
            *end_time = timestamp;
            *count += 1;
        }
    }

    // -----------------------------------------------------------------------
    // Serialization — field order matches C++ statistic.h for binary compat
    // -----------------------------------------------------------------------

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        match self {
            Statistic::Boolean {
                count,
                start_time,
                end_time,
                first,
                last,
                sum,
            } => {
                serialize::write_var_u64(w, *count)?;
                serialize::write_i64(w, *start_time)?;
                serialize::write_i64(w, *end_time)?;
                serialize::write_bool(w, *first)?;
                serialize::write_bool(w, *last)?;
                serialize::write_i64(w, *sum)?;
            }
            Statistic::Int32 {
                count,
                start_time,
                end_time,
                min,
                max,
                first,
                last,
                sum,
            } => {
                serialize::write_var_u64(w, *count)?;
                serialize::write_i64(w, *start_time)?;
                serialize::write_i64(w, *end_time)?;
                serialize::write_i32(w, *min)?;
                serialize::write_i32(w, *max)?;
                serialize::write_i32(w, *first)?;
                serialize::write_i32(w, *last)?;
                serialize::write_f64(w, *sum)?;
            }
            Statistic::Int64 {
                count,
                start_time,
                end_time,
                min,
                max,
                first,
                last,
                sum,
            } => {
                serialize::write_var_u64(w, *count)?;
                serialize::write_i64(w, *start_time)?;
                serialize::write_i64(w, *end_time)?;
                serialize::write_i64(w, *min)?;
                serialize::write_i64(w, *max)?;
                serialize::write_i64(w, *first)?;
                serialize::write_i64(w, *last)?;
                serialize::write_f64(w, *sum)?;
            }
            Statistic::Float {
                count,
                start_time,
                end_time,
                min,
                max,
                first,
                last,
                sum,
            } => {
                serialize::write_var_u64(w, *count)?;
                serialize::write_i64(w, *start_time)?;
                serialize::write_i64(w, *end_time)?;
                serialize::write_f32(w, *min)?;
                serialize::write_f32(w, *max)?;
                serialize::write_f32(w, *first)?;
                serialize::write_f32(w, *last)?;
                serialize::write_f64(w, *sum)?;
            }
            Statistic::Double {
                count,
                start_time,
                end_time,
                min,
                max,
                first,
                last,
                sum,
            } => {
                serialize::write_var_u64(w, *count)?;
                serialize::write_i64(w, *start_time)?;
                serialize::write_i64(w, *end_time)?;
                serialize::write_f64(w, *min)?;
                serialize::write_f64(w, *max)?;
                serialize::write_f64(w, *first)?;
                serialize::write_f64(w, *last)?;
                serialize::write_f64(w, *sum)?;
            }
            Statistic::Text {
                count,
                start_time,
                end_time,
                first,
                last,
            } => {
                serialize::write_var_u64(w, *count)?;
                serialize::write_i64(w, *start_time)?;
                serialize::write_i64(w, *end_time)?;
                serialize::write_bytes(w, first)?;
                serialize::write_bytes(w, last)?;
            }
        }
        Ok(())
    }

    /// Deserialize a Statistic of the given type from a reader.
    /// The caller must know the data type (from the chunk header).
    pub fn deserialize_from(r: &mut impl Read, data_type: TSDataType) -> Result<Self> {
        match data_type {
            TSDataType::Boolean => {
                let count = serialize::read_var_u64(r)?;
                let start_time = serialize::read_i64(r)?;
                let end_time = serialize::read_i64(r)?;
                let first = serialize::read_bool(r)?;
                let last = serialize::read_bool(r)?;
                let sum = serialize::read_i64(r)?;
                Ok(Statistic::Boolean {
                    count,
                    start_time,
                    end_time,
                    first,
                    last,
                    sum,
                })
            }
            TSDataType::Int32 => {
                let count = serialize::read_var_u64(r)?;
                let start_time = serialize::read_i64(r)?;
                let end_time = serialize::read_i64(r)?;
                let min = serialize::read_i32(r)?;
                let max = serialize::read_i32(r)?;
                let first = serialize::read_i32(r)?;
                let last = serialize::read_i32(r)?;
                let sum = serialize::read_f64(r)?;
                Ok(Statistic::Int32 {
                    count,
                    start_time,
                    end_time,
                    min,
                    max,
                    first,
                    last,
                    sum,
                })
            }
            TSDataType::Int64 => {
                let count = serialize::read_var_u64(r)?;
                let start_time = serialize::read_i64(r)?;
                let end_time = serialize::read_i64(r)?;
                let min = serialize::read_i64(r)?;
                let max = serialize::read_i64(r)?;
                let first = serialize::read_i64(r)?;
                let last = serialize::read_i64(r)?;
                let sum = serialize::read_f64(r)?;
                Ok(Statistic::Int64 {
                    count,
                    start_time,
                    end_time,
                    min,
                    max,
                    first,
                    last,
                    sum,
                })
            }
            TSDataType::Float => {
                let count = serialize::read_var_u64(r)?;
                let start_time = serialize::read_i64(r)?;
                let end_time = serialize::read_i64(r)?;
                let min = serialize::read_f32(r)?;
                let max = serialize::read_f32(r)?;
                let first = serialize::read_f32(r)?;
                let last = serialize::read_f32(r)?;
                let sum = serialize::read_f64(r)?;
                Ok(Statistic::Float {
                    count,
                    start_time,
                    end_time,
                    min,
                    max,
                    first,
                    last,
                    sum,
                })
            }
            TSDataType::Double => {
                let count = serialize::read_var_u64(r)?;
                let start_time = serialize::read_i64(r)?;
                let end_time = serialize::read_i64(r)?;
                let min = serialize::read_f64(r)?;
                let max = serialize::read_f64(r)?;
                let first = serialize::read_f64(r)?;
                let last = serialize::read_f64(r)?;
                let sum = serialize::read_f64(r)?;
                Ok(Statistic::Double {
                    count,
                    start_time,
                    end_time,
                    min,
                    max,
                    first,
                    last,
                    sum,
                })
            }
            TSDataType::Text | TSDataType::String => {
                let count = serialize::read_var_u64(r)?;
                let start_time = serialize::read_i64(r)?;
                let end_time = serialize::read_i64(r)?;
                let first = serialize::read_bytes(r)?;
                let last = serialize::read_bytes(r)?;
                Ok(Statistic::Text {
                    count,
                    start_time,
                    end_time,
                    first,
                    last,
                })
            }
        }
    }

    /// Reset the statistic to its initial empty state.
    pub fn reset(&mut self) {
        *self = Statistic::new(self.data_type());
    }

    /// Merge another Statistic into this one, extending the time range and
    /// updating aggregate fields.
    ///
    /// Used by TsFileIOWriter when building a TimeseriesIndex that spans
    /// multiple ChunkMeta entries: the merged statistic covers all chunks.
    /// `self` is assumed to have chronologically earlier data than `other`.
    pub fn merge(&mut self, other: &Statistic) {
        if other.is_empty() {
            return;
        }
        match (self, other) {
            (
                Statistic::Boolean { count, start_time, end_time, last, sum, .. },
                Statistic::Boolean {
                    count: oc, start_time: os, end_time: oe, last: ol, sum: osum, ..
                },
            ) => {
                *count += oc;
                *start_time = (*start_time).min(*os);
                *end_time = (*end_time).max(*oe);
                *last = *ol;
                *sum += osum;
            }
            (
                Statistic::Int32 { count, start_time, end_time, min, max, last, sum, .. },
                Statistic::Int32 {
                    count: oc, start_time: os, end_time: oe,
                    min: omin, max: omax, last: ol, sum: osum, ..
                },
            ) => {
                *count += oc;
                *start_time = (*start_time).min(*os);
                *end_time = (*end_time).max(*oe);
                *min = (*min).min(*omin);
                *max = (*max).max(*omax);
                *last = *ol;
                *sum += osum;
            }
            (
                Statistic::Int64 { count, start_time, end_time, min, max, last, sum, .. },
                Statistic::Int64 {
                    count: oc, start_time: os, end_time: oe,
                    min: omin, max: omax, last: ol, sum: osum, ..
                },
            ) => {
                *count += oc;
                *start_time = (*start_time).min(*os);
                *end_time = (*end_time).max(*oe);
                *min = (*min).min(*omin);
                *max = (*max).max(*omax);
                *last = *ol;
                *sum += osum;
            }
            (
                Statistic::Float { count, start_time, end_time, min, max, last, sum, .. },
                Statistic::Float {
                    count: oc, start_time: os, end_time: oe,
                    min: omin, max: omax, last: ol, sum: osum, ..
                },
            ) => {
                *count += oc;
                *start_time = (*start_time).min(*os);
                *end_time = (*end_time).max(*oe);
                if omin < min { *min = *omin; }
                if omax > max { *max = *omax; }
                *last = *ol;
                *sum += osum;
            }
            (
                Statistic::Double { count, start_time, end_time, min, max, last, sum, .. },
                Statistic::Double {
                    count: oc, start_time: os, end_time: oe,
                    min: omin, max: omax, last: ol, sum: osum, ..
                },
            ) => {
                *count += oc;
                *start_time = (*start_time).min(*os);
                *end_time = (*end_time).max(*oe);
                if omin < min { *min = *omin; }
                if omax > max { *max = *omax; }
                *last = *ol;
                *sum += osum;
            }
            (
                Statistic::Text { count, start_time, end_time, last, .. },
                Statistic::Text {
                    count: oc, start_time: os, end_time: oe, last: ol, ..
                },
            ) => {
                *count += oc;
                *start_time = (*start_time).min(*os);
                *end_time = (*end_time).max(*oe);
                *last = ol.clone();
            }
            // Type mismatch: ignore the merge (should not happen in valid files).
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn new_statistic_is_empty() {
        for dt in [
            TSDataType::Boolean,
            TSDataType::Int32,
            TSDataType::Int64,
            TSDataType::Float,
            TSDataType::Double,
            TSDataType::Text,
        ] {
            let stat = Statistic::new(dt);
            assert!(stat.is_empty());
            assert_eq!(stat.count(), 0);
            assert_eq!(stat.data_type(), dt);
        }
    }

    #[test]
    fn update_bool_tracks_count_and_values() {
        let mut stat = Statistic::new(TSDataType::Boolean);
        stat.update_bool(100, true);
        stat.update_bool(200, false);
        stat.update_bool(300, true);

        assert_eq!(stat.count(), 3);
        assert_eq!(stat.start_time(), 100);
        assert_eq!(stat.end_time(), 300);
        if let Statistic::Boolean {
            first, last, sum, ..
        } = &stat
        {
            assert_eq!(*first, true);
            assert_eq!(*last, true);
            assert_eq!(*sum, 2); // two true values
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn update_i32_tracks_min_max_sum() {
        let mut stat = Statistic::new(TSDataType::Int32);
        stat.update_i32(10, 5);
        stat.update_i32(20, -3);
        stat.update_i32(30, 10);

        assert_eq!(stat.count(), 3);
        assert_eq!(stat.start_time(), 10);
        assert_eq!(stat.end_time(), 30);
        if let Statistic::Int32 {
            min,
            max,
            first,
            last,
            sum,
            ..
        } = &stat
        {
            assert_eq!(*min, -3);
            assert_eq!(*max, 10);
            assert_eq!(*first, 5);
            assert_eq!(*last, 10);
            assert!((sum - 12.0).abs() < f64::EPSILON);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn update_i64_tracks_min_max_sum() {
        let mut stat = Statistic::new(TSDataType::Int64);
        stat.update_i64(1, i64::MIN);
        stat.update_i64(2, i64::MAX);

        assert_eq!(stat.count(), 2);
        if let Statistic::Int64 {
            min,
            max,
            first,
            last,
            ..
        } = &stat
        {
            assert_eq!(*min, i64::MIN);
            assert_eq!(*max, i64::MAX);
            assert_eq!(*first, i64::MIN);
            assert_eq!(*last, i64::MAX);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn update_f32_tracks_min_max() {
        let mut stat = Statistic::new(TSDataType::Float);
        stat.update_f32(1, 1.5);
        stat.update_f32(2, -0.5);
        stat.update_f32(3, 3.0);

        if let Statistic::Float {
            min,
            max,
            first,
            last,
            sum,
            ..
        } = &stat
        {
            assert_eq!(*min, -0.5);
            assert_eq!(*max, 3.0);
            assert_eq!(*first, 1.5);
            assert_eq!(*last, 3.0);
            assert!((sum - 4.0).abs() < 1e-6);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn update_f64_tracks_min_max() {
        let mut stat = Statistic::new(TSDataType::Double);
        stat.update_f64(1, 100.0);
        stat.update_f64(2, -200.0);

        if let Statistic::Double { min, max, sum, .. } = &stat {
            assert_eq!(*min, -200.0);
            assert_eq!(*max, 100.0);
            assert!((sum - (-100.0)).abs() < f64::EPSILON);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn update_text_tracks_first_last() {
        let mut stat = Statistic::new(TSDataType::Text);
        stat.update_text(1, b"hello");
        stat.update_text(2, b"world");

        assert_eq!(stat.count(), 2);
        if let Statistic::Text { first, last, .. } = &stat {
            assert_eq!(first, b"hello");
            assert_eq!(last, b"world");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn single_value_sets_first_and_last() {
        let mut stat = Statistic::new(TSDataType::Int32);
        stat.update_i32(42, 7);

        if let Statistic::Int32 {
            first,
            last,
            min,
            max,
            ..
        } = &stat
        {
            assert_eq!(*first, 7);
            assert_eq!(*last, 7);
            assert_eq!(*min, 7);
            assert_eq!(*max, 7);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn reset_clears_to_empty() {
        let mut stat = Statistic::new(TSDataType::Int32);
        stat.update_i32(1, 100);
        stat.update_i32(2, 200);
        assert_eq!(stat.count(), 2);

        stat.reset();
        assert!(stat.is_empty());
        assert_eq!(stat.count(), 0);
    }

    // --- Serialization round-trips ---

    fn serialize_round_trip(stat: &Statistic) -> Statistic {
        let dt = stat.data_type();
        let mut buf = Vec::new();
        stat.serialize_to(&mut buf).unwrap();
        let mut cursor = Cursor::new(&buf);
        Statistic::deserialize_from(&mut cursor, dt).unwrap()
    }

    #[test]
    fn serialize_bool_round_trip() {
        let mut stat = Statistic::new(TSDataType::Boolean);
        stat.update_bool(100, true);
        stat.update_bool(200, false);
        let decoded = serialize_round_trip(&stat);
        assert_eq!(stat, decoded);
    }

    #[test]
    fn serialize_i32_round_trip() {
        let mut stat = Statistic::new(TSDataType::Int32);
        stat.update_i32(10, -5);
        stat.update_i32(20, 100);
        let decoded = serialize_round_trip(&stat);
        assert_eq!(stat, decoded);
    }

    #[test]
    fn serialize_i64_round_trip() {
        let mut stat = Statistic::new(TSDataType::Int64);
        stat.update_i64(1, i64::MIN);
        stat.update_i64(2, i64::MAX);
        let decoded = serialize_round_trip(&stat);
        assert_eq!(stat, decoded);
    }

    #[test]
    fn serialize_f32_round_trip() {
        let mut stat = Statistic::new(TSDataType::Float);
        stat.update_f32(1, 3.14);
        stat.update_f32(2, -2.71);
        let decoded = serialize_round_trip(&stat);
        assert_eq!(stat, decoded);
    }

    #[test]
    fn serialize_f64_round_trip() {
        let mut stat = Statistic::new(TSDataType::Double);
        stat.update_f64(1, f64::MIN);
        stat.update_f64(2, f64::MAX);
        let decoded = serialize_round_trip(&stat);
        assert_eq!(stat, decoded);
    }

    #[test]
    fn serialize_text_round_trip() {
        let mut stat = Statistic::new(TSDataType::Text);
        stat.update_text(1, b"first");
        stat.update_text(2, b"last");
        let decoded = serialize_round_trip(&stat);
        assert_eq!(stat, decoded);
    }

    #[test]
    fn serialize_empty_statistic_round_trip() {
        for dt in [
            TSDataType::Boolean,
            TSDataType::Int32,
            TSDataType::Int64,
            TSDataType::Float,
            TSDataType::Double,
            TSDataType::Text,
        ] {
            let stat = Statistic::new(dt);
            let decoded = serialize_round_trip(&stat);
            assert_eq!(stat, decoded);
        }
    }

    #[test]
    fn update_wrong_type_is_ignored() {
        // Calling update_i32 on a Boolean statistic should do nothing
        let mut stat = Statistic::new(TSDataType::Boolean);
        stat.update_i32(1, 42);
        assert!(stat.is_empty());
    }
}
