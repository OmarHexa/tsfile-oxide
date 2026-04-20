// C++ SingleDeviceTsBlockReader (reader/block/) time-aligns N measurement
// scanners for one non-aligned device via a k-way merge. Produces
// columnar TsBlocks that cover every timestamp present in any of the N
// streams, with nulls at positions where a given measurement has no
// value at that timestamp.
//
// Row-level filter application lives in the outer ResultSet::Iterator::next,
// not here — this type is purely a merge primitive.

use crate::bitmap::BitMap;
use crate::error::{Result, TsFileError};
use crate::reader::scan_iterator::SeriesScanIterator;
use crate::reader::tsblock::{Column, ColumnMeta, TsBlock};
use crate::types::TSDataType;
use std::sync::Arc;

/// Matches the C++ default batch size. Private — not part of the public API.
const BATCH_SIZE: usize = 1024;

pub struct SingleDeviceTsBlockReader<'a> {
    scanners: Vec<SeriesScanIterator<'a>>,
    column_meta: Arc<[ColumnMeta]>,
    /// One head block per scanner. `None` for scanners that are fully drained.
    head_blocks: Vec<Option<TsBlock>>,
    /// Row index into the corresponding `head_blocks[i]`. Undefined when
    /// `head_blocks[i]` is `None`.
    head_cursors: Vec<usize>,
}

impl<'a> SingleDeviceTsBlockReader<'a> {
    /// Build a new merger. `scanners` and `column_meta` must have the same
    /// non-zero length; otherwise returns `TsFileError::InvalidArg`.
    pub fn new(
        scanners: Vec<SeriesScanIterator<'a>>,
        column_meta: Arc<[ColumnMeta]>,
    ) -> Result<Self> {
        if scanners.is_empty() {
            return Err(TsFileError::InvalidArg(
                "SingleDeviceTsBlockReader requires at least one scanner".into(),
            ));
        }
        if scanners.len() != column_meta.len() {
            return Err(TsFileError::InvalidArg(format!(
                "scanner count {} must match column_meta length {}",
                scanners.len(),
                column_meta.len()
            )));
        }
        let n = scanners.len();
        Ok(Self {
            scanners,
            column_meta,
            head_blocks: vec![None; n],
            head_cursors: vec![0; n],
        })
    }

    /// Produce the next merged block of up to `BATCH_SIZE` rows.
    /// Returns `Ok(None)` when every scanner is exhausted.
    pub fn next_block(&mut self) -> Result<Option<TsBlock>> {
        // 1. Refill any empty head slot.
        for i in 0..self.scanners.len() {
            if self.head_blocks[i].is_none()
                && let Some(b) = self.scanners[i].next_block()? {
                    // Invariant: the writer never emits zero-row pages;
                    // a debug-only guard catches a regression in the
                    // chunk reader or writer that would otherwise panic
                    // below on an out-of-bounds index.
                    debug_assert!(!b.is_empty(), "scanner emitted a zero-row block");
                    self.head_blocks[i] = Some(b);
                    self.head_cursors[i] = 0;
                }
        }
        if self.head_blocks.iter().all(|h| h.is_none()) {
            return Ok(None);
        }

        // 2. Allocate per-column output vectors.
        let n_cols = self.column_meta.len();
        let mut out_times: Vec<i64> = Vec::with_capacity(BATCH_SIZE);
        let mut out_columns: Vec<ColumnBuilder> = (0..n_cols)
            .map(|c| ColumnBuilder::new(self.column_meta[c].data_type))
            .collect();

        // 3. Merge up to BATCH_SIZE rows.
        while out_times.len() < BATCH_SIZE {
            // 3a. Find the minimum timestamp across non-exhausted heads.
            let mut min_time: Option<i64> = None;
            for i in 0..self.scanners.len() {
                if let Some(b) = self.head_blocks[i].as_ref() {
                    let t = b.times[self.head_cursors[i]];
                    min_time = Some(match min_time {
                        None => t,
                        Some(current) => current.min(t),
                    });
                }
            }
            let Some(min_time) = min_time else { break; };

            // 3b. Push time.
            out_times.push(min_time);

            // 3c. For each column, consume if head matches, else null.
            // `c` indexes three parallel vectors (out_columns, head_blocks,
            // head_cursors) simultaneously, so a range loop is clearer than
            // enumerate() over one of them.
            #[allow(clippy::needless_range_loop)]
            for c in 0..n_cols {
                match self.head_blocks[c].as_ref() {
                    Some(b) if b.times[self.head_cursors[c]] == min_time => {
                        out_columns[c].push_from(&b.columns[0], self.head_cursors[c]);
                        self.head_cursors[c] += 1;
                    }
                    _ => out_columns[c].push_null(),
                }
            }

            // 3d. Refill any head that is now exhausted (cursor past end).
            for i in 0..self.scanners.len() {
                let exhausted = match self.head_blocks[i].as_ref() {
                    Some(b) => self.head_cursors[i] >= b.num_rows(),
                    None => false,
                };
                if exhausted {
                    let next = self.scanners[i].next_block()?;
                    if let Some(b) = next.as_ref() {
                        debug_assert!(!b.is_empty(), "scanner emitted a zero-row block");
                    }
                    self.head_blocks[i] = next;
                    self.head_cursors[i] = 0;
                }
            }

            // 3e. If every head is now None, stop the row loop.
            if self.head_blocks.iter().all(|h| h.is_none()) { break; }
        }

        // 4. Build output columns + attach null bitmaps.
        let columns = out_columns.into_iter().map(|cb| cb.finish()).collect();
        Ok(Some(TsBlock::new(out_times, columns, self.column_meta.clone())))
    }
}

/// Per-column staging buffer used by the merge loop. Holds the typed
/// `values` vector and a `BitMap` marking null rows; finishes into a
/// `Column` enum variant on completion.
struct ColumnBuilder {
    data_type: TSDataType,
    row_count: usize,
    bool_values: Vec<bool>,
    i32_values: Vec<i32>,
    i64_values: Vec<i64>,
    f32_values: Vec<f32>,
    f64_values: Vec<f64>,
    text_values: Vec<Vec<u8>>,
    string_values: Vec<String>,
    nulls_flags: Vec<bool>,
}

impl ColumnBuilder {
    fn new(data_type: TSDataType) -> Self {
        Self {
            data_type,
            row_count: 0,
            bool_values: Vec::new(),
            i32_values: Vec::new(),
            i64_values: Vec::new(),
            f32_values: Vec::new(),
            f64_values: Vec::new(),
            text_values: Vec::new(),
            string_values: Vec::new(),
            nulls_flags: Vec::new(),
        }
    }

    fn push_from(&mut self, col: &Column, row: usize) {
        match (self.data_type, col) {
            (TSDataType::Boolean, Column::Boolean { values, .. }) => {
                self.bool_values.push(values[row]);
            }
            (TSDataType::Int32, Column::Int32 { values, .. }) => {
                self.i32_values.push(values[row]);
            }
            (TSDataType::Int64, Column::Int64 { values, .. }) => {
                self.i64_values.push(values[row]);
            }
            (TSDataType::Float, Column::Float { values, .. }) => {
                self.f32_values.push(values[row]);
            }
            (TSDataType::Double, Column::Double { values, .. }) => {
                self.f64_values.push(values[row]);
            }
            (TSDataType::Text, Column::Text { values, .. }) => {
                self.text_values.push(values[row].clone());
            }
            (TSDataType::String, Column::String { values, .. }) => {
                self.string_values.push(values[row].clone());
            }
            (dt, _) => panic!(
                "ColumnBuilder::push_from type mismatch: expected {:?}, got {:?}",
                dt, col.data_type()
            ),
        }
        self.nulls_flags.push(false);
        self.row_count += 1;
    }

    fn push_null(&mut self) {
        match self.data_type {
            TSDataType::Boolean => self.bool_values.push(false),
            TSDataType::Int32 => self.i32_values.push(0),
            TSDataType::Int64 => self.i64_values.push(0),
            TSDataType::Float => self.f32_values.push(0.0),
            TSDataType::Double => self.f64_values.push(0.0),
            TSDataType::Text => self.text_values.push(Vec::new()),
            TSDataType::String => self.string_values.push(String::new()),
        }
        self.nulls_flags.push(true);
        self.row_count += 1;
    }

    fn finish(self) -> Column {
        let mut bm = BitMap::new(self.row_count);
        let mut any_null = false;
        for (i, &is_null) in self.nulls_flags.iter().enumerate() {
            if is_null {
                bm.set(i);
                any_null = true;
            }
        }
        let nulls = if any_null { Some(bm) } else { None };
        match self.data_type {
            TSDataType::Boolean => Column::Boolean { values: self.bool_values, nulls },
            TSDataType::Int32 => Column::Int32 { values: self.i32_values, nulls },
            TSDataType::Int64 => Column::Int64 { values: self.i64_values, nulls },
            TSDataType::Float => Column::Float { values: self.f32_values, nulls },
            TSDataType::Double => Column::Double { values: self.f64_values, nulls },
            TSDataType::Text => Column::Text { values: self.text_values, nulls },
            TSDataType::String => Column::String { values: self.string_values, nulls },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_scanner_list_errors() {
        let column_meta: Arc<[ColumnMeta]> = Arc::from(Vec::<ColumnMeta>::new());
        let result = SingleDeviceTsBlockReader::new(Vec::new(), column_meta);
        assert!(matches!(
            result.err().expect("must error"),
            TsFileError::InvalidArg(_)
        ));
    }

    #[test]
    fn column_builder_all_values_no_nulls() {
        let mut cb = ColumnBuilder::new(TSDataType::Int64);
        let src = Column::Int64 { values: vec![10, 20, 30], nulls: None };
        cb.push_from(&src, 0);
        cb.push_from(&src, 1);
        cb.push_from(&src, 2);
        let col = cb.finish();
        match col {
            Column::Int64 { values, nulls: None } => assert_eq!(values, vec![10, 20, 30]),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn column_builder_with_nulls_sets_bitmap() {
        let mut cb = ColumnBuilder::new(TSDataType::Int64);
        let src = Column::Int64 { values: vec![10, 20], nulls: None };
        cb.push_from(&src, 0);
        cb.push_null();
        cb.push_from(&src, 1);
        cb.push_null();
        let col = cb.finish();
        match col {
            Column::Int64 { values, nulls: Some(bm) } => {
                assert_eq!(values.len(), 4);
                assert_eq!(values[0], 10);
                assert_eq!(values[2], 20);
                assert!(!bm.get(0));
                assert!(bm.get(1));
                assert!(!bm.get(2));
                assert!(bm.get(3));
            }
            _ => panic!("expected Some(BitMap)"),
        }
    }

    #[test]
    fn column_builder_all_nulls() {
        let mut cb = ColumnBuilder::new(TSDataType::Double);
        cb.push_null();
        cb.push_null();
        let col = cb.finish();
        match col {
            Column::Double { values, nulls: Some(bm) } => {
                assert_eq!(values.len(), 2);
                assert!(bm.get(0));
                assert!(bm.get(1));
            }
            _ => panic!("expected Some(BitMap)"),
        }
    }
}
