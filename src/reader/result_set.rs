// C++ ResultSet exposes `bool next(bool& has_next)` + typed accessors.
// Rust's ResultSet implements Iterator<Item = Result<RowRecord>> on top
// of next_block(), which is the fast path for columnar consumers.
//
// Row-level filter application happens inside Iterator::next. Only the
// first column's value is passed to Filter::satisfy for single-column
// filter pushdown; multi-column aligned queries apply time-only filters
// at this stage (value filters over multi-column aligned queries are 5b).

use crate::error::Result;
use crate::reader::filter::Filter;
use crate::reader::row_record::RowRecord;
use crate::reader::scan_iterator::{AlignedSeriesScan, SeriesScanIterator};
use crate::reader::tsblock::{Column, ColumnMeta, TsBlock};
use crate::value::TsValue;
use std::sync::Arc;

enum ScanSource<'a> {
    Regular(SeriesScanIterator<'a>),
    Aligned(AlignedSeriesScan<'a>),
}

pub struct ResultSet<'a> {
    source: ScanSource<'a>,
    filter: Option<Arc<dyn Filter>>,
    column_meta: Arc<[ColumnMeta]>,
    current_block: Option<TsBlock>,
    row_cursor: usize,
}

impl<'a> ResultSet<'a> {
    pub(crate) fn from_regular(
        it: SeriesScanIterator<'a>,
        filter: Option<Arc<dyn Filter>>,
        column_meta: Arc<[ColumnMeta]>,
    ) -> Self {
        Self {
            source: ScanSource::Regular(it),
            filter,
            column_meta,
            current_block: None,
            row_cursor: 0,
        }
    }

    pub(crate) fn from_aligned(
        it: AlignedSeriesScan<'a>,
        filter: Option<Arc<dyn Filter>>,
        column_meta: Arc<[ColumnMeta]>,
    ) -> Self {
        Self {
            source: ScanSource::Aligned(it),
            filter,
            column_meta,
            current_block: None,
            row_cursor: 0,
        }
    }

    pub fn column_meta(&self) -> &[ColumnMeta] { &self.column_meta }

    pub fn next_block(&mut self) -> Result<Option<TsBlock>> {
        match &mut self.source {
            ScanSource::Regular(it) => it.next_block(),
            ScanSource::Aligned(it) => it.next_block(),
        }
    }

    fn row_of(block: &TsBlock, row: usize) -> RowRecord {
        let mut values = Vec::with_capacity(block.num_columns());
        for col in &block.columns {
            values.push(if col.is_null(row) { None } else { Some(value_at(col, row)) });
        }
        RowRecord::new(block.times[row], values)
    }
}

fn value_at(col: &Column, row: usize) -> TsValue {
    match col {
        Column::Boolean { values, .. } => TsValue::Boolean(values[row]),
        Column::Int32   { values, .. } => TsValue::Int32(values[row]),
        Column::Int64   { values, .. } => TsValue::Int64(values[row]),
        Column::Float   { values, .. } => TsValue::Float(values[row]),
        Column::Double  { values, .. } => TsValue::Double(values[row]),
        Column::Text    { values, .. } => TsValue::Text(values[row].clone()),
        Column::String  { values, .. } => TsValue::String(values[row].clone()),
    }
}

impl<'a> Iterator for ResultSet<'a> {
    type Item = Result<RowRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Refill current block when empty or exhausted.
            let need_refill = match &self.current_block {
                None => true,
                Some(b) => self.row_cursor >= b.num_rows(),
            };
            if need_refill {
                self.row_cursor = 0;
                match self.next_block() {
                    Ok(Some(b)) => self.current_block = Some(b),
                    Ok(None)    => { self.current_block = None; return None; }
                    Err(e)      => return Some(Err(e)),
                }
            }
            let block = self.current_block.as_ref().unwrap();
            let row = self.row_cursor;
            self.row_cursor += 1;
            let record = Self::row_of(block, row);

            // Row-level filter: pass column 0's value (or None if aligned
            // null slot). Multi-column value filters are deferred to 5b.
            if let Some(f) = self.filter.as_ref() {
                let v = record.values.first().and_then(|o| o.as_ref());
                if !f.satisfy(record.timestamp, v) { continue; }
            }
            return Some(Ok(record));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::BitMap;
    use crate::reader::tsblock::{Column, ColumnMeta, TsBlock};
    use crate::types::TSDataType;

    #[test]
    fn row_of_materializes_values() {
        let block = TsBlock::new(
            vec![0, 1, 2],
            vec![Column::Int64 { values: vec![10, 20, 30], nulls: None }],
            Arc::from(vec![ColumnMeta { name: "m".into(), data_type: TSDataType::Int64 }]),
        );
        let r0 = ResultSet::row_of(&block, 0);
        assert_eq!(r0.timestamp, 0);
        assert_eq!(r0.values[0], Some(TsValue::Int64(10)));
    }

    #[test]
    fn row_of_respects_null_bitmap() {
        let mut nulls = BitMap::new(3);
        nulls.set(1);
        let block = TsBlock::new(
            vec![0, 1, 2],
            vec![Column::Int64 { values: vec![10, 0, 30], nulls: Some(nulls) }],
            Arc::from(vec![ColumnMeta { name: "m".into(), data_type: TSDataType::Int64 }]),
        );
        let r0 = ResultSet::row_of(&block, 0);
        let r1 = ResultSet::row_of(&block, 1);
        let r2 = ResultSet::row_of(&block, 2);
        assert_eq!(r0.values[0], Some(TsValue::Int64(10)));
        assert_eq!(r1.values[0], None);
        assert_eq!(r2.values[0], Some(TsValue::Int64(30)));
    }
}
