// C++ value filters (filter/value/) are per-type template classes. The
// Rust surface uses TsValue as the bound; mismatched constants vs column
// type are rejected at query-build time (caller responsibility) so
// `satisfy` can compare same-variant values directly.

use crate::reader::filter::Filter;
use crate::statistic::Statistic;
use crate::value::TsValue;
use std::cmp::Ordering;

#[derive(Debug, Clone)] pub struct ValueEq { pub bound: TsValue }
#[derive(Debug, Clone)] pub struct ValueGt { pub bound: TsValue }
#[derive(Debug, Clone)] pub struct ValueLt { pub bound: TsValue }

impl ValueEq { pub fn new(bound: TsValue) -> Self { Self { bound } } }
impl ValueGt { pub fn new(bound: TsValue) -> Self { Self { bound } } }
impl ValueLt { pub fn new(bound: TsValue) -> Self { Self { bound } } }

/// Total-ordering compare of two TsValue of the same variant.
/// Returns None when variants differ (caller's responsibility to avoid
/// this at query-build time) or when a float comparison is NaN.
fn cmp_same_type(a: &TsValue, b: &TsValue) -> Option<Ordering> {
    match (a, b) {
        (TsValue::Boolean(x), TsValue::Boolean(y)) => Some(x.cmp(y)),
        (TsValue::Int32(x),   TsValue::Int32(y))   => Some(x.cmp(y)),
        (TsValue::Int64(x),   TsValue::Int64(y))   => Some(x.cmp(y)),
        (TsValue::Float(x),   TsValue::Float(y))   => x.partial_cmp(y),
        (TsValue::Double(x),  TsValue::Double(y))  => x.partial_cmp(y),
        (TsValue::Text(x),    TsValue::Text(y))    => Some(x.cmp(y)),
        (TsValue::String(x),  TsValue::String(y))  => Some(x.cmp(y)),
        _ => None,
    }
}

/// Statistic-level min/max lookup. Returns (min, max) as TsValue when
/// the statistic carries them. Non-numeric variants (Boolean, Text) do
/// not carry min/max and return None; pruning falls back to accept-all.
fn stat_min_max(stat: &Statistic) -> Option<(TsValue, TsValue)> {
    match stat {
        Statistic::Int32  { min, max, .. } => Some((TsValue::Int32(*min),  TsValue::Int32(*max))),
        Statistic::Int64  { min, max, .. } => Some((TsValue::Int64(*min),  TsValue::Int64(*max))),
        Statistic::Float  { min, max, .. } => Some((TsValue::Float(*min),  TsValue::Float(*max))),
        Statistic::Double { min, max, .. } => Some((TsValue::Double(*min), TsValue::Double(*max))),
        _ => None,
    }
}

impl Filter for ValueEq {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        let Some((min, max)) = stat_min_max(stat) else { return true; };
        cmp_same_type(&self.bound, &min).is_some_and(|o| o != Ordering::Less)
            && cmp_same_type(&self.bound, &max).is_some_and(|o| o != Ordering::Greater)
    }
    fn satisfy(&self, _time: i64, v: Option<&TsValue>) -> bool {
        v.and_then(|v| cmp_same_type(v, &self.bound)) == Some(Ordering::Equal)
    }
}

impl Filter for ValueGt {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        let Some((_min, max)) = stat_min_max(stat) else { return true; };
        cmp_same_type(&max, &self.bound).is_some_and(|o| o == Ordering::Greater)
    }
    fn satisfy(&self, _time: i64, v: Option<&TsValue>) -> bool {
        v.and_then(|v| cmp_same_type(v, &self.bound)) == Some(Ordering::Greater)
    }
}

impl Filter for ValueLt {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        let Some((min, _max)) = stat_min_max(stat) else { return true; };
        cmp_same_type(&min, &self.bound).is_some_and(|o| o == Ordering::Less)
    }
    fn satisfy(&self, _time: i64, v: Option<&TsValue>) -> bool {
        v.and_then(|v| cmp_same_type(v, &self.bound)) == Some(Ordering::Less)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TSDataType;

    /// Build an Int64 statistic with known min/max by feeding two points.
    fn stat_i64_minmax(min: i64, max: i64) -> Statistic {
        let mut s = Statistic::new(TSDataType::Int64);
        s.update_i64(0, min);
        s.update_i64(1, max);
        s
    }

    #[test]
    fn value_eq_row_level() {
        let f = ValueEq::new(TsValue::Int64(5));
        assert!( f.satisfy(0, Some(&TsValue::Int64(5))));
        assert!(!f.satisfy(0, Some(&TsValue::Int64(6))));
        assert!(!f.satisfy(0, None));
    }

    #[test]
    fn value_gt_row_level() {
        let f = ValueGt::new(TsValue::Double(1.0));
        assert!( f.satisfy(0, Some(&TsValue::Double(2.0))));
        assert!(!f.satisfy(0, Some(&TsValue::Double(1.0))));
    }

    #[test]
    fn value_lt_row_level() {
        let f = ValueLt::new(TsValue::Int32(10));
        assert!( f.satisfy(0, Some(&TsValue::Int32(9))));
        assert!(!f.satisfy(0, Some(&TsValue::Int32(10))));
    }

    #[test]
    fn value_eq_statistic_pruning_numeric() {
        let s = stat_i64_minmax(0, 100);
        assert!( ValueEq::new(TsValue::Int64(50)).satisfy_statistic(&s));
        assert!(!ValueEq::new(TsValue::Int64(500)).satisfy_statistic(&s));
    }

    #[test]
    fn value_gt_statistic_pruning_numeric() {
        let s = stat_i64_minmax(0, 100);
        assert!( ValueGt::new(TsValue::Int64(50)).satisfy_statistic(&s));
        assert!(!ValueGt::new(TsValue::Int64(100)).satisfy_statistic(&s));
    }

    #[test]
    fn value_lt_statistic_pruning_numeric() {
        let s = stat_i64_minmax(0, 100);
        assert!( ValueLt::new(TsValue::Int64(50)).satisfy_statistic(&s));
        assert!(!ValueLt::new(TsValue::Int64(0)).satisfy_statistic(&s));
    }

    #[test]
    fn null_value_rejects_value_filter() {
        let f = ValueEq::new(TsValue::Int32(0));
        assert!(!f.satisfy(0, None));
    }

    #[test]
    fn non_numeric_statistic_falls_back_to_accept() {
        let mut s = Statistic::new(TSDataType::Boolean);
        s.update_bool(0, true);
        let f = ValueEq::new(TsValue::Boolean(true));
        // Boolean statistic has no min/max tracked; pruning must accept.
        assert!(f.satisfy_statistic(&s));
    }
}
